import astroWorker from '@astrojs/cloudflare/entrypoints/server';

const ROOM_NAME = 'nifty50';
const WS_PATH = '/api/stocks/ws';
const BASE_PATH = '/stocks';
const NSE_HOME_URL = 'https://www.nseindia.com/';
const NSE_URL =
	'https://www.nseindia.com/api/NextApi/apiClient/indexTrackerApi?functionName=getIndicesHeatMap&&index=NIFTY%2050';
const REFRESH_INTERVAL_MS = 10_000;
const FETCH_TIMEOUT_MS = 8_000;
const COOKIE_REFRESH_MS = 20 * 60_000;
const BACKOFF_STEPS_MS = [10_000, 20_000, 40_000, 80_000, 160_000, 300_000];

type StockRow = {
	symbol: string;
	lastPrice: number;
	change: number;
	pchange: number;
	tradedVolume: number;
	tradedValue: number;
	high: number;
	low: number;
};

type StockSnapshot = {
	type: 'snapshot';
	updatedAt: string;
	stale?: boolean;
	warning?: string;
	count: number;
	stocks: StockRow[];
};

type StockError = {
	type: 'error';
	updatedAt: string;
	message: string;
};

type StockMessage = StockSnapshot | StockError;

type AppEnv = Env & {
	STOCK_ROOM: DurableObjectNamespace;
};

export class StockTickerRoom {
	private lastPayload = '';
	private latest: StockMessage | null = null;
	private cookieHeader = '';
	private cookieRefreshedAt = 0;
	private consecutiveFailures = 0;
	private forceCookieRefresh = false;

	constructor(
		private readonly state: DurableObjectState,
		private readonly env: AppEnv,
	) {}

	async fetch(request: Request): Promise<Response> {
		if (request.headers.get('Upgrade') !== 'websocket') {
			return new Response('Expected a WebSocket upgrade request.', { status: 426 });
		}

		const pair = new WebSocketPair();
		const [client, server] = Object.values(pair);

		this.state.acceptWebSocket(server);

		if (this.latest) {
			server.send(JSON.stringify(this.latest));
		}

		await this.scheduleNextTick(50);
		return new Response(null, { status: 101, webSocket: client });
	}

	async alarm(): Promise<void> {
		if (!this.hasClients()) {
			return;
		}

		await this.refreshAndBroadcast();

		if (this.hasClients()) {
			await this.scheduleNextTick(this.getNextRefreshDelay());
		}
	}

	async webSocketClose(): Promise<void> {
		await this.clearAlarmWhenIdle();
	}

	async webSocketError(): Promise<void> {
		await this.clearAlarmWhenIdle();
	}

	private hasClients(): boolean {
		return this.state.getWebSockets().length > 0;
	}

	private async scheduleNextTick(delayMs: number): Promise<void> {
		if (!this.hasClients()) {
			return;
		}

		const currentAlarm = await this.state.storage.getAlarm();
		if (currentAlarm === null) {
			await this.state.storage.setAlarm(Date.now() + delayMs);
		}
	}

	private async clearAlarmWhenIdle(): Promise<void> {
		if (!this.hasClients()) {
			await this.state.storage.deleteAlarm();
		}
	}

	private async refreshAndBroadcast(): Promise<void> {
		try {
			const payload = await this.fetchNifty50();
			const body = JSON.stringify(payload);

			this.latest = payload;
			this.consecutiveFailures = 0;
			this.forceCookieRefresh = false;

			if (body !== this.lastPayload) {
				this.lastPayload = body;
				this.broadcast(body);
			}
		} catch (error) {
			this.consecutiveFailures += 1;
			this.forceCookieRefresh = shouldRefreshCookies(error);

			const message = error instanceof Error ? error.message : 'Unable to refresh stock data.';
			const payload = this.getFailurePayload(message);
			const body = JSON.stringify(payload);

			this.latest = payload;

			if (body !== this.lastPayload) {
				this.lastPayload = body;
				this.broadcast(body);
			}
		}
	}

	private async fetchNifty50(): Promise<StockSnapshot> {
		if (
			this.forceCookieRefresh ||
			!this.cookieHeader ||
			Date.now() - this.cookieRefreshedAt > COOKIE_REFRESH_MS
		) {
			await this.refreshNseCookies();
			this.forceCookieRefresh = false;
		}

		let response = await fetchNseApi(this.cookieHeader);

		if (response.status === 401 || response.status === 403 || response.status === 520) {
			await this.refreshNseCookies();
			response = await fetchNseApi(this.cookieHeader);
		}

		if (!response.ok) {
			throw new Error(`NSE returned ${response.status}`);
		}

		const json = (await response.json()) as { data?: unknown };
		if (!Array.isArray(json.data)) {
			throw new Error('NSE response did not include a data array.');
		}

		const stocks = json.data
			.map(normalizeStock)
			.filter((stock): stock is StockRow => stock !== null)
			.sort((a, b) => b.pchange - a.pchange);

		return {
			type: 'snapshot',
			updatedAt: new Date().toISOString(),
			count: stocks.length,
			stocks,
		};
	}

	private getFailurePayload(message: string): StockMessage {
		if (this.latest?.type === 'snapshot') {
			return {
				...this.latest,
				stale: true,
				warning: message,
			};
		}

		return {
			type: 'error',
			updatedAt: new Date().toISOString(),
			message,
		};
	}

	private getNextRefreshDelay(): number {
		if (this.consecutiveFailures === 0) {
			return REFRESH_INTERVAL_MS;
		}

		return BACKOFF_STEPS_MS[Math.min(this.consecutiveFailures - 1, BACKOFF_STEPS_MS.length - 1)];
	}

	private async refreshNseCookies(): Promise<void> {
		const response = await fetch(NSE_HOME_URL, {
			cache: 'no-store',
			signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
			headers: browserHeaders(),
		});

		const cookieHeader = getCookieHeader(response.headers);

		if (cookieHeader) {
			this.cookieHeader = cookieHeader;
			this.cookieRefreshedAt = Date.now();
		}
	}

	private broadcast(body: string): void {
		for (const socket of this.state.getWebSockets()) {
			try {
				socket.send(body);
			} catch {
				socket.close(1011, 'Broadcast failed');
			}
		}
	}
}

function fetchNseApi(cookieHeader: string): Promise<Response> {
	return fetch(NSE_URL, {
		cache: 'no-store',
		signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
		headers: {
			...browserHeaders(),
			Accept:
				'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
			'Cache-Control': 'no-cache',
			Cookie: cookieHeader,
			Pragma: 'no-cache',
			Referer: 'https://www.nseindia.com/market-data/live-equity-market',
			'Upgrade-Insecure-Requests': '1',
		},
	});
}

function browserHeaders(): HeadersInit {
	return {
		'Accept-Language': 'en-US,en;q=0.9',
		'Sec-Ch-Ua': '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
		'Sec-Ch-Ua-Mobile': '?0',
		'Sec-Ch-Ua-Platform': '"macOS"',
		'Sec-Fetch-Dest': 'document',
		'Sec-Fetch-Mode': 'navigate',
		'Sec-Fetch-Site': 'none',
		'Sec-Fetch-User': '?1',
		'User-Agent':
			'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36',
	};
}

function getCookieHeader(headers: Headers): string {
	const cookies = headers.getSetCookie?.() ?? splitSetCookie(headers.get('set-cookie'));

	return cookies
		.map((cookie) => cookie.split(';', 1)[0])
		.filter(Boolean)
		.join('; ');
}

function splitSetCookie(value: string | null): string[] {
	if (!value) {
		return [];
	}

	return value.split(/,(?=\s*[^;,]+=)/);
}

function shouldRefreshCookies(error: unknown): boolean {
	return error instanceof Error && /NSE returned (401|403|520)/.test(error.message);
}

function normalizeStock(value: unknown): StockRow | null {
	if (!value || typeof value !== 'object') {
		return null;
	}

	const row = value as Record<string, unknown>;
	const symbol = toStringValue(row.symbol);

	if (!symbol) {
		return null;
	}

	return {
		symbol,
		lastPrice: toNumber(row.lastPrice),
		change: toNumber(row.change),
		pchange: toNumber(row.pchange),
		tradedVolume: toNumber(row.tradedVolume),
		tradedValue: toNumber(row.tradedValue),
		high: toNumber(row.high),
		low: toNumber(row.low),
	};
}

function toStringValue(value: unknown): string {
	return typeof value === 'string' ? value : '';
}

function toNumber(value: unknown): number {
	return typeof value === 'number' && Number.isFinite(value) ? value : 0;
}

export default {
	async fetch(request: Request, env: AppEnv, ctx: ExecutionContext): Promise<Response> {
		const url = new URL(request.url);

		if (url.pathname === WS_PATH || url.pathname === `${BASE_PATH}${WS_PATH}`) {
			if (!env.STOCK_ROOM) {
				return new Response('STOCK_ROOM Durable Object binding is missing.', { status: 500 });
			}

			const id = env.STOCK_ROOM.idFromName(ROOM_NAME);
			return env.STOCK_ROOM.get(id).fetch(request);
		}

		if (url.pathname === BASE_PATH) {
			url.pathname = `${BASE_PATH}/`;
			return Response.redirect(url, 308);
		}

		if (url.pathname.startsWith(`${BASE_PATH}/`)) {
			const rewrittenUrl = new URL(request.url);
			rewrittenUrl.pathname = url.pathname.slice(BASE_PATH.length) || '/';

			const rewrittenRequest = new Request(rewrittenUrl, request);
			const response = await astroWorker.fetch(rewrittenRequest, env, ctx);

			if (isHtmlResponse(response)) {
				return prefixRootAssetUrls(response);
			}

			return response;
		}

		return astroWorker.fetch(request, env, ctx);
	},
};

function isHtmlResponse(response: Response): boolean {
	return response.headers.get('content-type')?.includes('text/html') ?? false;
}

async function prefixRootAssetUrls(response: Response): Promise<Response> {
	const html = await response.text();
	const headers = new Headers(response.headers);
	headers.delete('content-length');

	return new Response(
		html
			.replaceAll('href="/favicon.svg"', `href="${BASE_PATH}/favicon.svg"`)
			.replaceAll('href="/_astro/', `href="${BASE_PATH}/_astro/`)
			.replaceAll('src="/_astro/', `src="${BASE_PATH}/_astro/`),
		{
			status: response.status,
			statusText: response.statusText,
			headers,
		},
	);
}
