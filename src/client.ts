/**
 * TradingView WebSocket Client
 * Client for TradingView real-time data feeds
 */

import WebSocket from "isomorphic-ws";

class UniversalEventEmitter {
  private events = new Map<string, Set<Function>>();
  
  on(event: string, listener: Function) {
    if (!this.events.has(event)) {
      this.events.set(event, new Set());
    }
    this.events.get(event)!.add(listener);
    return this;
  }
  
  off(event: string, listener: Function) {
    this.events.get(event)?.delete(listener);
    return this;
  }
  
  removeListener(event: string, listener: Function) {
    return this.off(event, listener);
  }
  
  emit(event: string, ...args: any[]) {
    const listeners = this.events.get(event);
    if (listeners) {
      listeners.forEach(listener => listener(...args));
    }
    return listeners ? listeners.size > 0 : false;
  }
  
  once(event: string, listener: Function) {
    const onceWrapper = (...args: any[]) => {
      this.off(event, onceWrapper);
      listener(...args);
    };
    return this.on(event, onceWrapper);
  }
  
  listenerCount(event: string): number {
    return this.events.get(event)?.size || 0;
  }
  
  eventNames(): string[] {
    return Array.from(this.events.keys());
  }
}

const HOST = "wss://data.tradingview.com/socket.io/websocket?&type=chart";
const PROHOST = "wss://prodata.tradingview.com/socket.io/websocket?&type=chart";

import type { Report, AnnualReport, QuarterlyReport } from "./client.quote.ts";

export type Message = Array<any> | Record<string, any> | string;

export type Session = {
  protocol: Record<string, unknown>;
  socket: WebSocket;
  send: (event: string, payload: Message) => Promise<void>;
  close: () => Promise<void>;
} & UniversalEventEmitter;

export class ProtocolError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ProtocolError";
  }
}

export class CriticalError extends Error {
  chartId: string;
  info: unknown;
  constructor(chartId: string, message: string, info?: unknown) {
    super(`${message}${info ? ', ' + JSON.stringify(info) : ''}`);
    this.name = "CriticalError";
    this.chartId = chartId;
    this.info = info;
  }
}

export class PayloadError extends Error {
  chartId: string;
  params: unknown;
  constructor(chartId: string, params: unknown) {
    super(`Payload error for chart ${chartId}: ${JSON.stringify(params)}`);
    this.name = "PayloadError";
    this.chartId = chartId;
    this.params = params;
  }
}

export class SymbolError extends Error {
  chartId: string;
  symbolId: string;
  time: number;
  constructor(chartId: string, symbolId: string, message: string, time: number) {
    super(message);
    this.name = "SymbolError";
    this.chartId = chartId;
    this.symbolId = symbolId;
    this.time = time;
  }
}

export class SeriesError extends Error {
  chartId: string;
  seriesId: string;
  turnaroundId: string;
  node: string;
  time: number;
  constructor(chartId: string, seriesId: string, turnaroundId: string, message: string, node: string, time: number) {
    super(message);
    this.name = "SeriesError";
    this.chartId = chartId;
    this.seriesId = seriesId;
    this.turnaroundId = turnaroundId;
    this.node = node;
    this.time = time;
  }
}

export class StudyError extends Error {
  chartId: string;
  studyId: string;
  turnaroundId: string;
  node: string;
  time: number;
  constructor(chartId: string, studyId: string, turnaroundId: string, message: string, node: string, time: number) {
    super(message);
    this.name = "StudyError";
    this.chartId = chartId;
    this.studyId = studyId;
    this.turnaroundId = turnaroundId;
    this.node = node;
    this.time = time;
  }
}

export async function createSession(token?: string, verbose?: boolean): Promise<Session> {
  const emitter = new UniversalEventEmitter();

  let closed = false;
  let protocol: unknown = undefined;
  let interval: ReturnType<typeof setInterval> | undefined;

  let socket: WebSocket;
  try {
    // Only set headers if custom WebSocket headers are supported (not in browser)
    const isCustomHeadersSupported = typeof WebSocket === "function" && typeof (globalThis as any).process !== "undefined" && (globalThis as any).process.versions?.node;
    if (isCustomHeadersSupported) {
      socket = new WebSocket(token ? PROHOST : HOST, {
        headers: {
          "Origin": "https://www.tradingview.com",
          "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
          "Cache-Control": "no-cache",
          "Pragma": "no-cache",
        },
        followRedirects: true
      });
    } else {
      socket = new WebSocket(token ? PROHOST : HOST);
    }
  } catch (err) {
    throw new Error("WebSocket creation failed: " + (err instanceof Error ? err.message : String(err)));
  }

  const closeSession = async () => {
    if (closed) return;
    closed = true;
    if (interval) clearInterval(interval);

    try { 
      if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) {
        socket.close(); 
      }
    } catch { }
    emitter.emit("close");
  };
  await new Promise<void>((resolve, reject) => {
    if (socket.readyState === WebSocket.OPEN) {
      resolve();
      return;
    }

    const timeout = setTimeout(() => {
      reject(new Error("WebSocket connection timeout"));
    }, 10000);

    const onOpen = () => {
      clearTimeout(timeout);
      resolve();
    };

    const onError = (error: unknown) => {
      clearTimeout(timeout);
      const message = error instanceof Error ? error.message : String(error);
      reject(new Error("WebSocket connection failed: " + message));
    };

    if (typeof (socket as any).on === 'function') {
      (socket as any).on('open', onOpen);
      (socket as any).on('error', onError);
    } else {
      socket.addEventListener('open', onOpen);
      socket.addEventListener('error', onError);
    }
  });

  const messageHandler = (event: unknown) => {
    if (closed) return;

    const eventData = (event as { data?: unknown })?.data;
    const text = typeof eventData === 'string' ? eventData :
                 typeof event === 'string' ? event :
                 String(eventData || event);
    
    const packets = text.split("~m~").filter(Boolean).reduce((acc: string[], packet: string, i: number) => {
      if (i % 2 === 0) acc.push("~m~" + packet);
      else acc[acc.length - 1] += "~m~" + packet;
      return acc;
    }, [] as string[]);

    for (const raw of packets) {
      if (raw.includes("~m~~h~")) {
        const m = raw.match(/~m~(\d+)~m~~h~(\d+)/);
        if (m) {
          send("heartbeat", `~m~${m[1]}~m~~h~${m[2]}`);
        }
      } else if (raw.includes('~m~{"session_id"')) {
        const m = raw.match(/~m~(\d+)~m~(.+)/);
        if (m) {
          protocol = JSON.parse(m[2]);
          emitter.emit("protocol", protocol);
        }
      } else {
        const m = raw.match(/~m~(\d+)~m~(.+)/);
        if (!m) continue;
        const { m: event, p: payload } = JSON.parse(m[2]);
        if (!protocol) {
          protocol = {};
        }
        if (!closed && protocol) {
          emitter.emit("open", protocol);
        }
        emitter.emit("message", { event, payload });
        if (verbose) console.log(`[RECEIVED] [${event}]`, payload);
        if (event && typeof event === "string" && event.includes("error")) {
          emitter.emit("error", event, payload);
        } else {
          emitter.emit(event, payload);
        }
      }
    }
  };

  const errorHandler = (error: unknown) => {
    if (!closed) {
      emitter.emit("error", error);
    }
  };

  const closeHandler = () => {
    closeSession();
  };

  if (socket.addEventListener) {
    socket.addEventListener('message', messageHandler);
    socket.addEventListener('error', errorHandler);
    socket.addEventListener('close', closeHandler);
  } else {
    (socket as any).on('message', messageHandler);
    (socket as any).on('error', errorHandler);
    (socket as any).on('close', closeHandler);
  }

  // Wait for protocol negotiation
  await new Promise<void>((resolve, reject) => {
    const onProtocol = (proto: unknown) => {
      protocol = proto;
      emitter.off("error", onError);
      resolve();
    };
    const onError = (err: unknown) => {
      emitter.off("protocol", onProtocol);
      reject(err instanceof Error ? err : new Error(String(err)));
    };
    emitter.once("protocol", onProtocol);
    emitter.once("error", onError);
  });

  // Send function
  const send = async (event: string, payload: Message | string): Promise<void> => {
    if (closed || socket.readyState !== WebSocket.OPEN) {
      throw new Error("Cannot send on closed session");
    }
    
    let message: string;
    if (typeof payload === 'string') {
      // For heartbeat messages, send directly
      message = payload;
    } else {
      // For regular messages, format as JSON
      const jsonMessage = JSON.stringify({ m: event, p: payload });
      message = `~m~${jsonMessage.length}~m~${jsonMessage}`;
      if (verbose) console.log(`[SEND] ${event}`, payload);
    }
    
    socket.send(message);
  };

  // Authenticate and set locale
  await send("set_auth_token", [token || "unauthorized_user_token"]);
  await send("set_locale", ["en", "US"]);

  // Clean up session if no listeners (optional but keep logic safe)
  interval = setInterval(() => {
    if (emitter.eventNames().length === 0 && !closed) {
      closeSession();
    }
  }, 100);

  // Attach Session methods/properties
  Object.assign(emitter, {
    protocol,
    socket,
    send,
    close: closeSession,
  });

  return emitter as Session;
}

export type ResolveSymbol = {
  id: string;
  pro_name: string;
  full_name: string;
  description: string;
  short_description: string;
  exchange: string;
  pricescale: number;
  minmov: number;
  session: string;
  timezone: string;
  type: string;
  has_intraday: boolean;
  is_tradable: boolean;
  listed_exchange: string;
  currency_code?: string;
  source2: {
    id: string;
    name: string;
    description: string;
    [key: string]: unknown;
  }
  [key: string]: unknown;
}

type SeriesCreated = {
  id: string;
  updateMode: string;
  turnaroundId: string;
  info: Record<string, unknown>;
}

type SeriesModified = SeriesCreated;

type StudyCreated = {
  id: string;
  seriesId: string;
  turnaroundId: string;
  info: Record<string, unknown>;
}

type StudyModified = StudyCreated;

interface EventType {
  "resolve_symbol": ResolveSymbol;
  "create_series": SeriesCreated;
  "modify_series": SeriesModified;
  "remove_series": void,
  "create_study": StudyCreated;
  "modify_study": StudyModified;
  "remove_study": void;
}

const RESULTING_EVENTS: (keyof EventType)[] = [
  "resolve_symbol",
  "create_series",
  "modify_series",
  "remove_series",
  "create_study",
  "modify_study",
  "remove_study"
];

export function request<E extends keyof EventType>(
  session: Session,
  event: E,
  payload: Message,
): Promise<EventType[E]>;
export function request(
  session: Session,
  event: string,
  payload: Message,
): Promise<any>;
export function request(session: Session, event: string, payload: Message): Promise<unknown> {
  if (!RESULTING_EVENTS.includes(event as keyof EventType)) {
    throw new Error(`Invalid event type: ${event}. Expected one of: ${RESULTING_EVENTS.join(', ')}`);
  }
  return new Promise((resolve, reject) => {
    const handleResponse = (response: { event: string, payload: unknown }) => {
      // Defensive: Both payloads must be arrays for all index access
      if (!Array.isArray(payload) || !Array.isArray(response.payload)) return;

      switch (event) {
        case 'resolve_symbol': {
          if (response.event !== 'symbol_resolved') return;
          if (response.payload[0] !== payload[0] || response.payload[1] !== payload[1]) return;
          response.payload[2].id = response.payload[1];
          resolve(response.payload[2] as EventType['resolve_symbol']);
          break;
        }
        case 'create_series':
        case 'modify_series': {
          if (response.event !== 'series_completed') return;
          if (response.payload[0] !== payload[0] || response.payload[1] !== payload[1] || response.payload[3] !== payload[2]) return;
          resolve({
            id: payload[1],
            updateMode: response.payload[2],
            turnaroundId: response.payload[3],
            info: response.payload[4] || {}
          } as EventType['create_series']);
          break;
        }
        case "remove_series": {
          if (response.event !== "series_deleted") return;
          if (response.payload[0] !== payload[0] || response.payload[1] !== payload[1]) return;
          resolve(undefined);
          break;
        }
        case 'create_study':
        case 'modify_study': {
          if (response.event !== 'study_completed') return;
          if (
            response.payload[0] !== payload[0] ||
            response.payload[1] !== payload[1] ||
            response.payload[2] !== (Array.isArray(payload) ? payload[3] + "_" + payload[2] : undefined)
          ) return;
          resolve({
            id: response.payload[1],
            seriesId: payload[1],
            turnaroundId: response.payload[2],
            info: response.payload[4] || {}
          } as EventType['create_study']);
          break;
        }
        case "remove_study": {
          if (response.event !== "study_deleted") return;
          if (response.payload[0] !== payload[0] || response.payload[1] !== payload[1]) return;
          resolve(undefined);
          break;
        }
        default: return;
      }
      session.removeListener('message', handleResponse);
      session.removeListener('error', handleError);
    };

    const handleError = (e: string, error: unknown) => {
      // Defensive: Both payload and error must be arrays for all index access
      if (!Array.isArray(payload) || !Array.isArray(error)) return;

      switch (e) {
        case 'critical_error': {
          if (payload[0] !== error[0]) return;
          reject(new CriticalError(payload[0], error[1], error[2]));
          break;
        }
        case 'symbol_error': {
          if (event !== 'resolve_symbol') return;
          if (payload[0] !== error[0] || payload[1] !== error[1]) return;
          reject(new SymbolError(payload[0], payload[1], error[2], error[3]));
          break;
        }
        case 'series_error': {
          if (event !== 'create_series') return;
          if (payload[0] !== error[0] || payload[1] !== error[1] || payload[2] !== error[2]) return;
          reject(new SeriesError(payload[0], payload[1], payload[2], error[3], error[4], error[5]));
          break;
        }
        case 'study_error': {
          if (event !== 'create_study') return;
          if (payload[0] !== error[0] || payload[1] !== error[1] || (Array.isArray(payload) ? payload[3] + "_" + payload[2] : undefined) !== error[2]) return;
          const message = typeof error[3].error === 'string' ? error[3].error.replace("{symbol}", payload[1]) : JSON.stringify(error[3]);
          reject(new StudyError(payload[0], payload[1], payload[2], message, error[4], error[5]));
          break;
        }
        case "protocol_error": {
          if (event !== 'set_auth_token') return;
          reject(new ProtocolError(`Protocol error: ${error[0]}`));
          break;
        }
        case "error_message": {
          if (payload[0] !== error[0]) return;
          reject(new PayloadError(payload[0], error[1]));
          break;
        }
        default: {
          reject(new Error(`Unknown error event: ${e}, payload: ${JSON.stringify(error)}`));
          break;
        }
      }
      session.removeListener('message', handleResponse);
      session.removeListener('error', handleError);
    };

    session.on('message', handleResponse);
    session.on('error', handleError);

    session.send(event, payload);
  });
}

let requestCounter = 0;
const useId = () => (++requestCounter).toString(36);

export interface Chart {
  id: string;
  resolve: (symbol: string, exchange: string) => Promise<ResolveSymbol>;
  close: () => void;
}

export function createChart(session: Session): Promise<Chart> {
  const chart: Chart = {
    id: `chart_${useId()}`,
    resolve: (symbol: string, exchange: string) => {
      return request(session, 'resolve_symbol', [chart.id, `symbol_${useId()}`, `${exchange}:${symbol}`]);
    },
    close: () => {
      session.send('chart_delete_session', [chart.id]);
      if (session.listenerCount('error') > 0)
        session.emit('error', "chart_deleted", [chart.id, "chart session closed"]);
    }
  };
  session.send('chart_create_session', [chart.id]);
  return Promise.resolve(chart);
}

export interface Series {
  id: string;
  timeframe: string;
  history: number[][];
  scope?: { from: number, to: number }
  modify(options: { symbol?: string, timeframe?: string, range?: Range, count?: number }): Promise<Series>;
  close(): Promise<void>;
  stream(): AsyncIterableIterator<number[]>;
  [Symbol.iterator](): IterableIterator<number[]>;
}

export type Range = number[] | "1D" | "5d" | "1M" | "3M" | "YTD" | "12M" | "60M" | "ALL";

function parse(range: Range): [number, number] {
  if (Array.isArray(range)) {
    if (range.length !== 2) throw new Error("Range must be an array of two numbers");
    return [range[0], range[1]];
  } else if (range.startsWith('r,')) {
    const parts = range.slice(2).split(':');
    if (parts.length !== 2) throw new Error("Range string must be in the format 'r,start:end'");
    const start = parseInt(parts[0], 10);
    const end = parseInt(parts[1], 10);
    if (isNaN(start) || isNaN(end)) throw new Error("Invalid range values in string");
    if (start > end) throw new Error("Start time must be less than or equal to end time");
    return [start, end];
  } else if (typeof range === "string") {
    const now = Math.floor(Date.now() / 1000); // Current time in seconds
    switch (range) {
      case "1D":
        return [now - 24 * 60 * 60, now]; // Last 24 hours
      case "5d":
        return [now - 5 * 24 * 60 * 60, now]; // Last 5 days
      case "1M":
        return [now - 30 * 24 * 60 * 60, now]; // Last 30 days
      case "3M":
        return [now - 90 * 24 * 60 * 60, now]; // Last 90 days
      case "YTD": {
        const startOfYear = new Date(new Date().getFullYear(), 0, 1).getTime() / 1000; // Start of the current year
        return [startOfYear, now]; // Year to date
      }
      case "12M":
        return [now - 365 * 24 * 60 * 60, now]; // Last 12 months
      case "60M":
        return [now - 60 * 24 * 60 * 60, now]; // Last 60 months
      case "ALL":
        return [0, now]; // All time
      default:
        throw new Error(`Invalid range string: ${range}`);
    }
  } else {
    throw new Error(`Invalid range type: ${typeof range}. Expected an array or a string.`);
  }
};

export function createSeries(
  session: Session,
  chart: Chart,
  symbol: ResolveSymbol,
  timeframe: string,
  count: number,
  range?: Range
): Promise<Series> {
  let iteratorListener: (payload: unknown[]) => void;
  const timeseries: Record<string, number[][]> = {};
  const series: Series = {
    id: `series_${useId()}`,
    timeframe,
    history: [],
    scope: undefined,
    modify(options) {
      const { symbol: newSymbol, timeframe: newTimeframe, range: newRange, count: newCount } = options || {};
      if (newTimeframe) series.timeframe = newTimeframe;
      return request(session, 'modify_series', [
        chart.id,
        series.id,
        series.id, // turnaroundId
        newSymbol ? newSymbol : symbol.id,
        newTimeframe ? newTimeframe : series.timeframe,
        newCount ? newCount : count,
        Array.isArray(newRange) ? `r,${newRange[0]}:${newRange[1]}` : newRange || ""
      ]).then(() => series);
    },
    close: () => request(session, 'remove_series', [chart.id, series.id]).then(() => {
      if (session.listenerCount('error') > 0)
        session.emit('series_error', "series_deleted", [chart.id, series.id, "series session closed"]);
    }),
    stream() {
      let currentTime = series.history.length > 0 ? series.history[0][0] : 0;
      return {
        next: () => {
          const nextRecord = series.history.find(record => record[0] > currentTime);
          // if (!nextRecord && series.scope?.to) return Promise.resolve({ done: true, value: undefined });
          if (!nextRecord) {
            session.send("request_more_tickmarks", [chart.id, series.id, 1]);
            return new Promise(resolve => {
              function listener(payload: unknown[]) {
                if (!Array.isArray(payload) || payload[0] !== chart.id || typeof (payload[1] as Record<string, any>)?.[series.id] === "undefined") return;
                const seriesData = (payload[1] as Record<string, any>)[series.id];
                const data = (seriesData.s as Array<{ v: number[] }>).map(i => i.v);
                series.history.push(...data);
                series.history.sort((a, b) => a[0] - b[0]);
                // Remove duplicates.
                series.history = series.history.filter((item, i, self) => i === self.findIndex((t) => t[0] === item[0]));
                const nextRecord = series.history.find(record => record[0] > currentTime);
                if (!nextRecord) return;
                currentTime = nextRecord[0];
                resolve({ done: false, value: nextRecord });
                session.removeListener('du', listener);
                session.send("request_more_tickmarks", [chart.id, series.id, 1]);
              }
              session.on('du', listener);
              iteratorListener = listener;
            });
          }
          currentTime = nextRecord[0];
          return Promise.resolve({ done: false, value: nextRecord });
        },
        [Symbol.asyncIterator]() {
          return this;
        }
      }
    },
    [Symbol.iterator]() {
      let currentTime = series.history.length > 0 ? series.history[0][0] : 0;
      return {
        next: () => {
          const nextRecord = series.history.find(record => record[0] > currentTime);
          if (!nextRecord) return { done: true, value: undefined };
          currentTime = nextRecord[0];
          return { done: false, value: nextRecord };
        },
        [Symbol.iterator]() {
          return this;
        }
      };
    }
  };

  function refreshHistory() {
    timeseries[series.timeframe] = timeseries[series.timeframe] || [];
    series.history = timeseries[series.timeframe].filter(record => {
      if (series.scope && series.scope.from && series.scope.to) {
        return record[0] >= series.scope.from && record[0] <= series.scope.to;
      }
      return true; // If no range is set, return all records
    });
  }

  function timescale_update(payload: unknown[]) {
    if (!Array.isArray(payload) || payload[0] !== chart.id || typeof (payload[1] as Record<string, any>)?.[series.id] === "undefined") return;
    const seriesData = (payload[1] as Record<string, any>)[series.id];
    const data = (seriesData.s as Array<{ v: number[] }>).map(i => i.v);
    timeseries[series.timeframe] = timeseries[series.timeframe] || [];
    timeseries[series.timeframe] = timeseries[series.timeframe].concat(data);
    // Sort the history by timestamp
    timeseries[series.timeframe].sort((a, b) => a[0] - b[0]);
    // Remove duplicates.
    timeseries[series.timeframe] = timeseries[series.timeframe].filter((item, i, self) => i === self.findIndex((t) => t[0] === item[0]));
    refreshHistory();
  }

  function series_timeframe(payload: unknown[]) {
    if (!Array.isArray(payload) || payload[0] !== chart.id || payload[1] !== series.id) return;
    const range = parse(payload[5] as Range);
    series.scope = {
      from: range[0],
      to: range[1]
    };
    refreshHistory();
  }

  function error_handler(e: string, error: unknown) {
    if (!Array.isArray(error) || !['series_error', 'chart_deleted'].includes(e)) return;
    if (e === 'series_error' && (error[0] !== chart.id || error[1] !== series.id)) return;
    if (e === 'chart_deleted' && error[0] !== chart.id) return;
    cleanup();
  }

  function cleanup() {
    session.removeListener('du', timescale_update);
    session.removeListener('timescale_update', timescale_update);
    session.removeListener('series_timeframe', series_timeframe);
    session.removeListener('error', error_handler);
    session.removeListener('close', cleanup);
    if (iteratorListener)
      session.removeListener('du', iteratorListener);
  }

  session.on('du', timescale_update);
  session.on('timescale_update', timescale_update);
  session.on('series_timeframe', series_timeframe);
  session.on('error', error_handler);
  session.on("close", cleanup);

  return request(session, 'create_series', [
    chart.id,
    series.id,
    series.id, // turnaroundId
    symbol.id,
    timeframe,
    count,
    range ? (Array.isArray(range) ? `r,${range[0]}:${range[1]}` : range) : ""
  ]).then(() => series);
}

export type Input =
  | { type: "integer", value: number }
  | { type: "source", value: string }
  | { type: "text", value: string }
  | { type: "float", value: number }
  | { type: "bool", value: boolean }
  | { type: "resolution", value: string }
  | { type: "symbol", value: string }
  | { type: "color", value: string }
  | { type: "timeframe", value: string }
  | { type: string, value: any };

export interface Indicator {
  id: string;
  metadata?: {
    text: string;
    pineId?: string;
    pineVersion?: string;
    [key: string]: unknown;
  },
  parameters?: Array<Input> | Record<string, unknown>;
}

export interface Study {
  id: string;
  series: Series;
  history: number[][];
  modify: (parameters: Array<{
    v: number;
    f: boolean;
    t: string;
  }> | Record<string, unknown>) => Promise<Study>;
  close(): Promise<void>;
  stream(): AsyncIterableIterator<number[]>;
  [Symbol.iterator](): IterableIterator<number[]>;
}

export function createStudy(session: Session, chart: Chart, series: Series, indicator: Indicator): Promise<Study> {
  let iteratorListener: (payload: unknown[]) => void;
  const timeseries: Record<string, number[][]> = {};
  const study: Study = {
    id: `study_${useId()}`,
    series,
    history: [],
    modify(options) {
      const parameters = Array.isArray(options) ? options.reduce((acc, param, index) => {
        acc[`in_${index}`] = { v: (param as any).value, f: true, t: (param as any).type }
        return acc;
      }, {} as Record<string, unknown>) : options;

      return request(session, 'modify_study', [
        chart.id,
        study.id,
        indicator.id,
        parameters
      ]).then(() => study);
    },
    close: () => request(session, 'remove_study', [chart.id, study.id]).then(() => {
      if (session.listenerCount('study_error') > 0)
        session.emit('study_error', "study_deleted", [chart.id, study.id, "study session closed"]);
    }),
    stream() {
      let currentTime = study.history.length > 0 ? study.history[0][0] : 0;
      return {
        next: () => {
          const nextRecord = study.history.find(record => record[0] > currentTime);
          // if (!nextRecord && series.scope?.to) return Promise.resolve({ done: true, value: undefined });
          if (!nextRecord) {
            session.send("request_more_tickmarks", [chart.id, series.id, 1]);
            return new Promise(resolve => {
              function listener(payload: unknown[]) {
                if (!Array.isArray(payload) || payload[0] !== chart.id || typeof (payload[1] as Record<string, any>)?.[study.id] === "undefined") return;
                const studyData = (payload[1] as Record<string, any>)[study.id];
                const data = (studyData.st as Array<{ v: number[] }>).map(i => i.v);
                study.history.push(...data);
                study.history.sort((a, b) => a[0] - b[0]);
                // Remove duplicates.
                study.history = study.history.filter((item, i, self) => i === self.findIndex((t) => t[0] === item[0]));
                const nextRecord = study.history.find(record => record[0] > currentTime);
                if (!nextRecord) return;
                currentTime = nextRecord[0];
                resolve({ done: false, value: nextRecord });
                session.removeListener('du', listener);
                session.send("request_more_tickmarks", [chart.id, series.id, 1]);
              }
              session.on('du', listener);
              iteratorListener = listener;
            });
          }
          currentTime = nextRecord[0];
          return Promise.resolve({ done: false, value: nextRecord });
        },
        [Symbol.asyncIterator]() {
          return this;
        }
      };
    },
    [Symbol.iterator]() {
      let currentTime = study.history.length > 0 ? study.history[0][0] : 0;
      return {
        next: () => {
          const nextRecord = study.history.find(record => record[0] > currentTime);
          if (!nextRecord) return { done: true, value: undefined };
          currentTime = nextRecord[0];
          return { done: false, value: nextRecord };
        },
        [Symbol.iterator]() {
          return this;
        }
      };
    }
  };

  function refreshHistory() {
    timeseries[series.timeframe] = timeseries[series.timeframe] || [];
    study.history = timeseries[series.timeframe].filter(record => {
      if (series.scope && series.scope.from && series.scope.to) {
        return record[0] >= series.scope.from && record[0] <= series.scope.to;
      }

      // If not range is set, trim to the first and last timestamp of the series.
      if (series.history.length > 0) {
        const firstTimestamp = series.history[0][0];
        const lastTimestamp = series.history[series.history.length - 1][0];
        return record[0] >= firstTimestamp && record[0] <= lastTimestamp;
      }

      return true; // If no range is set, return all records
    });
  }

  function timescale_update(payload: unknown[]) {
    if (!Array.isArray(payload) || payload[0] !== chart.id || typeof (payload[1] as Record<string, any>)?.[study.id] === "undefined") return;
    const studyData = (payload[1] as Record<string, any>)[study.id];
    const data = (studyData.st as Array<{ v: number[] }>).map(i => i.v);
    timeseries[series.timeframe] = timeseries[series.timeframe] || [];
    timeseries[series.timeframe] = timeseries[series.timeframe].concat(data);
    // Sort the history by timestamp
    timeseries[series.timeframe].sort((a, b) => a[0] - b[0]);
    // Remove duplicates.
    timeseries[series.timeframe] = timeseries[series.timeframe].filter((item, i, self) => i === self.findIndex((t) => t[0] === item[0]));
    refreshHistory();
  }

  function error_handler(e: string, error: unknown) {
    if (!Array.isArray(error) || !['study_error', 'series_deleted', 'chart_deleted'].includes(e)) return;
    if (e === 'study_error' && (error[0] !== chart.id || error[1] !== study.id)) return;
    if (e === 'chart_deleted' && error[0] !== chart.id) return;
    if (e === 'series_deleted' && error[0] !== chart.id && error[1] !== series.id) return;
    cleanup();
  }

  function cleanup() {
    session.removeListener('du', timescale_update);
    session.removeListener('timescale_update', timescale_update);
    session.removeListener('error', error_handler);
    session.removeListener('close', cleanup);
    if (iteratorListener)
      session.removeListener('du', iteratorListener);
  }

  session.on('du', timescale_update);
  session.on('timescale_update', timescale_update);
  session.on('error', error_handler);
  session.on("close", cleanup);

  const parameters = Array.isArray(indicator.parameters) ? indicator.parameters.reduce((acc, param, index) => {
    acc[`in_${index}`] = { v: param.value, f: true, t: param.type }
    return acc;
  }, {} as Record<string, unknown>) : indicator.parameters;

  return request(session, 'create_study', [
    chart.id,
    study.id,
    study.id, // turnaroundId
    series.id,
    indicator.id,
    Object.assign(indicator.metadata ?? {}, parameters ?? {})
  ]).then(() => study);
}

export function createScript(session: Session, chart: Chart, series: Series, script: string, parameters: Input[] = [], metadata: Record<string, unknown> = {}): Promise<Study> {
  return createStudy(session, chart, series, {
    id: "Script@tv-scripting-101!",
    metadata: {
      text: script,
      ...metadata
    },
    parameters
  })
}

export interface Quote {
  id: string;
  symbol: string;
  exchange: string;
  reports: Report[]
}

export function createQuote(session: Session, symbol: string, exchange: string): Promise<Quote> {
  const quote: Quote = {
    id: `quote_${useId()}`,
    symbol,
    exchange,
    reports: [],
  }

  return new Promise((resolve, reject) => {
    function data_handler(payload: unknown[]) {
      if (!Array.isArray(payload) || payload[0] !== quote.id) return;
      const data = payload[1] as { n?: string, s?: string, errmsg?: string, v?: any };
      if (data?.n !== `${exchange}:${symbol}`) return;
      if (data?.s === "error") {
        reject(new Error(`Quote error for ${symbol} on ${exchange}: ${data.errmsg}`));
        cleanup();
      };
      if (!data?.v?.fiscal_period_end_fy_h) return; // Skip if it's just an ask/bid update

      const v = data.v;

      delete v.revenues_fy_h;
      delete v.earnings_fy_h;
      delete v.revenues_fq_h;
      delete v.earnings_fq_h;

      // Create yearly reports
      {
        const fiscalLabels = v["fiscal_period_end_fy_h"];
        const keys = Object.keys(v).filter(k => k.endsWith("_fy_h"));

        // Assume data is most recent first; reverse to match ascending fiscal years
        const years = [...fiscalLabels].reverse();

        const fieldValues: Record<string, unknown[]> = {};
        for (const key of keys) {
          const values = v[key];
          fieldValues[key] = Array.isArray(values) ? [...values].reverse() : [];
        }

        for (let i = 0; i < years.length; i++) {
          const entry: Record<string, unknown> = { date: years[i], symbol };
          for (const key of keys) {
            const cleanKey = key.replace(/_fy_h$/, "");
            const values = fieldValues[key];
            if (i < values.length) entry[cleanKey] = values[i];
          }
          entry.type = "annual";
          entry.date = new Date((entry.date as number) * 1000).toISOString().split('T')[0]; // Format date to YYYY-MM-DD
          quote.reports.push(entry as unknown as Report);
        }
      }

      // Create quarterly reports
      {
        const fiscalLabels = v["fiscal_period_fq_h"];
        const keys = Object.keys(v).filter(k => k.endsWith("_fq_h"));

        const quarters = [...fiscalLabels].reverse();
        const fieldValues: Record<string, unknown[]> = {};
        for (const key of keys) {
          const values = v[key];
          fieldValues[key] = Array.isArray(values) ? [...values].reverse() : [];
        }

        for (let i = 0; i < quarters.length; i++) {
          const entry: Record<string, unknown> = { date: quarters[i], symbol };
          for (const key of keys) {
            const cleanKey = key.replace(/_fq_h$/, "");
            const values = fieldValues[key];
            if (i < values.length) entry[cleanKey] = values[i];
          }
          entry.type = "quarterly";
          quote.reports.push(entry as unknown as Report);
        }
      }

      session.send('quote_delete_session', [quote.id]);
      cleanup();
      resolve(quote);
    }
    function cleanup() {
      session.removeListener('qsd', data_handler);
      session.removeListener('close', cleanup);
    }
    session.on('qsd', data_handler);
    session.on('close', cleanup);

    session.send('quote_create_session', [quote.id]);
    session.send("quote_add_symbols", [quote.id, `${exchange}:${symbol}`]);
    session.send("quote_fast_symbols", [quote.id, `${exchange}:${symbol}`]);
  });
}