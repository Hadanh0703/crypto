import { Activity, Cpu, Newspaper, RefreshCw } from 'lucide-react';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { CandlestickChart } from '../components/chart/CandlestickChart';
import { IndicatorToggle, type IndicatorKey } from '../components/chart/IndicatorToggle';
import { IntervalSelector } from '../components/chart/IntervalSelector';
import { RsiPanel } from '../components/chart/RsiPanel';
import { TickerSelector } from '../components/chart/TickerSelector';
import { VolumeChart } from '../components/chart/VolumeChart';
import { NewsDetailModal } from '../components/news/NewsDetailModal';
import { type TickerSymbol, tickers } from '../constants/chart';
import { indicatorsApi } from '../services/indicatorsApi';
import { marketApi } from '../services/marketApi';
import { newsApi } from '../services/newsApi';
import { signalsApi } from '../services/signalsApi';
import type { IndicatorResponse } from '../types/indicator';
import type { Candle } from '../types/market';
import type { NewsItem } from '../types/news';
import type { Signal } from '../types/signal';
import { formatMsDate, formatTimestampDate, sentimentLabel } from '../utils/format';
import { type ChartInterval, intervalMap } from '../utils/time';

type ChartState = {
  candles: Candle[];
  loading: boolean;
  error?: string;
  serverTime?: number;
};

type IndicatorState = {
  data: IndicatorResponse | null;
  loading: boolean;
  error?: string;
};

type SignalsState = {
  data: Signal[];
  loading: boolean;
  error?: string;
};

type NewsState = {
  data: NewsItem[];
  loading: boolean;
  error?: string;
};

const intervalOptions: ChartInterval[] = ['5m', '15m', '1h', '4h', '1d'];
const defaultIndicators: Record<IndicatorKey, boolean> = {
  ma20: true,
  ma50: true,
  bollinger: false,
  rsi: true,
};

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : 'Unknown error';
}

function getTickerParam(value: string | null): TickerSymbol {
  return tickers.includes(value as TickerSymbol) ? (value as TickerSymbol) : 'BTCUSDT';
}

function getIntervalParam(value: string | null): ChartInterval {
  return intervalOptions.includes(value as ChartInterval) ? (value as ChartInterval) : '5m';
}

function signalBadgeClass(type?: string | null) {
  const normalized = type?.toLowerCase() ?? '';
  if (normalized.includes('pump') || normalized.includes('buy')) return 'bg-emerald-50 text-emerald-700 border-emerald-200';
  if (normalized.includes('dump') || normalized.includes('sell')) return 'bg-rose-50 text-rose-700 border-rose-200';
  return 'bg-purple-50 text-purple-700 border-purple-200';
}

function sentimentBadgeClass(sentiment?: string | null) {
  const normalized = sentiment?.toLowerCase() ?? 'neutral';
  if (normalized === 'positive') return 'bg-emerald-50 text-emerald-700';
  if (normalized === 'negative') return 'bg-rose-50 text-rose-700';
  return 'bg-amber-50 text-amber-700';
}

function formatConfidence(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return 'N/A';
  const percent = value <= 1 ? value * 100 : value;
  return `${percent.toFixed(0)}%`;
}

export function ChartPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [refreshKey, setRefreshKey] = useState(0);
  const [state, setState] = useState<ChartState>({ candles: [], loading: true });
  const [indicatorToggles, setIndicatorToggles] = useState(defaultIndicators);
  const [indicators, setIndicators] = useState<IndicatorState>({ data: null, loading: true });
  const [showSignals, setShowSignals] = useState(true);
  const [signals, setSignals] = useState<SignalsState>({ data: [], loading: true });
  const [marketSignals, setMarketSignals] = useState<SignalsState>({ data: [], loading: true });
  const [latestNews, setLatestNews] = useState<NewsState>({ data: [], loading: true });
  const [selectedNews, setSelectedNews] = useState<NewsItem | null>(null);

  const symbol = getTickerParam(searchParams.get('symbol'));
  const interval = getIntervalParam(searchParams.get('interval'));
  const intervalMs = intervalMap[interval];

  const rangeLabel = useMemo(() => {
    if (state.candles.length === 0) return 'Last 24 hours';
    const first = state.candles[0];
    const last = state.candles[state.candles.length - 1];
    return `${formatMsDate(first.openTime)} - ${formatMsDate(last.closeTime)}`;
  }, [state.candles]);

  const activeMaLabels = [
    indicatorToggles.ma20 ? 'MA20' : null,
    indicatorToggles.ma50 ? 'MA50' : null,
    indicatorToggles.bollinger ? 'Bollinger Bands' : null,
  ].filter(Boolean);

  const missingAdvancedIndicators = Boolean(
    indicators.data &&
      indicatorToggles.bollinger &&
      (!indicators.data.bollingerUpper?.length ||
        !indicators.data.bollingerMiddle?.length ||
        !indicators.data.bollingerLower?.length),
  );

  const visibleSignals = useMemo(
    () => signals.data.filter((signal) => signal.ticker === symbol),
    [signals.data, symbol],
  );

  const panelSignals = useMemo(() => {
    if (visibleSignals.length > 0) return visibleSignals;
    return marketSignals.data;
  }, [marketSignals.data, visibleSignals]);

  const handleSymbolChange = useCallback(
    (nextSymbol: TickerSymbol) => {
      setSearchParams((current) => {
        const next = new URLSearchParams(current);
        next.set('symbol', nextSymbol);
        next.set('interval', getIntervalParam(current.get('interval')));
        return next;
      });
      setState({ candles: [], loading: true });
      setIndicators({ data: null, loading: true });
      if (showSignals) setSignals((current) => ({ ...current, loading: true, error: undefined }));
    },
    [setSearchParams, showSignals],
  );

  const handleIntervalChange = useCallback(
    (nextInterval: ChartInterval) => {
      setSearchParams((current) => {
        const next = new URLSearchParams(current);
        next.set('symbol', getTickerParam(current.get('symbol')));
        next.set('interval', nextInterval);
        return next;
      });
      setState({ candles: [], loading: true });
      setIndicators({ data: null, loading: true });
      if (showSignals) setSignals((current) => ({ ...current, loading: true, error: undefined }));
    },
    [setSearchParams, showSignals],
  );

  function handleRefresh() {
    setState({ candles: [], loading: true });
    setIndicators({ data: null, loading: true });
    if (showSignals) setSignals((current) => ({ ...current, loading: true, error: undefined }));
    setRefreshKey((key) => key + 1);
  }

  useEffect(() => {
    let mounted = true;

    async function loadChart() {
      setState((current) => ({ ...current, loading: true, error: undefined }));
      setIndicators((current) => ({ ...current, loading: true, error: undefined }));

      const toTime = Date.now();
      let fromTime = toTime - 180 * 24 * 60 * 60 * 1000;

      if (intervalMs === 300000) {
        fromTime = toTime - 7 * 24 * 60 * 60 * 1000;
      } else if (intervalMs === 900000) {
        fromTime = toTime - 14 * 24 * 60 * 60 * 1000;
      } else if (intervalMs === 3600000) {
        fromTime = toTime - 90 * 24 * 60 * 60 * 1000;
      }

      const [serverTimeResult, candlesResult, indicatorsResult] = await Promise.allSettled([
        marketApi.getServerTime().catch(() => undefined),
        marketApi.getKlines({
          ticker: symbol,
          interval: intervalMs,
          fromTime,
          toTime,
        }),
        indicatorsApi.getIndicators({
          ticker: symbol,
          interval: intervalMs,
          fromTime,
          toTime,
          indicators: 'ma,rsi,ema,bb',
        }),
      ]);

      if (!mounted) return;

      if (candlesResult.status === 'fulfilled') {
        setState({
          candles: candlesResult.value,
          loading: false,
          serverTime: serverTimeResult.status === 'fulfilled' ? serverTimeResult.value : undefined,
        });
      } else {
        setState({ candles: [], loading: false, error: errorMessage(candlesResult.reason) });
      }

      if (indicatorsResult.status === 'fulfilled') {
        setIndicators({ data: indicatorsResult.value, loading: false });
      } else {
        setIndicators({ data: null, loading: false, error: errorMessage(indicatorsResult.reason) });
      }
    }

    void loadChart();

    return () => {
      mounted = false;
    };
  }, [symbol, intervalMs, refreshKey]);

  useEffect(() => {
    let mounted = true;

    setSignals((current) => ({ ...current, loading: true, error: undefined }));
    setMarketSignals((current) => ({ ...current, loading: true, error: undefined }));
    setLatestNews((current) => ({ ...current, loading: true, error: undefined }));

    Promise.allSettled([
      signalsApi.getActiveSignals(50, symbol),
      signalsApi.getActiveSignals(10),
      newsApi.getLatestNews(8),
    ]).then(([tickerSignalsRes, allSignalsRes, newsRes]) => {
      if (!mounted) return;

      setSignals(
        tickerSignalsRes.status === 'fulfilled'
          ? { data: tickerSignalsRes.value, loading: false }
          : { data: [], loading: false, error: errorMessage(tickerSignalsRes.reason) },
      );

      setMarketSignals(
        allSignalsRes.status === 'fulfilled'
          ? { data: allSignalsRes.value, loading: false }
          : { data: [], loading: false, error: errorMessage(allSignalsRes.reason) },
      );

      setLatestNews(
        newsRes.status === 'fulfilled'
          ? { data: newsRes.value, loading: false }
          : { data: [], loading: false, error: errorMessage(newsRes.reason) },
      );
    });

    return () => {
      mounted = false;
    };
  }, [refreshKey, symbol]);

  return (
    <div className="space-y-5">
      <section className="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
        <div>
          <p className="eyebrow">Market Terminal</p>
          <h2 className="mt-1.5 text-2xl font-bold text-slate-950">{symbol} Unified Terminal</h2>
          <p className="mt-1 text-sm text-slate-500">
            Real-time OHLCV candlesticks, technical indicators, ML anomalies &amp; signals, and correlated market news.
          </p>
        </div>

        <div className="flex flex-wrap items-end gap-3 rounded-lg border border-slate-200 bg-white p-3.5 shadow-sm">
          <TickerSelector value={symbol} onChange={handleSymbolChange} />
          <IntervalSelector value={interval} onChange={handleIntervalChange} />
          <IndicatorToggle value={indicatorToggles} onChange={setIndicatorToggles} />
          <label className="grid gap-2 text-sm font-semibold text-slate-600">
            Overlay
            <span className="inline-flex h-10 items-center gap-2 rounded-lg border border-slate-300 bg-white px-3 text-sm font-bold text-slate-700 shadow-sm">
              <input
                checked={showSignals}
                className="h-4 w-4 accent-blue-600"
                type="checkbox"
                onChange={(event) => setShowSignals(event.target.checked)}
              />
              Signals
            </span>
          </label>
          <button
            className="inline-flex h-10 items-center justify-center gap-2 rounded-lg border border-blue-200 bg-blue-50 px-4 text-sm font-bold text-blue-700 transition hover:bg-blue-100"
            type="button"
            onClick={handleRefresh}
          >
            <RefreshCw size={16} aria-hidden="true" />
            Refresh
          </button>
        </div>
      </section>

      {/* Executive Split-Grid: ~70% Chart Stack | ~30% Live Signals & News Panel */}
      <div className="grid grid-cols-1 gap-6 xl:grid-cols-12">
        <div className="min-w-0 space-y-4 xl:col-span-8">
          <section className="rounded-lg border border-slate-200 bg-white shadow-sm">
            <div className="flex flex-col gap-2 border-b border-slate-200 px-5 py-3.5 sm:flex-row sm:items-center sm:justify-between">
              <div>
                {indicators.loading ? (
                  <p className="text-xs text-slate-400">Loading indicators...</p>
                ) : indicators.error ? (
                  <p className="text-xs text-amber-600">Indicators unavailable: {indicators.error}</p>
                ) : indicators.data &&
                  (indicators.data.ma20?.length || indicators.data.ma50?.length || indicators.data.rsi?.length) ? (
                  <p className="text-xs font-medium text-slate-500">
                    Indicators: MA20 ({indicators.data.ma20?.length ?? 0}), MA50 ({indicators.data.ma50?.length ?? 0}), BB (
                    {indicators.data.bollingerUpper?.length ?? 0}), RSI ({indicators.data.rsi?.length ?? 0})
                  </p>
                ) : (
                  <p className="text-xs text-slate-400">No indicator data for this range.</p>
                )}
                {missingAdvancedIndicators ? (
                  <p className="mt-1 text-xs text-amber-600">Some indicators are not available from API yet.</p>
                ) : null}
              </div>
              <p className="text-xs font-semibold text-slate-500">
                Server time: {state.serverTime ? formatMsDate(state.serverTime) : '-'}
              </p>
            </div>

            {state.loading ? (
              <div className="p-8 text-sm text-slate-500">Loading market candles...</div>
            ) : state.error ? (
              <div className="m-5 rounded-lg border border-red-200 bg-red-50 p-5 text-sm text-red-700">
                Chart unavailable: {state.error}
              </div>
            ) : state.candles.length === 0 ? (
              <div className="p-8 text-sm text-slate-500">No candle data found for this ticker and interval.</div>
            ) : (
              <div className="space-y-4 p-4">
                <section className="min-w-0 overflow-hidden rounded-lg border border-slate-100 bg-white">
                  <div className="flex flex-wrap items-center justify-between gap-2 border-b border-slate-100 px-4 py-3">
                    <div>
                      <h4 className="text-base font-bold text-slate-950">
                        {symbol} · {interval.toUpperCase()} OHLCV
                      </h4>
                      <p className="mt-0.5 text-xs text-slate-500">{rangeLabel}</p>
                    </div>
                    {activeMaLabels.length > 0 ? (
                      <div className="flex flex-wrap gap-1.5">
                        {activeMaLabels.map((label) => (
                          <span
                            key={label}
                            className="rounded-md border border-slate-200 bg-slate-50 px-2 py-0.5 text-[11px] font-semibold text-slate-600"
                          >
                            {label}
                          </span>
                        ))}
                      </div>
                    ) : null}
                  </div>
                  <CandlestickChart
                    candles={state.candles}
                    ma20={indicatorToggles.ma20 ? indicators.data?.ma20 : undefined}
                    ma50={indicatorToggles.ma50 ? indicators.data?.ma50 : undefined}
                    bollingerUpper={indicatorToggles.bollinger ? indicators.data?.bollingerUpper : undefined}
                    bollingerMiddle={indicatorToggles.bollinger ? indicators.data?.bollingerMiddle : undefined}
                    bollingerLower={indicatorToggles.bollinger ? indicators.data?.bollingerLower : undefined}
                    signals={showSignals ? visibleSignals : undefined}
                  />
                </section>
                <VolumeChart candles={state.candles} />
                {indicatorToggles.rsi ? <RsiPanel data={indicators.data?.rsi} /> : null}
              </div>
            )}
          </section>
        </div>

        {/* Right Analytical Panel (~30%) */}
        <aside className="min-w-0 space-y-5 xl:col-span-4">
          {/* Active Signals & KMeans ML Anomalies */}
          <section className="rounded-lg border border-slate-200 bg-white shadow-sm">
            <div className="flex items-center justify-between border-b border-slate-200 px-4 py-3.5">
              <div className="flex items-center gap-2">
                <Cpu size={16} className="text-purple-600" aria-hidden="true" />
                <h3 className="text-sm font-bold text-slate-950">Active Signals &amp; ML Anomalies</h3>
              </div>
              <span className="rounded-full bg-purple-50 px-2.5 py-0.5 text-[11px] font-bold text-purple-700">
                {visibleSignals.length > 0 ? `${visibleSignals.length} ${symbol}` : `${panelSignals.length} Market`}
              </span>
            </div>

            {signals.loading && marketSignals.loading ? (
              <div className="p-4 text-xs text-slate-500">Loading active signals...</div>
            ) : signals.error && marketSignals.error ? (
              <div className="p-4 text-xs text-amber-600">Signals unavailable: {signals.error}</div>
            ) : panelSignals.length === 0 ? (
              <div className="p-4 text-xs text-slate-500">No active signals detected for {symbol} or market.</div>
            ) : (
              <ul className="max-h-[340px] divide-y divide-slate-100 overflow-y-auto">
                {panelSignals.slice(0, 8).map((signal) => {
                  const reason =
                    typeof signal.metadata?.reason === 'string' ? signal.metadata.reason : undefined;
                  const price =
                    typeof signal.metadata?.price === 'number' || typeof signal.metadata?.price === 'string'
                      ? Number(signal.metadata.price)
                      : null;

                  return (
                    <li key={signal.id} className="px-4 py-3 hover:bg-slate-50/70">
                      <div className="flex items-center justify-between gap-2">
                        <div className="flex items-center gap-2">
                          <span className="text-sm font-bold text-slate-950">{signal.ticker}</span>
                          <span
                            className={`rounded-full border px-2 py-0.5 text-[11px] font-bold ${signalBadgeClass(signal.type)}`}
                          >
                            {signal.type}
                          </span>
                        </div>
                        <span className="text-xs font-semibold text-slate-600">
                          {formatConfidence(signal.confidence)}
                        </span>
                      </div>
                      {reason ? <p className="mt-1 text-xs text-slate-600">{reason}</p> : null}
                      <div className="mt-1.5 flex items-center justify-between text-[11px] text-slate-400">
                        <span>{formatMsDate(signal.detectedAtMs)}</span>
                        {price !== null && Number.isFinite(price) ? (
                          <span className="font-semibold text-slate-600">${price.toLocaleString()}</span>
                        ) : null}
                      </div>
                    </li>
                  );
                })}
              </ul>
            )}
          </section>

          {/* Latest Market News */}
          <section className="rounded-lg border border-slate-200 bg-white shadow-sm">
            <div className="flex items-center justify-between border-b border-slate-200 px-4 py-3.5">
              <div className="flex items-center gap-2">
                <Newspaper size={16} className="text-blue-600" aria-hidden="true" />
                <h3 className="text-sm font-bold text-slate-950">Latest Market News</h3>
              </div>
              <Activity size={14} className="text-slate-400" aria-hidden="true" />
            </div>

            {latestNews.loading ? (
              <div className="p-4 text-xs text-slate-500">Loading latest news...</div>
            ) : latestNews.error ? (
              <div className="p-4 text-xs text-amber-600">News unavailable: {latestNews.error}</div>
            ) : latestNews.data.length === 0 ? (
              <div className="p-4 text-xs text-slate-500">No recent news articles found.</div>
            ) : (
              <ul className="max-h-[420px] divide-y divide-slate-100 overflow-y-auto">
                {latestNews.data.map((item) => (
                  <li
                    key={item.id}
                    className="cursor-pointer px-4 py-3 transition-colors hover:bg-slate-50"
                    onClick={() => setSelectedNews(item)}
                  >
                    <div className="flex items-center justify-between gap-2">
                      <span
                        className={`rounded-full px-2 py-0.5 text-[11px] font-bold ${sentimentBadgeClass(item.sentiment)}`}
                      >
                        {sentimentLabel(item.sentiment)}
                      </span>
                      <span className="text-[11px] font-medium text-slate-400">
                        {formatTimestampDate(item.publishedOn)}
                      </span>
                    </div>
                    <p className="mt-1.5 line-clamp-2 text-xs font-semibold leading-snug text-slate-900">
                      {item.title ?? '-'}
                    </p>
                    <p className="mt-1 text-[11px] text-slate-500">{item.sourceName ?? '-'}</p>
                  </li>
                ))}
              </ul>
            )}
          </section>
        </aside>
      </div>

      <NewsDetailModal item={selectedNews} onClose={() => setSelectedNews(null)} />
    </div>
  );
}
