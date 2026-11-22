import { ArrowUpRight, Cpu, RefreshCw, TrendingDown, TrendingUp, Zap } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import { Link, useSearchParams } from 'react-router-dom';
import { VolumeSpikeList } from '../components/overview/VolumeSpikeList';
import { overviewApi } from '../services/overviewApi';
import { signalsApi } from '../services/signalsApi';
import type { VolumeSpike } from '../types/overview';
import type { Signal } from '../types/signal';
import { formatMsDate } from '../utils/format';

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : 'Unknown error';
}

function signalBadgeClass(type?: string | null) {
  const normalized = type?.toLowerCase() ?? '';
  if (normalized.includes('pump') || normalized.includes('buy')) return 'bg-emerald-50 text-emerald-700 border-emerald-200';
  if (normalized.includes('dump') || normalized.includes('sell')) return 'bg-rose-50 text-rose-700 border-rose-200';
  return 'bg-purple-50 text-purple-700 border-purple-200';
}

export function SignalsPage() {
  const [searchParams] = useSearchParams();
  const [refreshKey, setRefreshKey] = useState(0);
  const [tickerFilter, setTickerFilter] = useState<string>('');
  const [signals, setSignals] = useState<{ data: Signal[]; loading: boolean; error?: string }>({
    data: [],
    loading: true,
  });
  const [spikes, setSpikes] = useState<{ data: VolumeSpike[]; loading: boolean; error?: string }>({
    data: [],
    loading: true,
  });

  const activeSymbol = searchParams.get('symbol') ?? '';

  useEffect(() => {
    let mounted = true;
    setSignals((current) => ({ ...current, loading: true, error: undefined }));
    setSpikes((current) => ({ ...current, loading: true, error: undefined }));

    Promise.allSettled([
      signalsApi.getActiveSignals(50),
      overviewApi.getVolumeSpikes(2, 12),
    ]).then(([signalsRes, spikesRes]) => {
      if (!mounted) return;
      setSignals(
        signalsRes.status === 'fulfilled'
          ? { data: signalsRes.value, loading: false }
          : { data: [], loading: false, error: errorMessage(signalsRes.reason) },
      );
      setSpikes(
        spikesRes.status === 'fulfilled'
          ? { data: spikesRes.value, loading: false }
          : { data: [], loading: false, error: errorMessage(spikesRes.reason) },
      );
    });

    return () => {
      mounted = false;
    };
  }, [refreshKey]);

  const filteredSignals = useMemo(() => {
    const query = tickerFilter.trim().toUpperCase();
    if (!query) return signals.data;
    return signals.data.filter((s) => s.ticker.toUpperCase().includes(query));
  }, [signals.data, tickerFilter]);

  const stats = useMemo(() => {
    let pumps = 0;
    let dumps = 0;
    signals.data.forEach((s) => {
      const t = s.type.toUpperCase();
      if (t.includes('PUMP') || t.includes('BUY')) pumps += 1;
      if (t.includes('DUMP') || t.includes('SELL')) dumps += 1;
    });
    return { total: signals.data.length, pumps, dumps };
  }, [signals.data]);

  return (
    <div className="space-y-6">
      <section className="flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <p className="eyebrow">Intelligence</p>
          <h2 className="mt-1.5 text-2xl font-bold text-slate-950">ML Anomalies &amp; Signals</h2>
          <p className="mt-1 text-sm text-slate-500">
            Spark Batch rule-based Pump/Dump signals, KMeans clustering anomaly monitor, and 24h volume spike radar.
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-3">
          <input
            type="text"
            value={tickerFilter}
            onChange={(event) => setTickerFilter(event.target.value)}
            placeholder={activeSymbol ? `Filter ticker (e.g. ${activeSymbol})` : 'Filter ticker...'}
            className="h-10 rounded-lg border border-slate-300 bg-white px-3 text-sm text-slate-900 shadow-sm outline-none focus:border-blue-600"
          />
          <button
            type="button"
            onClick={() => setRefreshKey((k) => k + 1)}
            className="inline-flex h-10 items-center gap-2 rounded-lg border border-blue-200 bg-blue-50 px-4 text-sm font-bold text-blue-700 transition hover:bg-blue-100"
          >
            <RefreshCw size={15} aria-hidden="true" />
            Refresh
          </button>
        </div>
      </section>

      {/* Summary KPI Strip */}
      <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <div className="rounded-lg border border-slate-200 bg-white p-5 shadow-sm">
          <div className="flex items-center justify-between text-xs font-bold uppercase text-slate-500">
            <span>Active Signals</span>
            <Zap size={16} className="text-blue-600" />
          </div>
          <p className="mt-2 text-2xl font-bold text-slate-950">{stats.total}</p>
          <p className="mt-1 text-xs text-slate-500">ClickHouse `signals` feed</p>
        </div>

        <div className="rounded-lg border border-slate-200 bg-white p-5 shadow-sm">
          <div className="flex items-center justify-between text-xs font-bold uppercase text-slate-500">
            <span>Pump Alerts</span>
            <TrendingUp size={16} className="text-emerald-600" />
          </div>
          <p className="mt-2 text-2xl font-bold text-emerald-700">{stats.pumps}</p>
          <p className="mt-1 text-xs text-slate-500">High-volume upward surges</p>
        </div>

        <div className="rounded-lg border border-slate-200 bg-white p-5 shadow-sm">
          <div className="flex items-center justify-between text-xs font-bold uppercase text-slate-500">
            <span>Dump Alerts</span>
            <TrendingDown size={16} className="text-rose-600" />
          </div>
          <p className="mt-2 text-2xl font-bold text-rose-700">{stats.dumps}</p>
          <p className="mt-1 text-xs text-slate-500">High-volume downward drops</p>
        </div>

        <div className="rounded-lg border border-slate-200 bg-white p-5 shadow-sm">
          <div className="flex items-center justify-between text-xs font-bold uppercase text-slate-500">
            <span>Spark MLlib Model</span>
            <Cpu size={16} className="text-purple-600" />
          </div>
          <p className="mt-2 text-lg font-bold text-purple-700">KMeans (k=4)</p>
          <p className="mt-1 text-xs text-slate-500">Volume · Price Δ% · Volatility σ</p>
        </div>
      </section>

      {/* Split Grid: Signals Table + Volume Spikes */}
      <div className="grid grid-cols-1 gap-6 xl:grid-cols-12">
        <section className="rounded-lg border border-slate-200 bg-white shadow-sm xl:col-span-8">
          <div className="border-b border-slate-200 px-5 py-4">
            <h3 className="text-base font-bold text-slate-950">Detected Signals Feed</h3>
          </div>

          {signals.loading ? (
            <div className="p-6 text-sm text-slate-500">Loading active signals...</div>
          ) : signals.error ? (
            <div className="p-6 text-sm text-red-700">Failed to load signals: {signals.error}</div>
          ) : filteredSignals.length === 0 ? (
            <div className="p-6 text-sm text-slate-500">No matching signals found.</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm">
                <thead className="border-b border-slate-200 bg-slate-50 text-xs font-bold uppercase text-slate-500">
                  <tr>
                    <th className="px-5 py-3">Ticker</th>
                    <th className="px-5 py-3">Signal</th>
                    <th className="px-5 py-3">Price</th>
                    <th className="px-5 py-3">Reason</th>
                    <th className="px-5 py-3">Detected At</th>
                    <th className="px-5 py-3 text-right">Terminal</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {filteredSignals.map((signal) => {
                    const price =
                      typeof signal.metadata?.price === 'number' || typeof signal.metadata?.price === 'string'
                        ? Number(signal.metadata.price)
                        : null;
                    const reason = typeof signal.metadata?.reason === 'string' ? signal.metadata.reason : '-';

                    return (
                      <tr key={signal.id} className="hover:bg-slate-50/70">
                        <td className="px-5 py-3.5 font-bold text-slate-950">{signal.ticker}</td>
                        <td className="px-5 py-3.5">
                          <span
                            className={`rounded-full border px-2.5 py-0.5 text-xs font-bold ${signalBadgeClass(signal.type)}`}
                          >
                            {signal.type}
                          </span>
                        </td>
                        <td className="px-5 py-3.5 font-semibold text-slate-700">
                          {price !== null && Number.isFinite(price) ? `$${price.toLocaleString()}` : '-'}
                        </td>
                        <td className="px-5 py-3.5 text-slate-600">{reason}</td>
                        <td className="px-5 py-3.5 text-xs text-slate-500">{formatMsDate(signal.detectedAtMs)}</td>
                        <td className="px-5 py-3.5 text-right">
                          <Link
                            to={`/chart?symbol=${signal.ticker}`}
                            className="inline-flex items-center gap-1 rounded-md border border-blue-200 bg-blue-50 px-2.5 py-1 text-xs font-bold text-blue-700 hover:bg-blue-100"
                          >
                            Inspect
                            <ArrowUpRight size={13} />
                          </Link>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </section>

        <div className="xl:col-span-4">
          <VolumeSpikeList spikes={spikes.data} loading={spikes.loading} error={spikes.error} />
        </div>
      </div>
    </div>
  );
}
