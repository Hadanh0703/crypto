import { Activity, BarChart3, Cpu, Newspaper, TrendingUp } from 'lucide-react';
import { NavLink, Outlet, useLocation, useNavigate, useSearchParams } from 'react-router-dom';
import { type TickerSymbol, tickers } from '../constants/chart';

const navItems = [
  { label: 'Overview', to: '/overview', icon: TrendingUp },
  { label: 'Market Terminal', to: '/chart', icon: BarChart3 },
  { label: 'ML Anomalies & Signals', to: '/signals', icon: Cpu },
  { label: 'News & Sentiment', to: '/news-impact', icon: Activity },
  { label: 'News Feed', to: '/news', icon: Newspaper },
];

function getValidSymbol(raw: string | null): TickerSymbol {
  return tickers.includes(raw as TickerSymbol) ? (raw as TickerSymbol) : 'BTCUSDT';
}

export function DashboardLayout() {
  const [searchParams, setSearchParams] = useSearchParams();
  const location = useLocation();
  const navigate = useNavigate();

  const activeSymbol = getValidSymbol(searchParams.get('symbol'));

  function handleTickerChange(nextSymbol: TickerSymbol) {
    if (location.pathname === '/chart' || location.pathname === '/news-impact' || location.pathname === '/signals') {
      setSearchParams((current) => {
        const next = new URLSearchParams(current);
        next.set('symbol', nextSymbol);
        return next;
      });
      return;
    }

    navigate(`/chart?symbol=${nextSymbol}`);
  }

  return (
    <div className="app-shell">
      <aside className="sidebar" aria-label="Sidebar navigation">
        <div className="sidebar-brand">
          <p className="eyebrow">Crypto BigData</p>
          <h1>Intelligence Dashboard</h1>
        </div>

        <div className="mt-4 rounded-lg border border-slate-200 bg-slate-50/70 p-3">
          <label className="block text-[11px] font-bold uppercase tracking-wider text-slate-500" htmlFor="sidebar-ticker">
            Active Pair
          </label>
          <select
            id="sidebar-ticker"
            value={activeSymbol}
            onChange={(event) => handleTickerChange(event.target.value as TickerSymbol)}
            className="mt-1.5 h-9 w-full rounded-md border border-slate-300 bg-white px-2.5 text-sm font-bold text-slate-900 shadow-sm outline-none focus:border-blue-600 focus:ring-2 focus:ring-blue-100"
          >
            {tickers.map((ticker) => (
              <option key={ticker} value={ticker}>
                {ticker}
              </option>
            ))}
          </select>
          <div className="mt-2 flex flex-wrap gap-1.5">
            {(['BTCUSDT', 'ETHUSDT', 'SOLUSDT'] as const).map((quickTicker) => (
              <button
                key={quickTicker}
                type="button"
                onClick={() => handleTickerChange(quickTicker)}
                className={`rounded px-2 py-0.5 text-[11px] font-bold transition ${
                  activeSymbol === quickTicker
                    ? 'bg-blue-600 text-white'
                    : 'border border-slate-200 bg-white text-slate-600 hover:border-blue-300 hover:text-blue-700'
                }`}
              >
                {quickTicker.replace('USDT', '')}
              </button>
            ))}
          </div>
        </div>

        <nav className="sidebar-nav" aria-label="Dashboard navigation">
          {navItems.map((item) => {
            const Icon = item.icon;
            const target =
              item.to === '/chart' || item.to === '/news-impact' || item.to === '/signals'
                ? `${item.to}?symbol=${activeSymbol}`
                : item.to;

            return (
              <NavLink
                key={item.to}
                to={target}
                className={({ isActive }) => (isActive ? 'nav-link active' : 'nav-link')}
              >
                <Icon size={17} aria-hidden="true" />
                <span>{item.label}</span>
              </NavLink>
            );
          })}
        </nav>

        <div className="mt-auto hidden pt-4 lg:block">
          <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2.5 text-xs text-slate-500">
            <div className="flex items-center gap-2 font-semibold text-slate-700">
              <span className="h-2 w-2 rounded-full bg-emerald-500" aria-hidden="true" />
              Lambda Pipeline Live
            </div>
            <p className="mt-1 text-[11px] text-slate-500">Kafka · Spark · ClickHouse · MLlib</p>
          </div>
        </div>
      </aside>

      <main className="main-workspace">
        <div className="page-shell">
          <Outlet />
        </div>
      </main>
    </div>
  );
}
