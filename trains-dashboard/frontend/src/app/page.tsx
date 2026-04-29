import { getSummary, getPerformance, getStationDelays, getOtpTrend, getRecentMovements, getHourlyCounts } from '@/lib/api'
import { HourlyChart, OtpTrendChart, TocDelaysChart, StationDelaysChart } from '@/components/Charts'
import { MovementsTable } from '@/components/MovementsTable'

function StatCard({
  label,
  value,
  sub,
  colour,
}: {
  label: string
  value: string | number
  sub?: string
  colour: string
}) {
  return (
    <div className="bg-slate-800 rounded-lg p-5 flex flex-col gap-1">
      <span className="text-slate-400 text-xs font-medium uppercase tracking-wider">{label}</span>
      <span className={`text-3xl font-bold tabular-nums ${colour}`}>{value}</span>
      {sub && <span className="text-slate-500 text-xs">{sub}</span>}
    </div>
  )
}

export default async function Dashboard() {
  const [summary, performance, stationDelays, otpTrend, movements, hourly] = await Promise.all([
    getSummary().catch(() => ({
      total_movements: 0, cancellations: 0, on_time_pct: 0, avg_delay_mins: 0,
    })),
    getPerformance().catch(() => []),
    getStationDelays().catch(() => []),
    getOtpTrend().catch(() => []),
    getRecentMovements().catch(() => []),
    getHourlyCounts().catch(() => []),
  ])

  return (
    <main className="max-w-7xl mx-auto px-4 py-8 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-100">TRUST Movement Dashboard</h1>
          <p className="text-slate-400 text-sm mt-1">UK rail movement data from the NROD TRUST feed</p>
        </div>
        <span className="text-slate-500 text-xs">Auto-refreshes every 60s</span>
      </div>

      {/* Stat cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard
          label="Movements Today"
          value={summary.total_movements.toLocaleString()}
          colour="text-blue-400"
        />
        <StatCard
          label="On Time"
          value={`${summary.on_time_pct.toFixed(1)}%`}
          sub="of all movements today"
          colour={summary.on_time_pct >= 80 ? 'text-green-400' : 'text-yellow-400'}
        />
        <StatCard
          label="Cancellations Today"
          value={summary.cancellations.toLocaleString()}
          colour="text-red-400"
        />
        <StatCard
          label="Avg Delay"
          value={`${summary.avg_delay_mins.toFixed(1)} min`}
          sub="when late (today)"
          colour="text-yellow-400"
        />
      </div>

      {/* Charts — row 1: volume + OTP trend; row 2: TOC delays + station delays */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <HourlyChart data={hourly} />
        <OtpTrendChart data={otpTrend} />
        <TocDelaysChart data={performance} />
        <StationDelaysChart data={stationDelays} />
      </div>

      {/* Movements table */}
      <MovementsTable data={movements} />
    </main>
  )
}
