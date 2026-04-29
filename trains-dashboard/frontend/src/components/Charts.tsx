'use client'

import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { HourlyCount, OtpHour, StationDelay, TocPerformance } from '@/lib/api'

// ---------------------------------------------------------------------------
// Hourly movements bar chart
// ---------------------------------------------------------------------------

export function HourlyChart({ data }: { data: HourlyCount[] }) {
  const formatted = data.map((d) => ({
    hour: new Date(d.hour).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' }),
    count: d.count,
  }))

  return (
    <div className="bg-slate-800 rounded-lg p-4">
      <h2 className="text-slate-400 text-sm font-medium uppercase tracking-wider mb-4">
        Movements per Hour (last 24h)
      </h2>
      <ResponsiveContainer width="100%" height={240}>
        <BarChart data={formatted} margin={{ top: 4, right: 8, left: -8, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
          <XAxis dataKey="hour" tick={{ fill: '#94a3b8', fontSize: 11 }} interval="preserveStartEnd" />
          <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} />
          <Tooltip
            contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: 6 }}
            labelStyle={{ color: '#e2e8f0' }}
            itemStyle={{ color: '#60a5fa' }}
          />
          <Bar dataKey="count" fill="#3b82f6" radius={[3, 3, 0, 0]} name="Movements" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

// ---------------------------------------------------------------------------
// On-time % trend line chart
// ---------------------------------------------------------------------------

function otpLineColor(pct: number): string {
  if (pct >= 90) return '#22c55e'
  if (pct >= 75) return '#eab308'
  return '#ef4444'
}

export function OtpTrendChart({ data }: { data: OtpHour[] }) {
  const formatted = data.map((d) => ({
    hour: new Date(d.hour).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' }),
    on_time_pct: d.on_time_pct,
    total: d.total,
  }))

  return (
    <div className="bg-slate-800 rounded-lg p-4">
      <h2 className="text-slate-400 text-sm font-medium uppercase tracking-wider mb-4">
        On-Time % per Hour (last 24h)
      </h2>
      <ResponsiveContainer width="100%" height={240}>
        <LineChart data={formatted} margin={{ top: 4, right: 8, left: -8, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
          <XAxis dataKey="hour" tick={{ fill: '#94a3b8', fontSize: 11 }} interval="preserveStartEnd" />
          <YAxis domain={[0, 100]} tick={{ fill: '#94a3b8', fontSize: 11 }} unit="%" />
          <Tooltip
            contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: 6 }}
            labelStyle={{ color: '#e2e8f0' }}
            formatter={(value: number, _name: string, props) => [
              `${value}% on time (${props.payload.total.toLocaleString()} movements)`,
              'On-time %',
            ]}
          />
          <Line
            type="monotone"
            dataKey="on_time_pct"
            stroke="#60a5fa"
            strokeWidth={2}
            dot={(props) => {
              const { cx, cy, payload } = props
              return <circle key={payload.hour} cx={cx} cy={cy} r={3} fill={otpLineColor(payload.on_time_pct)} stroke="none" />
            }}
            activeDot={{ r: 5 }}
            name="On-time %"
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Top delayed TOCs bar chart
// ---------------------------------------------------------------------------

export function TocDelaysChart({ data }: { data: TocPerformance[] }) {
  const chartData = data
    .filter((d) => d.late > 0)
    .sort((a, b) => b.late - a.late)
    .map((d) => ({ ...d, label: d.toc_name || d.toc_id }))

  const maxLabelLen = Math.max(...chartData.map((d) => d.label.length), 1)
  const axisWidth = Math.min(maxLabelLen * 7, 160)

  return (
    <div className="bg-slate-800 rounded-lg p-4">
      <h2 className="text-slate-400 text-sm font-medium uppercase tracking-wider mb-4">
        Most Delayed Operators (last 24h)
      </h2>
      <ResponsiveContainer width="100%" height={320}>
        <BarChart data={chartData} layout="vertical" margin={{ top: 4, right: 24, left: 4, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#334155" horizontal={false} />
          <XAxis type="number" tick={{ fill: '#94a3b8', fontSize: 11 }} />
          <YAxis
            dataKey="label"
            type="category"
            tick={{ fill: '#94a3b8', fontSize: 10 }}
            width={axisWidth}
          />
          <Tooltip
            contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: 6 }}
            labelStyle={{ color: '#e2e8f0' }}
            formatter={(value: number, _name: string, props) => [
              `${value.toLocaleString()} late (${props.payload.on_time_pct}% on time)`,
              props.payload.label,
            ]}
          />
          <Bar dataKey="late" name="Late movements" fill="#ef4444" radius={[0, 3, 3, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Top delayed stations bar chart
// ---------------------------------------------------------------------------

export function StationDelaysChart({ data }: { data: StationDelay[] }) {
  const chartData = data.map((d) => ({
    ...d,
    label: d.station_name || d.loc_stanox,
  }))

  const maxLabelLen = Math.max(...chartData.map((d) => d.label.length), 1)
  const axisWidth = Math.min(maxLabelLen * 7, 160)

  return (
    <div className="bg-slate-800 rounded-lg p-4">
      <h2 className="text-slate-400 text-sm font-medium uppercase tracking-wider mb-4">
        Top Delayed Stations (last 24h)
      </h2>
      <ResponsiveContainer width="100%" height={320}>
        <BarChart data={chartData} layout="vertical" margin={{ top: 4, right: 24, left: 4, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#334155" horizontal={false} />
          <XAxis type="number" tick={{ fill: '#94a3b8', fontSize: 11 }} />
          <YAxis
            dataKey="label"
            type="category"
            tick={{ fill: '#94a3b8', fontSize: 10 }}
            width={axisWidth}
          />
          <Tooltip
            contentStyle={{ backgroundColor: '#1e293b', border: '1px solid #334155', borderRadius: 6 }}
            labelStyle={{ color: '#e2e8f0' }}
            formatter={(value: number, _name: string, props) => [
              `${value.toLocaleString()} late movements (${props.payload.late_pct}% of movements)`,
              props.payload.label,
            ]}
          />
          <Bar dataKey="late_count" name="Late movements" fill="#ef4444" radius={[0, 3, 3, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
