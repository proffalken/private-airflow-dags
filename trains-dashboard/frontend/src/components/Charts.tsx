'use client'

import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { HourlyCount, TocPerformance } from '@/lib/api'

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
// OTP by operator bar chart
// ---------------------------------------------------------------------------

function otpColor(pct: number): string {
  if (pct >= 90) return '#22c55e'
  if (pct >= 75) return '#eab308'
  return '#ef4444'
}

export function PerformanceChart({ data }: { data: TocPerformance[] }) {
  // Use toc_name for labels; fall back to toc_id if name is missing
  const chartData = data.map((d) => ({
    ...d,
    label: d.toc_name || d.toc_id,
  }))

  // Truncate long operator names so they fit the y-axis
  const maxLabelLen = Math.max(...chartData.map((d) => d.label.length), 1)
  const axisWidth = Math.min(maxLabelLen * 7, 140)

  return (
    <div className="bg-slate-800 rounded-lg p-4">
      <h2 className="text-slate-400 text-sm font-medium uppercase tracking-wider mb-4">
        On-Time % by Operator (last 24h)
      </h2>
      <ResponsiveContainer width="100%" height={240}>
        <BarChart data={chartData} layout="vertical" margin={{ top: 4, right: 24, left: 4, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#334155" horizontal={false} />
          <XAxis type="number" domain={[0, 100]} tick={{ fill: '#94a3b8', fontSize: 11 }} unit="%" />
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
              `${value}% on time (${props.payload.total.toLocaleString()} movements)`,
              props.payload.label,
            ]}
          />
          <Bar dataKey="on_time_pct" name="On Time %" radius={[0, 3, 3, 0]}>
            {chartData.map((entry, i) => (
              <Cell key={i} fill={otpColor(entry.on_time_pct)} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
