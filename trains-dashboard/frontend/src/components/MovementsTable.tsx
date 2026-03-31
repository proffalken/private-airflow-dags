import type { Movement } from '@/lib/api'

const statusColour: Record<string, string> = {
  'ON TIME': 'text-green-400',
  'LATE':    'text-red-400',
  'EARLY':   'text-blue-400',
}

function fmt(iso: string | null): string {
  if (!iso) return '—'
  return new Date(iso).toLocaleTimeString('en-GB', {
    hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone: 'UTC',
  })
}

export function MovementsTable({ data }: { data: Movement[] }) {
  return (
    <div className="bg-slate-800 rounded-lg p-4 overflow-x-auto">
      <h2 className="text-slate-400 text-sm font-medium uppercase tracking-wider mb-4">
        Recent Movements
      </h2>
      <table className="w-full text-sm text-left">
        <thead>
          <tr className="text-slate-400 border-b border-slate-700">
            <th className="pb-2 pr-4 font-medium">Train</th>
            <th className="pb-2 pr-4 font-medium">TOC</th>
            <th className="pb-2 pr-4 font-medium">Event</th>
            <th className="pb-2 pr-4 font-medium">STANOX</th>
            <th className="pb-2 pr-4 font-medium">Status</th>
            <th className="pb-2 pr-4 font-medium">Variation</th>
            <th className="pb-2 pr-4 font-medium">Actual (UTC)</th>
            <th className="pb-2 font-medium">Queued (UTC)</th>
          </tr>
        </thead>
        <tbody>
          {data.map((m, i) => (
            <tr
              key={i}
              className="border-b border-slate-700/50 hover:bg-slate-700/30 transition-colors"
            >
              <td className="py-1.5 pr-4 font-mono text-slate-200">{m.train_id ?? '—'}</td>
              <td className="py-1.5 pr-4 text-slate-300">{m.toc_id ?? '—'}</td>
              <td className="py-1.5 pr-4 text-slate-300">{m.event_type ?? '—'}</td>
              <td className="py-1.5 pr-4 font-mono text-slate-400">{m.loc_stanox ?? '—'}</td>
              <td className={`py-1.5 pr-4 font-medium ${statusColour[m.variation_status ?? ''] ?? 'text-slate-400'}`}>
                {m.variation_status ?? '—'}
              </td>
              <td className="py-1.5 pr-4 text-slate-300">
                {m.timetable_variation != null
                  ? `${m.timetable_variation > 0 ? '+' : ''}${m.timetable_variation} min`
                  : '—'}
              </td>
              <td className="py-1.5 pr-4 font-mono text-slate-400">{fmt(m.actual_ts)}</td>
              <td className="py-1.5 font-mono text-slate-400">{fmt(m.msg_queue_ts)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {data.length === 0 && (
        <p className="text-slate-500 text-center py-8">No movements loaded yet.</p>
      )}
    </div>
  )
}
