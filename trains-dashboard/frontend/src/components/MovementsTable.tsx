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

function stanox(code: string | null, name: string | null): string {
  if (!code && !name) return '—'
  if (name && name !== code) return name
  return code ?? '—'
}

const COLUMNS: { key: string; label: string; description: string }[] = [
  { key: 'train_id',           label: 'Train',      description: 'TRUST train ID assigned for this service activation' },
  { key: 'toc_name',           label: 'Operator',   description: 'Train Operating Company responsible for this service' },
  { key: 'event_type',         label: 'Event',      description: 'DEPARTURE or ARRIVAL at the reported location' },
  { key: 'origin_stanme',      label: 'From',       description: 'First location reported for this train today (journey origin proxy)' },
  { key: 'loc_stanme',         label: 'At',         description: 'Station or location where this movement event occurred (STANOX)' },
  { key: 'next_report_stanme', label: 'Next',       description: 'Next scheduled reporting point for this train (STANOX)' },
  { key: 'variation_status',   label: 'Status',     description: 'ON TIME / LATE / EARLY relative to the working timetable' },
  { key: 'timetable_variation',label: 'Variation',  description: 'Minutes early (negative) or late (positive) vs the planned time' },
  { key: 'actual_ts',          label: 'Actual',     description: 'Actual time of the movement event (UTC)' },
  { key: 'msg_queue_ts',       label: 'Reported',   description: 'Time the message was queued by Network Rail (UTC)' },
]

export function MovementsTable({ data }: { data: Movement[] }) {
  return (
    <div className="bg-slate-800 rounded-lg p-4 space-y-4">
      <h2 className="text-slate-400 text-sm font-medium uppercase tracking-wider">
        Recent Movements
      </h2>

      {/* Column legend */}
      <details className="group">
        <summary className="cursor-pointer text-xs text-slate-500 hover:text-slate-300 transition-colors select-none">
          Column key / glossary
        </summary>
        <div className="mt-2 grid grid-cols-1 sm:grid-cols-2 gap-x-6 gap-y-1">
          {COLUMNS.map(({ label, description }) => (
            <div key={label} className="flex gap-2 text-xs py-0.5">
              <span className="text-slate-300 font-medium w-20 shrink-0">{label}</span>
              <span className="text-slate-500">{description}</span>
            </div>
          ))}
        </div>
        <p className="mt-2 text-xs text-slate-600">
          Station names sourced from the Network Rail CORPUS reference file.
          Codes shown where the name lookup is not yet available — run the
          <code className="mx-1 text-slate-400">corpus_loader</code>
          DAG to populate station names.
        </p>
      </details>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="w-full min-w-[700px] text-sm text-left table-fixed">
          <colgroup>
            <col className="w-24" />   {/* Train */}
            <col className="w-32" />   {/* Operator */}
            <col className="w-20" />   {/* Event */}
            <col className="w-28" />   {/* From */}
            <col className="w-28" />   {/* At */}
            <col className="w-28" />   {/* Next */}
            <col className="w-20" />   {/* Status */}
            <col className="w-20" />   {/* Variation */}
            <col className="w-16" />   {/* Actual */}
            <col className="w-16" />   {/* Reported */}
          </colgroup>
          <thead>
            <tr className="text-slate-400 border-b border-slate-700">
              {COLUMNS.map(({ key, label }) => (
                <th key={key} className="pb-2 pr-2 font-medium whitespace-nowrap">{label}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.map((m, i) => (
              <tr
                key={i}
                className="border-b border-slate-700/50 hover:bg-slate-700/30 transition-colors"
              >
                <td className="py-1.5 pr-2 font-mono text-slate-200 text-xs truncate">
                  {m.train_id ?? '—'}
                </td>
                <td className="py-1.5 pr-2 text-slate-300 truncate" title={m.toc_name ?? m.toc_id ?? ''}>
                  {m.toc_name ?? m.toc_id ?? '—'}
                </td>
                <td className="py-1.5 pr-2 text-slate-300 whitespace-nowrap">
                  {m.event_type ?? '—'}
                </td>
                <td className="py-1.5 pr-2 text-slate-400 text-xs truncate" title={stanox(m.origin_stanox, m.origin_stanme)}>
                  {stanox(m.origin_stanox, m.origin_stanme)}
                </td>
                <td className="py-1.5 pr-2 text-slate-300 truncate" title={stanox(m.loc_stanox, m.loc_stanme)}>
                  {stanox(m.loc_stanox, m.loc_stanme)}
                </td>
                <td className="py-1.5 pr-2 text-slate-400 text-xs truncate" title={stanox(m.next_report_stanox, m.next_report_stanme)}>
                  {stanox(m.next_report_stanox, m.next_report_stanme)}
                </td>
                <td className={`py-1.5 pr-2 font-medium whitespace-nowrap ${statusColour[m.variation_status ?? ''] ?? 'text-slate-400'}`}>
                  {m.variation_status ?? '—'}
                </td>
                <td className="py-1.5 pr-2 text-slate-300 whitespace-nowrap tabular-nums">
                  {m.timetable_variation != null
                    ? `${m.timetable_variation > 0 ? '+' : ''}${m.timetable_variation} min`
                    : '—'}
                </td>
                <td className="py-1.5 pr-2 font-mono text-slate-400 whitespace-nowrap text-xs">
                  {fmt(m.actual_ts)}
                </td>
                <td className="py-1.5 font-mono text-slate-400 whitespace-nowrap text-xs">
                  {fmt(m.msg_queue_ts)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {data.length === 0 && (
          <p className="text-slate-500 text-center py-8">No movements loaded yet.</p>
        )}
      </div>
    </div>
  )
}
