const BACKEND = process.env.BACKEND_URL ?? 'http://localhost:8000'

async function apiFetch<T>(path: string): Promise<T> {
  const res = await fetch(`${BACKEND}${path}`, { next: { revalidate: 60 } })
  if (!res.ok) throw new Error(`API error: ${res.status} ${path}`)
  return res.json() as Promise<T>
}

export interface Summary {
  total_movements: number
  cancellations: number
  on_time_pct: number
  avg_delay_mins: number
}

export interface TocPerformance {
  toc_id: string
  total: number
  on_time: number
  late: number
  early: number
  on_time_pct: number
}

export interface Movement {
  train_id: string | null
  train_uid: string | null
  toc_id: string | null
  event_type: string | null
  loc_stanox: string | null
  variation_status: string | null
  timetable_variation: number | null
  actual_ts: string | null
  planned_ts: string | null
  msg_queue_ts: string | null
}

export interface HourlyCount {
  hour: string
  count: number
}

export const getSummary        = () => apiFetch<Summary>('/api/summary')
export const getPerformance    = () => apiFetch<TocPerformance[]>('/api/performance')
export const getRecentMovements = () => apiFetch<Movement[]>('/api/movements/recent')
export const getHourlyCounts   = () => apiFetch<HourlyCount[]>('/api/movements/hourly')
