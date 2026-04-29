'use client'

import { useState } from 'react'
import { ItemCard } from '@/components/ItemCard'

type TimeOption = {
  label: string
  value: string
  description: string
}

const TIME_OPTIONS: TimeOption[] = [
  { label: '30 min', value: 'quick', description: 'A quick experiment or small script' },
  { label: 'Half day', value: 'afternoon', description: '2–4 hours of focused work' },
  { label: 'Full day', value: 'full_day', description: 'A solid day of building' },
  { label: 'Multi-day', value: 'multi_day', description: 'A proper project to sink into' },
]

interface Item {
  id: number
  title: string | null
  uri: string | null
  body: string | null
  source_context: string | null
  type: string | null
  summary: string | null
  tags: string[]
  flagged_for_deletion: boolean
  saved_at: string | null
  time_estimate: string | null
  estimate_reasoning: string | null
}

export default function BuildPage() {
  const [selected, setSelected] = useState<string | null>(null)
  const [items, setItems] = useState<Item[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [searched, setSearched] = useState(false)

  const fetchSuggestions = async (time: string) => {
    setSelected(time)
    setLoading(true)
    setError(null)
    setSearched(true)

    try {
      const res = await fetch(`/api/suggest?time=${time}`)
      if (!res.ok) throw new Error(`Request failed: ${res.status}`)
      const data: Item[] = await res.json()
      setItems(data)
    } catch (e) {
      setError('Failed to fetch suggestions. Try again.')
      setItems([])
    } finally {
      setLoading(false)
    }
  }

  return (
    <main className="container mx-auto px-4 py-8 max-w-4xl">
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-2xl font-bold text-gray-900">What can I build?</h1>
        <a href="/" className="text-sm text-gray-500 hover:text-gray-700">
          Back to archive
        </a>
      </div>

      <p className="text-gray-600 mb-6">
        How much time do you have?
      </p>

      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-8">
        {TIME_OPTIONS.map(opt => (
          <button
            key={opt.value}
            onClick={() => fetchSuggestions(opt.value)}
            className={`p-4 rounded-lg border-2 text-left transition-colors ${
              selected === opt.value
                ? 'border-blue-600 bg-blue-50'
                : 'border-gray-200 bg-white hover:border-blue-300 hover:bg-gray-50'
            }`}
          >
            <div className="font-semibold text-gray-900">{opt.label}</div>
            <div className="text-xs text-gray-500 mt-1">{opt.description}</div>
          </button>
        ))}
      </div>

      {loading && (
        <p className="text-center text-gray-500 py-12">Finding something to build...</p>
      )}

      {error && (
        <p className="text-center text-red-500 py-12">{error}</p>
      )}

      {!loading && searched && items.length === 0 && !error && (
        <p className="text-center text-gray-500 py-12">
          No items sized for this time slot yet — the project_sizer DAG will fill these in weekly.
        </p>
      )}

      {!loading && items.length > 0 && (
        <div className="space-y-3">
          {items.map(item => (
            <div key={item.id}>
              {item.estimate_reasoning && (
                <p className="text-xs text-blue-600 mb-1 px-1">{item.estimate_reasoning}</p>
              )}
              <ItemCard item={item} />
            </div>
          ))}
          <button
            onClick={() => selected && fetchSuggestions(selected)}
            className="w-full mt-4 py-2 text-sm text-gray-600 hover:text-gray-900 border border-gray-200 rounded-lg hover:border-gray-300 transition-colors"
          >
            Show me different suggestions
          </button>
        </div>
      )}
    </main>
  )
}
