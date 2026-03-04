'use client'

import { useRouter, useSearchParams } from 'next/navigation'
import { useState } from 'react'

export function SearchBar() {
  const router = useRouter()
  const searchParams = useSearchParams()

  const [q, setQ] = useState(searchParams.get('q') || '')
  const [sourceContext, setSourceContext] = useState(searchParams.get('source_context') || '')
  const [tags, setTags] = useState(searchParams.get('tags') || '')
  const [flagged, setFlagged] = useState(searchParams.get('flagged') || '')

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    const params = new URLSearchParams()
    if (q) params.set('q', q)
    if (sourceContext) params.set('source_context', sourceContext)
    if (tags) params.set('tags', tags)
    if (flagged) params.set('flagged', flagged)
    router.push(`/?${params.toString()}`)
  }

  const handleReset = () => {
    setQ('')
    setSourceContext('')
    setTags('')
    setFlagged('')
    router.push('/')
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-3 p-4 bg-white rounded-lg border border-gray-200">
      <div className="flex gap-2">
        <input
          type="text"
          value={q}
          onChange={e => setQ(e.target.value)}
          placeholder="Search titles and text…"
          className="flex-1 px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
        <button
          type="submit"
          className="px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700 transition-colors"
        >
          Search
        </button>
        <button
          type="button"
          onClick={handleReset}
          className="px-4 py-2 bg-gray-100 text-gray-600 text-sm rounded-md hover:bg-gray-200 transition-colors"
        >
          Reset
        </button>
      </div>
      <div className="flex gap-2 flex-wrap">
        <input
          type="text"
          value={sourceContext}
          onChange={e => setSourceContext(e.target.value)}
          placeholder="Source context"
          className="w-40 px-3 py-1.5 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
        <input
          type="text"
          value={tags}
          onChange={e => setTags(e.target.value)}
          placeholder="Tags (comma separated)"
          className="flex-1 min-w-40 px-3 py-1.5 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
        <select
          value={flagged}
          onChange={e => setFlagged(e.target.value)}
          className="px-3 py-1.5 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          <option value="">All items</option>
          <option value="false">Not flagged</option>
          <option value="true">Flagged for deletion</option>
        </select>
      </div>
    </form>
  )
}
