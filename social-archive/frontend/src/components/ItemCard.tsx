'use client'

import { useState } from 'react'
import { flagItem } from '@/lib/api'

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
}

export function ItemCard({ item }: { item: Item }) {
  const [flagged, setFlagged] = useState(item.flagged_for_deletion)
  const [loading, setLoading] = useState(false)

  const toggleFlag = async () => {
    const newFlagged = !flagged
    setFlagged(newFlagged) // optimistic update
    setLoading(true)
    try {
      await flagItem(item.id, newFlagged)
    } catch {
      setFlagged(!newFlagged) // revert on error
    } finally {
      setLoading(false)
    }
  }

  return (
    <div
      className={`p-4 border rounded-lg transition-colors ${
        flagged ? 'border-red-300 bg-red-50' : 'border-gray-200 bg-white'
      }`}
    >
      <div className="flex justify-between items-start gap-4">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1 flex-wrap">
            {item.source_context && (
              <span className="text-xs font-medium text-blue-600">{item.source_context}</span>
            )}
            {item.type && (
              <span className="text-xs bg-gray-100 text-gray-600 px-2 py-0.5 rounded">
                {item.type}
              </span>
            )}
            {item.score !== null && (
              <span className="text-xs text-gray-500">↑ {item.score}</span>
            )}
          </div>

          {item.title && (
            <h3 className="font-medium text-gray-900">
              {item.uri ? (
                <a
                  href={item.uri}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="hover:underline"
                >
                  {item.title}
                </a>
              ) : (
                item.title
              )}
            </h3>
          )}

          {item.summary && (
            <p className="text-sm text-gray-600 mt-1 line-clamp-2">{item.summary}</p>
          )}

          <div className="flex items-center gap-3 mt-2 flex-wrap">
            {item.author && (
              <span className="text-xs text-gray-500">u/{item.author}</span>
            )}
            {item.saved_at && (
              <span className="text-xs text-gray-400">
                {new Date(item.saved_at).toLocaleDateString()}
              </span>
            )}
            {item.tags && item.tags.length > 0 && (
              <div className="flex gap-1 flex-wrap">
                {item.tags.map(tag => (
                  <span
                    key={tag}
                    className="text-xs bg-blue-100 text-blue-700 px-1.5 py-0.5 rounded"
                  >
                    {tag}
                  </span>
                ))}
              </div>
            )}
          </div>
        </div>

        <button
          onClick={toggleFlag}
          disabled={loading}
          title={flagged ? 'Unflag item' : 'Flag for deletion'}
          className={`shrink-0 px-3 py-1.5 text-sm rounded-md font-medium transition-colors disabled:opacity-50 ${
            flagged
              ? 'bg-red-100 text-red-700 hover:bg-red-200'
              : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
          }`}
        >
          {flagged ? '🚩 Flagged' : 'Flag'}
        </button>
      </div>
    </div>
  )
}
