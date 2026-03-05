'use client'

import { useState, useRef, useEffect } from 'react'
import { flagItem, editItem } from '@/lib/api'

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
  const [flagLoading, setFlagLoading] = useState(false)

  // Title editing
  const [title, setTitle] = useState(item.title ?? '')
  const [editingTitle, setEditingTitle] = useState(false)
  const [titleDraft, setTitleDraft] = useState(title)
  const titleInputRef = useRef<HTMLInputElement>(null)

  // Tags editing
  const [tags, setTags] = useState(item.tags)
  const [editingTags, setEditingTags] = useState(false)
  const [tagsDraft, setTagsDraft] = useState(item.tags.join(', '))
  const tagsInputRef = useRef<HTMLInputElement>(null)

  useEffect(() => { if (editingTitle) titleInputRef.current?.focus() }, [editingTitle])
  useEffect(() => { if (editingTags) tagsInputRef.current?.focus() }, [editingTags])

  const toggleFlag = async () => {
    const newFlagged = !flagged
    setFlagged(newFlagged)
    setFlagLoading(true)
    try {
      await flagItem(item.id, newFlagged)
    } catch {
      setFlagged(!newFlagged)
    } finally {
      setFlagLoading(false)
    }
  }

  const saveTitle = async () => {
    const trimmed = titleDraft.trim()
    setEditingTitle(false)
    if (trimmed === title) return
    const prev = title
    setTitle(trimmed)
    try {
      await editItem(item.id, { title: trimmed })
    } catch {
      setTitle(prev)
    }
  }

  const cancelTitle = () => {
    setTitleDraft(title)
    setEditingTitle(false)
  }

  const saveTags = async () => {
    const newTags = tagsDraft
      .split(',')
      .map(t => t.trim().toLowerCase())
      .filter(Boolean)
    setEditingTags(false)
    if (JSON.stringify(newTags) === JSON.stringify(tags)) return
    const prev = tags
    setTags(newTags)
    try {
      await editItem(item.id, { tags: newTags })
    } catch {
      setTags(prev)
    }
  }

  const cancelTags = () => {
    setTagsDraft(tags.join(', '))
    setEditingTags(false)
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
          </div>

          {/* Title — inline editable */}
          <div className="group flex items-start gap-1">
            {editingTitle ? (
              <div className="flex items-center gap-1 flex-1">
                <input
                  ref={titleInputRef}
                  value={titleDraft}
                  onChange={e => setTitleDraft(e.target.value)}
                  onKeyDown={e => { if (e.key === 'Enter') saveTitle(); if (e.key === 'Escape') cancelTitle() }}
                  className="flex-1 text-sm font-medium px-2 py-0.5 border border-blue-400 rounded focus:outline-none focus:ring-1 focus:ring-blue-500"
                />
                <button onClick={saveTitle} className="text-xs text-green-700 hover:text-green-900 font-medium">Save</button>
                <button onClick={cancelTitle} className="text-xs text-gray-500 hover:text-gray-700">Cancel</button>
              </div>
            ) : (
              <>
                <h3 className="font-medium text-gray-900 flex-1">
                  {item.uri ? (
                    <a href={item.uri} target="_blank" rel="noopener noreferrer" className="hover:underline">
                      {title || '(no title)'}
                    </a>
                  ) : (
                    title || '(no title)'
                  )}
                </h3>
                <button
                  onClick={() => { setTitleDraft(title); setEditingTitle(true) }}
                  className="opacity-0 group-hover:opacity-100 text-gray-400 hover:text-gray-600 text-xs shrink-0 mt-0.5 transition-opacity"
                  title="Edit title"
                >
                  ✎
                </button>
              </>
            )}
          </div>

          {item.summary && !editingTitle && (
            <p className="text-sm text-gray-600 mt-1 line-clamp-2">{item.summary}</p>
          )}

          <div className="flex items-center gap-3 mt-2 flex-wrap">
            {item.saved_at && (
              <span className="text-xs text-gray-400">
                {new Date(item.saved_at).toLocaleDateString()}
              </span>
            )}

            {/* Tags — inline editable */}
            <div className="group flex items-center gap-1 flex-wrap">
              {editingTags ? (
                <div className="flex items-center gap-1 flex-1 min-w-0">
                  <input
                    ref={tagsInputRef}
                    value={tagsDraft}
                    onChange={e => setTagsDraft(e.target.value)}
                    onKeyDown={e => { if (e.key === 'Enter') saveTags(); if (e.key === 'Escape') cancelTags() }}
                    placeholder="tag1, tag2, tag3"
                    className="flex-1 text-xs px-2 py-0.5 border border-blue-400 rounded focus:outline-none focus:ring-1 focus:ring-blue-500"
                  />
                  <button onClick={saveTags} className="text-xs text-green-700 hover:text-green-900 font-medium shrink-0">Save</button>
                  <button onClick={cancelTags} className="text-xs text-gray-500 hover:text-gray-700 shrink-0">Cancel</button>
                </div>
              ) : (
                <>
                  {tags.length > 0 && (
                    <div className="flex gap-1 flex-wrap">
                      {tags.map(tag => (
                        <a
                          key={tag}
                          href={`/?tags=${encodeURIComponent(tag)}`}
                          className="text-xs bg-blue-100 text-blue-700 px-1.5 py-0.5 rounded hover:bg-blue-200 transition-colors"
                        >
                          {tag}
                        </a>
                      ))}
                    </div>
                  )}
                  <button
                    onClick={() => { setTagsDraft(tags.join(', ')); setEditingTags(true) }}
                    className="opacity-0 group-hover:opacity-100 text-gray-400 hover:text-gray-600 text-xs shrink-0 transition-opacity"
                    title="Edit tags"
                  >
                    ✎
                  </button>
                </>
              )}
            </div>
          </div>
        </div>

        <button
          onClick={toggleFlag}
          disabled={flagLoading}
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
