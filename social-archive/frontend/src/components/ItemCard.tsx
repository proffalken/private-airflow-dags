'use client'

import { useState, useRef, useEffect } from 'react'
import { flagItem, editItem } from '@/lib/api'

// Matches CONTENT_TYPES in backend/app/routes/items.py
const CONTENT_TYPE_LABELS: Record<string, string> = {
  recipe: '🍳 Recipe',
  project: '🔧 Project',
  article: '📄 Article',
  reference: '📚 Reference',
  tool: '🛠 Tool',
  other: 'Other',
}

const CONTENT_TYPE_COLORS: Record<string, string> = {
  recipe: 'bg-orange-100 text-orange-700',
  project: 'bg-purple-100 text-purple-700',
  article: 'bg-green-100 text-green-700',
  reference: 'bg-yellow-100 text-yellow-700',
  tool: 'bg-cyan-100 text-cyan-700',
  other: 'bg-gray-100 text-gray-600',
}

const ALL_CONTENT_TYPES = Object.keys(CONTENT_TYPE_LABELS)

interface StructuredData {
  // Recipe fields
  ingredients?: string[]
  steps?: string[]
  servings?: number | string
  cook_time?: string
  prep_time?: string
  dietary?: string[]
  cuisine?: string
  // Project fields
  materials?: string[]
  tools?: string[]
  skills?: string[]
  difficulty?: string
  estimated_cost?: string
  [key: string]: unknown
}

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
  content_type: string | null
  structured_data: StructuredData | null
}

function RecipePanel({ data }: { data: StructuredData }) {
  const [open, setOpen] = useState(false)
  const hasDetail = (data.ingredients?.length ?? 0) > 0 || (data.steps?.length ?? 0) > 0

  if (!hasDetail) return null

  return (
    <div className="mt-2 border border-orange-200 rounded-md overflow-hidden text-sm">
      <button
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-center justify-between px-3 py-1.5 bg-orange-50 text-orange-800 text-xs font-medium hover:bg-orange-100 transition-colors"
      >
        <span>Recipe details</span>
        <span>{open ? '▲' : '▼'}</span>
      </button>
      {open && (
        <div className="px-3 py-2 space-y-2 bg-white">
          {(data.cuisine || data.servings || data.prep_time || data.cook_time) && (
            <div className="flex gap-3 flex-wrap text-xs text-gray-600">
              {data.cuisine && <span>🌍 {data.cuisine}</span>}
              {data.servings && <span>🍽 {data.servings} servings</span>}
              {data.prep_time && <span>⏱ prep {data.prep_time}</span>}
              {data.cook_time && <span>🔥 cook {data.cook_time}</span>}
            </div>
          )}
          {data.dietary && data.dietary.length > 0 && (
            <div className="flex gap-1 flex-wrap">
              {data.dietary.map(d => (
                <span key={d} className="text-xs bg-green-100 text-green-700 px-1.5 py-0.5 rounded">{d}</span>
              ))}
            </div>
          )}
          {data.ingredients && data.ingredients.length > 0 && (
            <div>
              <p className="text-xs font-semibold text-gray-700 mb-1">Ingredients</p>
              <ul className="list-disc list-inside space-y-0.5">
                {data.ingredients.map((ing, i) => (
                  <li key={i} className="text-xs text-gray-600">{ing}</li>
                ))}
              </ul>
            </div>
          )}
          {data.steps && data.steps.length > 0 && (
            <div>
              <p className="text-xs font-semibold text-gray-700 mb-1">Steps</p>
              <ol className="list-decimal list-inside space-y-0.5">
                {data.steps.map((step, i) => (
                  <li key={i} className="text-xs text-gray-600">{step}</li>
                ))}
              </ol>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function ProjectPanel({ data }: { data: StructuredData }) {
  const [open, setOpen] = useState(false)
  const hasDetail = (data.materials?.length ?? 0) > 0 || (data.tools?.length ?? 0) > 0

  if (!hasDetail) return null

  return (
    <div className="mt-2 border border-purple-200 rounded-md overflow-hidden text-sm">
      <button
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-center justify-between px-3 py-1.5 bg-purple-50 text-purple-800 text-xs font-medium hover:bg-purple-100 transition-colors"
      >
        <span>Project details</span>
        <span>{open ? '▲' : '▼'}</span>
      </button>
      {open && (
        <div className="px-3 py-2 space-y-2 bg-white">
          {(data.difficulty || data.estimated_cost) && (
            <div className="flex gap-3 flex-wrap text-xs text-gray-600">
              {data.difficulty && <span>📊 {data.difficulty}</span>}
              {data.estimated_cost && <span>💰 {data.estimated_cost}</span>}
            </div>
          )}
          {data.materials && data.materials.length > 0 && (
            <div>
              <p className="text-xs font-semibold text-gray-700 mb-1">Materials</p>
              <ul className="list-disc list-inside space-y-0.5">
                {data.materials.map((m, i) => (
                  <li key={i} className="text-xs text-gray-600">{m}</li>
                ))}
              </ul>
            </div>
          )}
          {data.tools && data.tools.length > 0 && (
            <div>
              <p className="text-xs font-semibold text-gray-700 mb-1">Tools needed</p>
              <ul className="list-disc list-inside space-y-0.5">
                {data.tools.map((t, i) => (
                  <li key={i} className="text-xs text-gray-600">{t}</li>
                ))}
              </ul>
            </div>
          )}
          {data.skills && data.skills.length > 0 && (
            <div className="flex gap-1 flex-wrap">
              {data.skills.map(s => (
                <span key={s} className="text-xs bg-purple-100 text-purple-700 px-1.5 py-0.5 rounded">{s}</span>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
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

  // Content type editing
  const [contentType, setContentType] = useState(item.content_type ?? '')
  const [editingContentType, setEditingContentType] = useState(false)

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

  const saveContentType = async (newType: string) => {
    setEditingContentType(false)
    if (newType === contentType) return
    const prev = contentType
    setContentType(newType)
    try {
      await editItem(item.id, { content_type: newType || null } as Parameters<typeof editItem>[1])
    } catch {
      setContentType(prev)
    }
  }

  const ctColor = CONTENT_TYPE_COLORS[contentType] ?? 'bg-gray-100 text-gray-600'
  const ctLabel = CONTENT_TYPE_LABELS[contentType] ?? contentType

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
            {/* Content type badge — inline editable */}
            {editingContentType ? (
              <select
                value={contentType}
                autoFocus
                onChange={e => saveContentType(e.target.value)}
                onBlur={() => setEditingContentType(false)}
                className="text-xs border border-blue-400 rounded px-1 py-0.5 focus:outline-none focus:ring-1 focus:ring-blue-500"
              >
                <option value="">— unclassified —</option>
                {ALL_CONTENT_TYPES.map(ct => (
                  <option key={ct} value={ct}>{CONTENT_TYPE_LABELS[ct]}</option>
                ))}
              </select>
            ) : contentType ? (
              <button
                onClick={() => setEditingContentType(true)}
                title="Click to change content type"
                className={`text-xs px-2 py-0.5 rounded font-medium ${ctColor} hover:opacity-80 transition-opacity`}
              >
                {ctLabel}
              </button>
            ) : (
              <button
                onClick={() => setEditingContentType(true)}
                title="Classify this item"
                className="text-xs px-2 py-0.5 rounded text-gray-400 border border-dashed border-gray-300 hover:border-gray-400 hover:text-gray-500 transition-colors"
              >
                + classify
              </button>
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

          {/* Structured data panels — only shown when data is present */}
          {contentType === 'recipe' && item.structured_data && (
            <RecipePanel data={item.structured_data} />
          )}
          {contentType === 'project' && item.structured_data && (
            <ProjectPanel data={item.structured_data} />
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
