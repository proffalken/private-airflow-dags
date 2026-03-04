import { cookies } from 'next/headers'
import { redirect } from 'next/navigation'
import { Suspense } from 'react'
import { ItemCard } from '@/components/ItemCard'
import { SearchBar } from '@/components/SearchBar'

const BACKEND_URL = process.env.BACKEND_URL || 'http://social-archive-backend:8000'

interface SearchParams {
  q?: string
  tags?: string
  source_context?: string
  type?: string
  flagged?: string
  limit?: string
  offset?: string
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
}

interface ItemsResponse {
  items: Item[]
  total: number
  limit: number
  offset: number
}

async function fetchItems(
  token: string,
  params: SearchParams,
): Promise<ItemsResponse | null> {
  const url = new URL(`${BACKEND_URL}/api/items`)

  if (params.q) url.searchParams.set('q', params.q)
  if (params.tags) {
    params.tags
      .split(',')
      .map(t => t.trim())
      .filter(Boolean)
      .forEach(t => url.searchParams.append('tags', t))
  }
  if (params.source_context) url.searchParams.set('source_context', params.source_context)
  if (params.type) url.searchParams.set('type', params.type)
  if (params.flagged) url.searchParams.set('flagged', params.flagged)
  url.searchParams.set('limit', params.limit || '50')
  url.searchParams.set('offset', params.offset || '0')

  try {
    const res = await fetch(url.toString(), {
      headers: { Authorization: `Bearer ${token}` },
      cache: 'no-store',
    })
    if (!res.ok) return null
    return res.json()
  } catch {
    return null
  }
}

export default async function Home({
  searchParams,
}: {
  searchParams: SearchParams
}) {
  const cookieStore = cookies()
  const token = cookieStore.get('token')?.value
  if (!token) redirect('/login')

  const data = await fetchItems(token, searchParams)
  if (!data) redirect('/login')

  const limit = parseInt(searchParams.limit || '50')
  const offset = parseInt(searchParams.offset || '0')

  const prevParams = new URLSearchParams(
    Object.entries(searchParams)
      .filter(([, v]) => v !== undefined)
      .map(([k, v]) => [k, v as string]),
  )
  prevParams.set('offset', String(Math.max(0, offset - limit)))

  const nextParams = new URLSearchParams(
    Object.entries(searchParams)
      .filter(([, v]) => v !== undefined)
      .map(([k, v]) => [k, v as string]),
  )
  nextParams.set('offset', String(offset + limit))

  return (
    <main className="container mx-auto px-4 py-8 max-w-4xl">
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Social Archive</h1>
        <div className="flex items-center gap-4">
          <span className="text-sm text-gray-500">{data.total.toLocaleString()} items</span>
          <a
            href="/api/auth/logout"
            className="text-sm text-gray-500 hover:text-gray-700"
          >
            Sign out
          </a>
        </div>
      </div>

      <Suspense>
        <SearchBar />
      </Suspense>

      <div className="mt-6 space-y-3">
        {data.items.map(item => (
          <ItemCard key={item.id} item={item} />
        ))}
        {data.items.length === 0 && (
          <p className="text-center text-gray-500 py-12">No items found.</p>
        )}
      </div>

      {data.total > limit && (
        <div className="flex justify-center gap-2 mt-8">
          {offset > 0 && (
            <a
              href={`/?${prevParams.toString()}`}
              className="px-4 py-2 bg-gray-200 text-gray-700 rounded-md hover:bg-gray-300 transition-colors"
            >
              Previous
            </a>
          )}
          {offset + limit < data.total && (
            <a
              href={`/?${nextParams.toString()}`}
              className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors"
            >
              Next
            </a>
          )}
        </div>
      )}
    </main>
  )
}
