import { NextRequest, NextResponse } from 'next/server'

const BACKEND_URL = process.env.BACKEND_URL || 'http://social-archive-backend:8000'

// Extension-specific bookmarks sync proxy.
// Accepts Authorization: Bearer <token> from the extension and forwards it
// to the backend — bypasses the httpOnly cookie used by the browser frontend.
export async function POST(request: NextRequest) {
  const authHeader = request.headers.get('Authorization')
  if (!authHeader?.startsWith('Bearer ')) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
  }

  let body: unknown
  try {
    body = await request.json()
  } catch {
    return NextResponse.json({ error: 'Invalid JSON body' }, { status: 400 })
  }

  let res: Response
  try {
    res = await fetch(`${BACKEND_URL}/api/bookmarks/sync`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: authHeader,
      },
      body: JSON.stringify(body),
    })
  } catch (err) {
    return NextResponse.json(
      { error: `Backend unreachable: ${err instanceof Error ? err.message : err}` },
      { status: 502 },
    )
  }

  const text = await res.text()
  let data: unknown
  try {
    data = JSON.parse(text)
  } catch {
    data = { error: text }
  }
  return NextResponse.json(data, { status: res.status })
}
