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

  const body = await request.json()
  const res = await fetch(`${BACKEND_URL}/api/bookmarks/sync`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: authHeader,
    },
    body: JSON.stringify(body),
  })
  const data = await res.json()
  return NextResponse.json(data, { status: res.status })
}
