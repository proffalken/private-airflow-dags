import { NextRequest, NextResponse } from 'next/server'
import { cookies } from 'next/headers'

const BACKEND_URL = process.env.BACKEND_URL || 'http://social-archive-backend:8000'

export async function GET(request: NextRequest) {
  const cookieStore = await cookies()
  const token = cookieStore.get('token')?.value
  if (!token) return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })

  const time = request.nextUrl.searchParams.get('time')
  if (!time) return NextResponse.json({ error: 'Missing time parameter' }, { status: 400 })

  const res = await fetch(`${BACKEND_URL}/api/suggest?time=${encodeURIComponent(time)}`, {
    headers: { Authorization: `Bearer ${token}` },
    cache: 'no-store',
  })

  const data = await res.json()
  return NextResponse.json(data, { status: res.status })
}
