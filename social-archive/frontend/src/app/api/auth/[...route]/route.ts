import { NextRequest, NextResponse } from 'next/server'
import { cookies } from 'next/headers'

const BACKEND_URL = process.env.BACKEND_URL || 'http://social-archive-backend:8000'

export async function POST(
  request: NextRequest,
  { params }: { params: Promise<{ route: string[] }> },
) {
  const { route: routeSegments } = await params
  const route = routeSegments.join('/')

  if (route === 'login') {
    const body = await request.json()
    const res = await fetch(`${BACKEND_URL}/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })

    if (!res.ok) {
      const data = await res.json()
      return NextResponse.json(data, { status: res.status })
    }

    const data = await res.json()
    const response = NextResponse.json({ ok: true })
    response.cookies.set('token', data.access_token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'lax',
      maxAge: 60 * 60 * 24, // 24 hours
      path: '/',
    })
    return response
  }

  return NextResponse.json({ error: 'Not found' }, { status: 404 })
}

export async function GET(
  _request: NextRequest,
  { params }: { params: Promise<{ route: string[] }> },
) {
  const { route: routeSegments } = await params
  const route = routeSegments.join('/')

  if (route === 'logout') {
    const response = NextResponse.redirect(
      new URL('/login', process.env.NEXTAUTH_URL || 'http://localhost:3000'),
    )
    response.cookies.delete('token')
    return response
  }

  if (route === 'me') {
    const cookieStore = await cookies()
    const token = cookieStore.get('token')?.value
    if (!token) return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })

    const res = await fetch(`${BACKEND_URL}/auth/me`, {
      headers: { Authorization: `Bearer ${token}` },
    })
    const data = await res.json()
    return NextResponse.json(data, { status: res.status })
  }

  return NextResponse.json({ error: 'Not found' }, { status: 404 })
}
