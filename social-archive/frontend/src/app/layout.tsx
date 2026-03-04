import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'Social Archive',
  description: 'Browse and manage your saved social media items',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen bg-gray-50">{children}</body>
    </html>
  )
}
