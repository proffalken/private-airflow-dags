import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'TRUST Movement Dashboard',
  description: 'Real-time UK rail movement performance from the NROD TRUST feed',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="bg-slate-900 text-slate-100 min-h-screen antialiased">
        {children}
      </body>
    </html>
  )
}
