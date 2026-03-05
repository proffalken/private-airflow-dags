/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'standalone',
  // BACKEND_URL is consumed server-side only (API routes + server components).
  // Set via k8s ConfigMap: http://social-archive-backend:8000
}

export default nextConfig
