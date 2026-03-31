/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'standalone',
  // BACKEND_URL is consumed server-side only (server components).
  // Set via k8s env: http://trains-dashboard-backend:8000
}

export default nextConfig
