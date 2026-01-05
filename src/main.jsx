import React, { useState, useEffect, lazy, Suspense } from 'react'
import { createRoot } from 'react-dom/client'
import App from './App'
import './styles.css'

// Lazy load BranhamApp - only downloaded when user navigates to #branham
const BranhamApp = lazy(() => import('./BranhamApp'))

// Loading fallback for BranhamApp
function BranhamLoadingFallback() {
  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-amber-50 to-orange-100">
      <div className="text-center">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-amber-600 mx-auto mb-4"></div>
        <p className="text-amber-800 font-medium">Loading Branham Archive...</p>
      </div>
    </div>
  )
}

// Simple router component that switches between main app and Branham archive
function Router() {
  const [currentApp, setCurrentApp] = useState(() => {
    // Check URL hash for initial route
    const hash = window.location.hash.replace('#', '')
    return hash === 'branham' ? 'branham' : 'main'
  })

  // Listen for hash changes
  useEffect(() => {
    const handleHashChange = () => {
      const hash = window.location.hash.replace('#', '')
      setCurrentApp(hash === 'branham' ? 'branham' : 'main')
    }
    window.addEventListener('hashchange', handleHashChange)
    return () => window.removeEventListener('hashchange', handleHashChange)
  }, [])

  const switchToMain = () => {
    window.location.hash = ''
    setCurrentApp('main')
  }

  const switchToBranham = () => {
    window.location.hash = 'branham'
    setCurrentApp('branham')
  }

  if (currentApp === 'branham') {
    return (
      <Suspense fallback={<BranhamLoadingFallback />}>
        <BranhamApp onSwitchToMain={switchToMain} />
      </Suspense>
    )
  }

  return <App onSwitchToBranham={switchToBranham} />
}

createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <Router />
  </React.StrictMode>
)
