import React, { useState } from 'react'
import Sidebar from './components/common/Sidebar'
import Dashboard from './pages/Dashboard'
import PipelinePage from './pages/PipelinePage'
import MonitoringPage from './pages/MonitoringPage'
import ConfigPage from './pages/ConfigPage'
import CatalogPage from './pages/CatalogPage'

export type Page = 'dashboard' | 'pipeline' | 'monitoring' | 'config' | 'catalog'

export default function App() {
  const [page, setPage] = useState<Page>('dashboard')
  const renderPage = () => {
    switch(page) {
      case 'dashboard':   return <Dashboard />
      case 'pipeline':    return <PipelinePage />
      case 'monitoring':  return <MonitoringPage />
      case 'config':      return <ConfigPage />
      case 'catalog':     return <CatalogPage />
      default:            return <Dashboard />
    }
  }
  return (
    <div style={{ display:'flex', height:'100vh', overflow:'hidden', background:'var(--bg-primary)' }}>
      <Sidebar activePage={page} onNavigate={setPage} />
      <main style={{ flex:1, overflow:'auto', padding:'24px', animation:'fade-in 0.3s ease' }}>
        {renderPage()}
      </main>
    </div>
  )
}
