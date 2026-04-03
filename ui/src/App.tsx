import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom'
import ODAGList from '@/pages/ODAGList'
import ODAGDetail from '@/pages/ODAGDetail'
import CDAGList from '@/pages/CDAGList'
import CDAGDetail from '@/pages/CDAGDetail'
import BatchExecution from '@/pages/BatchExecution'
import TemplateList from '@/pages/TemplateList'
import TemplateDetail from '@/pages/TemplateDetail'
import { useSSE } from '@/hooks/useSSE'
import { useTheme } from '@/hooks/useTheme'

function AppRoutes() {
  // Connect to the SSE stream once; invalidates queries on every server push.
  useSSE()

  return (
    <Routes>
      <Route path="/" element={<ODAGList />} />
      <Route path="/odags/:namespace/:name" element={<ODAGDetail />} />
      <Route path="/batch" element={<BatchExecution />} />
      <Route path="/templates" element={<TemplateList />} />
      <Route path="/templates/:namespace/:name" element={<TemplateDetail />} />
      <Route path="/cdags" element={<CDAGList />} />
      <Route path="/cdags/:namespace/:name" element={<CDAGDetail />} />
    </Routes>
  )
}

export default function App() {
  const { theme, toggle } = useTheme()

  return (
    <BrowserRouter>
      <div className="min-h-screen bg-surface text-on font-mono">
        <header className="border-b border-line px-6 py-3 flex items-center gap-8">
          <span className="font-bold text-on tracking-tight">DSF</span>
          <nav className="flex gap-6 text-sm">
            <NavLink
              to="/"
              end
              className={({ isActive }) =>
                isActive ? 'text-on' : 'text-on-muted hover:text-on-secondary'
              }
            >
              ODAGs
            </NavLink>
            <NavLink
              to="/batch"
              className={({ isActive }) =>
                isActive ? 'text-on' : 'text-on-muted hover:text-on-secondary'
              }
            >
              Batch
            </NavLink>
            <NavLink
              to="/templates"
              className={({ isActive }) =>
                isActive ? 'text-on' : 'text-on-muted hover:text-on-secondary'
              }
            >
              Templates
            </NavLink>
            <NavLink
              to="/cdags"
              className={({ isActive }) =>
                isActive ? 'text-on' : 'text-on-muted hover:text-on-secondary'
              }
            >
              CDAGs
            </NavLink>
          </nav>
          <button
            onClick={toggle}
            className="ml-auto text-on-muted hover:text-on text-sm px-2 py-1 border border-line rounded"
            title={`Switch to ${theme === 'light' ? 'dark' : 'light'} mode`}
          >
            {theme === 'light' ? 'Dark' : 'Light'}
          </button>
        </header>
        <main className="p-6">
          <AppRoutes />
        </main>
      </div>
    </BrowserRouter>
  )
}
