import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom'
import ODAGList from '@/pages/ODAGList'
import ODAGDetail from '@/pages/ODAGDetail'
import CDAGList from '@/pages/CDAGList'
import CDAGDetail from '@/pages/CDAGDetail'
import BatchExecution from '@/pages/BatchExecution'
import { useSSE } from '@/hooks/useSSE'

function AppRoutes() {
  // Connect to the SSE stream once; invalidates queries on every server push.
  useSSE()

  return (
    <Routes>
      <Route path="/" element={<ODAGList />} />
      <Route path="/odags/:namespace/:name" element={<ODAGDetail />} />
      <Route path="/batch" element={<BatchExecution />} />
      <Route path="/cdags" element={<CDAGList />} />
      <Route path="/cdags/:namespace/:name" element={<CDAGDetail />} />
    </Routes>
  )
}

export default function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-gray-950 text-gray-100 font-mono">
        <header className="border-b border-gray-800 px-6 py-3 flex items-center gap-8">
          <span className="font-bold text-white tracking-tight">DSF</span>
          <nav className="flex gap-6 text-sm">
            <NavLink
              to="/"
              end
              className={({ isActive }) =>
                isActive ? 'text-white' : 'text-gray-400 hover:text-gray-200'
              }
            >
              ODAGs
            </NavLink>
            <NavLink
              to="/batch"
              className={({ isActive }) =>
                isActive ? 'text-white' : 'text-gray-400 hover:text-gray-200'
              }
            >
              Batch
            </NavLink>
            <NavLink
              to="/cdags"
              className={({ isActive }) =>
                isActive ? 'text-white' : 'text-gray-400 hover:text-gray-200'
              }
            >
              CDAGs
            </NavLink>
          </nav>
        </header>
        <main className="p-6">
          <AppRoutes />
        </main>
      </div>
    </BrowserRouter>
  )
}
