import { useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'
import { api } from '@/api/client'
import StatusBadge from '@/components/StatusBadge'
import DAGGraph from '@/components/DAGGraph'

type Tab = 'graph' | 'tasks' | 'history'

export default function ODAGDetail() {
  const { namespace, name } = useParams<{ namespace: string; name: string }>()
  const [tab, setTab] = useState<Tab>('graph')

  const { data: dag, isLoading, error } = useQuery({
    queryKey: ['odag', namespace, name],
    queryFn: () => api.getODAG(namespace!, name!),
  })

  const { data: history } = useQuery({
    queryKey: ['odag-history', namespace, name],
    queryFn: () => api.getODAGHistory(namespace!, name!),
    enabled: tab === 'history',
  })

  if (isLoading) return <p className="text-gray-400">Loading...</p>
  if (error || !dag) return <p className="text-red-400">Error: {String(error)}</p>

  return (
    <div>
      {/* Breadcrumb */}
      <div className="text-sm text-gray-500 mb-4">
        <Link to="/" className="hover:text-gray-300">ODAGs</Link>
        <span className="mx-2">/</span>
        <span className="text-gray-300">{dag.namespace}/{dag.name}</span>
      </div>

      {/* Header */}
      <div className="flex items-center gap-4 mb-6">
        <h1 className="text-lg font-semibold">{dag.name}</h1>
        <StatusBadge phase={dag.phase} />
        <span className="text-gray-500 text-sm">scheduler: {dag.scheduler}</span>
        {dag.makespan != null && (
          <span className="text-gray-500 text-sm">makespan: {dag.makespan.toFixed(1)}s</span>
        )}
      </div>

      {/* Tabs */}
      <div className="flex gap-4 border-b border-gray-800 mb-6 text-sm">
        {(['graph', 'tasks', 'history'] as Tab[]).map(t => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`pb-2 capitalize ${tab === t ? 'text-white border-b-2 border-white' : 'text-gray-500 hover:text-gray-300'}`}
          >
            {t}
          </button>
        ))}
      </div>

      {/* Graph tab */}
      {tab === 'graph' && <DAGGraph dag={dag} />}

      {/* Tasks tab */}
      {tab === 'tasks' && (() => {
        const constraintMap = new Map(
          dag.spec.tasks.map(t => [t.name, t.constraints?.nodeNames ?? []])
        )
        return (
          <table className="w-full text-sm border-collapse">
            <thead>
              <tr className="text-left text-gray-400 border-b border-gray-800">
                <th className="pb-2 pr-4">Task</th>
                <th className="pb-2 pr-4">Phase</th>
                <th className="pb-2 pr-4">Node</th>
                <th className="pb-2 pr-4">Allowed Nodes</th>
                <th className="pb-2 pr-4">Pod</th>
                <th className="pb-2 pr-4">Retries</th>
                <th className="pb-2">Message</th>
              </tr>
            </thead>
            <tbody>
              {dag.tasks.map(task => {
                const allowed = constraintMap.get(task.name) ?? []
                return (
                  <tr key={task.name} className="border-b border-gray-900">
                    <td className="py-2 pr-4 font-medium">{task.name}</td>
                    <td className="py-2 pr-4"><StatusBadge phase={task.phase} /></td>
                    <td className="py-2 pr-4 text-gray-400">{task.node ?? '—'}</td>
                    <td className="py-2 pr-4 text-gray-500 text-xs">
                      {allowed.length > 0 ? allowed.join(', ') : <span className="text-gray-700">any</span>}
                    </td>
                    <td className="py-2 pr-4 text-gray-500 text-xs">{task.podName ?? '—'}</td>
                    <td className="py-2 pr-4 text-gray-400">{task.retries ?? 0}</td>
                    <td className="py-2 text-gray-500 text-xs">{task.message ?? ''}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        )
      })()}

      {/* History tab */}
      {tab === 'history' && (
        <div>
          {history && history.length > 0 ? (
            <>
              <ResponsiveContainer width="100%" height={200}>
                <LineChart data={history.map((h, i) => ({ run: i + 1, makespan: h.makespan }))}>
                  <XAxis dataKey="run" stroke="#6b7280" tick={{ fill: '#9ca3af' }} />
                  <YAxis stroke="#6b7280" tick={{ fill: '#9ca3af' }} unit="s" />
                  <Tooltip
                    contentStyle={{ background: '#111827', border: '1px solid #374151', color: '#f9fafb' }}
                    formatter={(v: number) => [`${v.toFixed(1)}s`, 'Makespan']}
                  />
                  <Line type="monotone" dataKey="makespan" stroke="#60a5fa" dot={false} />
                </LineChart>
              </ResponsiveContainer>
              <table className="w-full text-sm border-collapse mt-6">
                <thead>
                  <tr className="text-left text-gray-400 border-b border-gray-800">
                    <th className="pb-2 pr-4">Run</th>
                    <th className="pb-2 pr-4">Phase</th>
                    <th className="pb-2 pr-4">Makespan</th>
                    <th className="pb-2">Started</th>
                  </tr>
                </thead>
                <tbody>
                  {history.map((h, i) => (
                    <tr key={h.runId} className="border-b border-gray-900">
                      <td className="py-2 pr-4 text-gray-400">#{i + 1}</td>
                      <td className="py-2 pr-4"><StatusBadge phase={h.phase} /></td>
                      <td className="py-2 pr-4 text-gray-400">
                        {h.makespan != null ? `${h.makespan.toFixed(1)}s` : '—'}
                      </td>
                      <td className="py-2 text-gray-500 text-xs">{new Date(h.startTime).toLocaleString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </>
          ) : (
            <p className="text-gray-500">No historical runs recorded yet.</p>
          )}
        </div>
      )}
    </div>
  )
}
