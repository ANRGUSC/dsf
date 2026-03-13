import { useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { api } from '@/api/client'
import StatusBadge from '@/components/StatusBadge'
import CDAGGraph from '@/components/CDAGGraph'

type Tab = 'graph' | 'tasks' | 'spec'

export default function CDAGDetail() {
  const { namespace, name } = useParams<{ namespace: string; name: string }>()
  const [tab, setTab] = useState<Tab>('graph')

  const { data: cdag, isLoading, error } = useQuery({
    queryKey: ['cdag', namespace, name],
    queryFn: () => api.getCDAG(namespace!, name!),
  })

  if (isLoading) return <p className="text-gray-400">Loading...</p>
  if (error || !cdag) return <p className="text-red-400">Error: {String(error)}</p>

  return (
    <div>
      {/* Breadcrumb */}
      <div className="text-sm text-gray-500 mb-4">
        <Link to="/cdags" className="hover:text-gray-300">CDAGs</Link>
        <span className="mx-2">/</span>
        <span className="text-gray-300">{cdag.namespace}/{cdag.name}</span>
      </div>

      {/* Header */}
      <div className="flex items-center gap-4 mb-6">
        <h1 className="text-lg font-semibold">{cdag.name}</h1>
        <StatusBadge phase={cdag.phase} />
        <span className="text-gray-500 text-sm">scheduler: {cdag.scheduler}</span>
        <span className="text-gray-500 text-sm">{cdag.taskCount} tasks</span>
      </div>

      {/* Tabs */}
      <div className="flex gap-4 border-b border-gray-800 mb-6 text-sm">
        {(['graph', 'tasks', 'spec'] as Tab[]).map(t => (
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
      {tab === 'graph' && <CDAGGraph cdag={cdag} />}

      {/* Tasks tab — live replica status */}
      {tab === 'tasks' && (
        <table className="w-full text-sm border-collapse">
          <thead>
            <tr className="text-left text-gray-400 border-b border-gray-800">
              <th className="pb-2 pr-4">Task</th>
              <th className="pb-2 pr-4">Ready / Desired</th>
              <th className="pb-2 pr-4">Node</th>
              <th className="pb-2">Pods</th>
            </tr>
          </thead>
          <tbody>
            {cdag.tasks?.map(task => (
              <tr key={task.name} className="border-b border-gray-900">
                <td className="py-2 pr-4 font-medium">{task.name}</td>
                <td className="py-2 pr-4">
                  <span className={task.readyReplicas === task.desiredReplicas ? 'text-green-400' : 'text-yellow-400'}>
                    {task.readyReplicas ?? 0} / {task.desiredReplicas ?? 1}
                  </span>
                </td>
                <td className="py-2 pr-4 text-gray-400">{task.node ?? '—'}</td>
                <td className="py-2 text-gray-500 text-xs">
                  {task.podNames?.join(', ') ?? '—'}
                </td>
              </tr>
            ))}
            {(!cdag.tasks || cdag.tasks.length === 0) && (
              <tr>
                <td colSpan={4} className="py-4 text-gray-500 text-center">
                  No task status yet — controller is deploying…
                </td>
              </tr>
            )}
          </tbody>
        </table>
      )}

      {/* Spec tab */}
      {tab === 'spec' && (
        <table className="w-full text-sm border-collapse">
          <thead>
            <tr className="text-left text-gray-400 border-b border-gray-800">
              <th className="pb-2 pr-4">Task</th>
              <th className="pb-2 pr-4">Image</th>
              <th className="pb-2 pr-4">Replicas</th>
              <th className="pb-2 pr-4">Dependencies</th>
              <th className="pb-2">Allowed Nodes</th>
            </tr>
          </thead>
          <tbody>
            {cdag.spec?.tasks?.map(t => {
              const allowed = t.constraints?.nodeNames ?? []
              return (
                <tr key={t.name} className="border-b border-gray-900">
                  <td className="py-2 pr-4 font-medium">{t.name}</td>
                  <td className="py-2 pr-4 text-gray-400 text-xs">{t.image}</td>
                  <td className="py-2 pr-4 text-gray-400">{t.replicas ?? 1}</td>
                  <td className="py-2 pr-4 text-gray-500">
                    {t.dependencies.length ? t.dependencies.join(', ') : '—'}
                  </td>
                  <td className="py-2 text-gray-500 text-xs">
                    {allowed.length > 0 ? allowed.join(', ') : <span className="text-gray-700">any</span>}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      )}
    </div>
  )
}
