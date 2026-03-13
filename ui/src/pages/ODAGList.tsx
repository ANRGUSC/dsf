import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { api } from '@/api/client'
import StatusBadge from '@/components/StatusBadge'

export default function ODAGList() {
  const { data: odags, isLoading, error } = useQuery({
    queryKey: ['odags'],
    queryFn: api.listODAGs,
  })

  if (isLoading) return <p className="text-gray-400">Loading...</p>
  if (error) return <p className="text-red-400">Error: {String(error)}</p>

  return (
    <div>
      <h1 className="text-lg font-semibold mb-4">One-Shot DAGs</h1>
      <table className="w-full text-sm border-collapse">
        <thead>
          <tr className="text-left text-gray-400 border-b border-gray-800">
            <th className="pb-2 pr-4">Name</th>
            <th className="pb-2 pr-4">Namespace</th>
            <th className="pb-2 pr-4">Phase</th>
            <th className="pb-2 pr-4">Scheduler</th>
            <th className="pb-2 pr-4">Tasks</th>
            <th className="pb-2 pr-4">Makespan</th>
            <th className="pb-2">Age</th>
          </tr>
        </thead>
        <tbody>
          {odags?.length === 0 && (
            <tr>
              <td colSpan={7} className="pt-4 text-gray-500 text-center">
                No ODAGs found. Submit one with <code>dsf odag submit -f dag.yml</code>
              </td>
            </tr>
          )}
          {odags?.map(dag => (
            <tr key={`${dag.namespace}/${dag.name}`} className="border-b border-gray-900 hover:bg-gray-900">
              <td className="py-2 pr-4">
                <Link
                  to={`/odags/${dag.namespace}/${dag.name}`}
                  className="text-blue-400 hover:text-blue-300"
                >
                  {dag.name}
                </Link>
              </td>
              <td className="py-2 pr-4 text-gray-400">{dag.namespace}</td>
              <td className="py-2 pr-4"><StatusBadge phase={dag.phase} /></td>
              <td className="py-2 pr-4 text-gray-400">{dag.scheduler}</td>
              <td className="py-2 pr-4 text-gray-400">{dag.taskCount}</td>
              <td className="py-2 pr-4 text-gray-400">
                {dag.makespan != null ? `${dag.makespan.toFixed(1)}s` : '—'}
              </td>
              <td className="py-2 text-gray-500">{formatAge(dag.createdAt)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function formatAge(iso: string): string {
  const seconds = Math.floor((Date.now() - new Date(iso).getTime()) / 1000)
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h`
  return `${Math.floor(seconds / 86400)}d`
}
