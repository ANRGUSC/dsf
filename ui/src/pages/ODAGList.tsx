import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { api } from '@/api/client'
import StatusBadge from '@/components/StatusBadge'

export default function ODAGList() {
  const { data: odags, isLoading, error } = useQuery({
    queryKey: ['odags'],
    queryFn: api.listODAGs,
  })

  if (isLoading) return <p className="text-on-muted">Loading...</p>
  if (error) return <p className="text-red-500 dark:text-red-400">Error: {String(error)}</p>

  return (
    <div>
      <h1 className="text-lg font-semibold mb-4">One-Shot DAGs</h1>
      <table className="w-full text-sm border-collapse">
        <thead>
          <tr className="text-left text-on-muted border-b border-line">
            <th className="pb-2 pr-4">Name</th>
            <th className="pb-2 pr-4">Namespace</th>
            <th className="pb-2 pr-4">Phase</th>
            <th className="pb-2 pr-4">Tasks</th>
            <th className="pb-2 pr-4">Makespan</th>
            <th className="pb-2">Age</th>
          </tr>
        </thead>
        <tbody>
          {odags?.length === 0 && (
            <tr>
              <td colSpan={6} className="pt-4 text-on-faint text-center">
                No ODAGs found. Submit one with <code>dsf odag submit -f dag.yml</code>
              </td>
            </tr>
          )}
          {odags?.map(dag => (
            <tr key={`${dag.namespace}/${dag.name}`} className="border-b border-line-soft hover:bg-surface-alt">
              <td className="py-2 pr-4">
                <Link
                  to={`/odags/${dag.namespace}/${dag.name}`}
                  className="text-accent hover:text-accent-hover"
                >
                  {dag.name}
                </Link>
              </td>
              <td className="py-2 pr-4 text-on-muted">{dag.namespace}</td>
              <td className="py-2 pr-4"><StatusBadge phase={dag.phase} /></td>
              <td className="py-2 pr-4 text-on-muted">{dag.taskCount}</td>
              <td className="py-2 pr-4 text-on-muted">
                {dag.makespan != null ? `${dag.makespan.toFixed(1)}s` : '—'}
              </td>
              <td className="py-2 text-on-faint">{formatAge(dag.createdAt)}</td>
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
