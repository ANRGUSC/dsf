import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { api } from '@/api/client'
import StatusBadge from '@/components/StatusBadge'

export default function CDAGList() {
  const { data: cdags, isLoading, error } = useQuery({
    queryKey: ['cdags'],
    queryFn: api.listCDAGs,
  })

  if (isLoading) return <p className="text-gray-400">Loading...</p>
  if (error) return <p className="text-red-400">Error: {String(error)}</p>

  return (
    <div>
      <h1 className="text-lg font-semibold mb-4">Continuous DAGs</h1>
      <table className="w-full text-sm border-collapse">
        <thead>
          <tr className="text-left text-gray-400 border-b border-gray-800">
            <th className="pb-2 pr-4">Name</th>
            <th className="pb-2 pr-4">Namespace</th>
            <th className="pb-2 pr-4">Phase</th>
            <th className="pb-2 pr-4">Scheduler</th>
            <th className="pb-2 pr-4">Tasks</th>
            <th className="pb-2">Age</th>
          </tr>
        </thead>
        <tbody>
          {cdags?.length === 0 && (
            <tr>
              <td colSpan={6} className="pt-4 text-gray-500 text-center">
                No CDAGs found. Submit one with <code>dsf cdag submit -f cdag.yml</code>
              </td>
            </tr>
          )}
          {cdags?.map(cdag => (
            <tr key={`${cdag.namespace}/${cdag.name}`} className="border-b border-gray-900 hover:bg-gray-900">
              <td className="py-2 pr-4">
                <Link
                  to={`/cdags/${cdag.namespace}/${cdag.name}`}
                  className="text-blue-400 hover:text-blue-300"
                >
                  {cdag.name}
                </Link>
              </td>
              <td className="py-2 pr-4 text-gray-400">{cdag.namespace}</td>
              <td className="py-2 pr-4"><StatusBadge phase={cdag.phase} /></td>
              <td className="py-2 pr-4 text-gray-400">{cdag.scheduler}</td>
              <td className="py-2 pr-4 text-gray-400">{cdag.taskCount}</td>
              <td className="py-2 text-gray-500">{formatAge(cdag.createdAt)}</td>
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
