import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { api } from '@/api/client'
import StatusBadge from '@/components/StatusBadge'

export default function CDAGTemplateList() {
  const { data: templates, isLoading, error } = useQuery({
    queryKey: ['cdag-templates'],
    queryFn: api.listCDAGTemplates,
  })

  if (isLoading) return <p className="text-on-muted">Loading...</p>
  if (error) return <p className="text-red-500 dark:text-red-400">Error: {String(error)}</p>

  return (
    <div>
      <h1 className="text-lg font-semibold mb-4">CDAG Templates</h1>
      <table className="w-full text-sm border-collapse">
        <thead>
          <tr className="text-left text-on-muted border-b border-line">
            <th className="pb-2 pr-4">Name</th>
            <th className="pb-2 pr-4">Namespace</th>
            <th className="pb-2 pr-4">Scheduler</th>
            <th className="pb-2 pr-4">Tasks</th>
            <th className="pb-2 pr-4">Instances</th>
            <th className="pb-2 pr-4">Last Phase</th>
            <th className="pb-2">Age</th>
          </tr>
        </thead>
        <tbody>
          {templates?.length === 0 && (
            <tr>
              <td colSpan={7} className="pt-4 text-on-faint text-center">
                No CDAG templates found. Create one with <code>dsf cdag-template apply -f template.yml</code>
              </td>
            </tr>
          )}
          {templates?.map(t => (
            <tr key={`${t.namespace}/${t.name}`} className="border-b border-line-soft hover:bg-surface-alt">
              <td className="py-2 pr-4">
                <Link
                  to={`/cdag-templates/${t.namespace}/${t.name}`}
                  className="text-accent hover:text-accent-hover"
                >
                  {t.name}
                </Link>
              </td>
              <td className="py-2 pr-4 text-on-muted">{t.namespace}</td>
              <td className="py-2 pr-4 text-on-muted">{t.scheduler}</td>
              <td className="py-2 pr-4 text-on-muted">{t.taskCount}</td>
              <td className="py-2 pr-4 text-on-muted">{t.instanceCount}</td>
              <td className="py-2 pr-4">
                {t.lastInstancePhase ? <StatusBadge phase={t.lastInstancePhase} /> : <span className="text-on-faint">—</span>}
              </td>
              <td className="py-2 text-on-faint">{formatAge(t.createdAt)}</td>
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
