import { useQuery } from '@tanstack/react-query'
import { Link, useSearchParams } from 'react-router-dom'
import { api } from '@/api/client'
import StatusBadge from '@/components/StatusBadge'

type FilterType = 'all' | 'odag' | 'cdag'

interface UnifiedTemplate {
  type: 'ODAG' | 'CDAG'
  name: string
  namespace: string
  description: string
  scheduler: string
  taskCount: number
  count: number
  lastPhase?: string
  createdAt: string
  detailPath: string
}

export default function TemplateList() {
  const [searchParams, setSearchParams] = useSearchParams()
  const typeParam = searchParams.get('type') as FilterType | null
  const filter: FilterType = typeParam === 'odag' || typeParam === 'cdag' ? typeParam : 'all'

  const { data: odagTemplates, isLoading: loadingODAG, error: errorODAG } = useQuery({
    queryKey: ['templates'],
    queryFn: api.listTemplates,
  })

  const { data: cdagTemplates, isLoading: loadingCDAG, error: errorCDAG } = useQuery({
    queryKey: ['cdag-templates'],
    queryFn: api.listCDAGTemplates,
  })

  const isLoading = loadingODAG || loadingCDAG
  const error = errorODAG || errorCDAG

  if (isLoading) return <p className="text-on-muted">Loading...</p>
  if (error) return <p className="text-red-500 dark:text-red-400">Error: {String(error)}</p>

  // Merge both types into a unified list.
  const allTemplates: UnifiedTemplate[] = []

  for (const t of odagTemplates ?? []) {
    allTemplates.push({
      type: 'ODAG',
      name: t.name,
      namespace: t.namespace,
      description: t.description,
      scheduler: t.scheduler,
      taskCount: t.taskCount,
      count: t.runCount,
      lastPhase: t.lastRunPhase,
      createdAt: t.createdAt,
      detailPath: `/templates/odag/${t.namespace}/${t.name}`,
    })
  }

  for (const t of cdagTemplates ?? []) {
    allTemplates.push({
      type: 'CDAG',
      name: t.name,
      namespace: t.namespace,
      description: t.description,
      scheduler: t.scheduler,
      taskCount: t.taskCount,
      count: t.instanceCount,
      lastPhase: t.lastInstancePhase,
      createdAt: t.createdAt,
      detailPath: `/templates/cdag/${t.namespace}/${t.name}`,
    })
  }

  allTemplates.sort((a, b) => a.name.localeCompare(b.name))

  const templates = filter === 'all'
    ? allTemplates
    : allTemplates.filter(t => t.type.toLowerCase() === filter)

  const setFilter = (f: FilterType) => {
    if (f === 'all') {
      setSearchParams({})
    } else {
      setSearchParams({ type: f })
    }
  }

  const filters: { key: FilterType; label: string }[] = [
    { key: 'all', label: `All (${allTemplates.length})` },
    { key: 'odag', label: `ODAG (${allTemplates.filter(t => t.type === 'ODAG').length})` },
    { key: 'cdag', label: `CDAG (${allTemplates.filter(t => t.type === 'CDAG').length})` },
  ]

  return (
    <div>
      <h1 className="text-lg font-semibold mb-4">Templates</h1>

      {/* Filter tabs */}
      <div className="flex gap-1 border-b border-line mb-4">
        {filters.map(f => (
          <button
            key={f.key}
            onClick={() => setFilter(f.key)}
            className={`px-4 py-2 text-sm border-b-2 -mb-px ${
              filter === f.key
                ? 'border-blue-500 text-on'
                : 'border-transparent text-on-muted hover:text-on-secondary'
            }`}
          >
            {f.label}
          </button>
        ))}
      </div>

      <table className="w-full text-sm border-collapse">
        <thead>
          <tr className="text-left text-on-muted border-b border-line">
            <th className="pb-2 pr-4">Name</th>
            {filter === 'all' && <th className="pb-2 pr-4">Type</th>}
            <th className="pb-2 pr-4">Namespace</th>
            <th className="pb-2 pr-4">Scheduler</th>
            <th className="pb-2 pr-4">Tasks</th>
            <th className="pb-2 pr-4">{filter === 'cdag' ? 'Instances' : filter === 'odag' ? 'Runs' : 'Runs / Instances'}</th>
            <th className="pb-2 pr-4">Last Phase</th>
            <th className="pb-2">Age</th>
          </tr>
        </thead>
        <tbody>
          {templates.length === 0 && (
            <tr>
              <td colSpan={filter === 'all' ? 8 : 7} className="pt-4 text-on-faint text-center">
                No templates found. Create one with <code>dsf template apply -f template.yml</code> or <code>dsf cdag-template apply -f template.yml</code>
              </td>
            </tr>
          )}
          {templates.map(t => (
            <tr key={`${t.type}/${t.namespace}/${t.name}`} className="border-b border-line-soft hover:bg-surface-alt">
              <td className="py-2 pr-4">
                <Link
                  to={t.detailPath}
                  className="text-accent hover:text-accent-hover"
                >
                  {t.name}
                </Link>
              </td>
              {filter === 'all' && (
                <td className="py-2 pr-4">
                  <span className={`text-xs px-2 py-0.5 rounded ${
                    t.type === 'ODAG'
                      ? 'bg-blue-100/50 dark:bg-blue-900/40 text-blue-700 dark:text-blue-300'
                      : 'bg-purple-100/50 dark:bg-purple-900/40 text-purple-700 dark:text-purple-300'
                  }`}>
                    {t.type}
                  </span>
                </td>
              )}
              <td className="py-2 pr-4 text-on-muted">{t.namespace}</td>
              <td className="py-2 pr-4 text-on-muted">{t.scheduler}</td>
              <td className="py-2 pr-4 text-on-muted">{t.taskCount}</td>
              <td className="py-2 pr-4 text-on-muted">{t.count}</td>
              <td className="py-2 pr-4">
                {t.lastPhase ? <StatusBadge phase={t.lastPhase} /> : <span className="text-on-faint">—</span>}
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
