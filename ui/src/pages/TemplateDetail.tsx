import { useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api, TemplateRun } from '@/api/client'
import StatusBadge from '@/components/StatusBadge'
import TemplateGraph from '@/components/TemplateGraph'

type Tab = 'graph' | 'tasks' | 'runs' | 'profile' | 'spec'

export default function TemplateDetail() {
  const { namespace, name } = useParams<{ namespace: string; name: string }>()
  const [tab, setTab] = useState<Tab>('graph')
  const queryClient = useQueryClient()

  const { data: template, isLoading, error } = useQuery({
    queryKey: ['template', namespace, name],
    queryFn: () => api.getTemplate(namespace!, name!),
    enabled: !!namespace && !!name,
  })

  const { data: runs } = useQuery({
    queryKey: ['template-runs', namespace, name],
    queryFn: () => api.getTemplateRuns(namespace!, name!),
    enabled: !!namespace && !!name,
  })

  const runMutation = useMutation({
    mutationFn: () => api.runTemplate(namespace!, name!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['template', namespace, name] })
      queryClient.invalidateQueries({ queryKey: ['template-runs', namespace, name] })
      queryClient.invalidateQueries({ queryKey: ['odags'] })
    },
  })

  if (isLoading) return <p className="text-on-muted">Loading...</p>
  if (error) return <p className="text-red-500 dark:text-red-400">Error: {String(error)}</p>
  if (!template) return <p className="text-on-muted">Not found</p>

  const tabs: { key: Tab; label: string }[] = [
    { key: 'graph', label: 'Graph' },
    { key: 'tasks', label: 'Tasks' },
    { key: 'runs', label: `Runs (${template.runCount})` },
    { key: 'profile', label: 'Profile' },
    { key: 'spec', label: 'Spec' },
  ]

  return (
    <div>
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div>
          <Link to="/templates" className="text-on-muted text-sm hover:text-on-secondary">
            Templates
          </Link>
          <span className="text-on-faint mx-1">/</span>
          <span className="text-on-muted text-sm">ODAG</span>
          <span className="text-on-faint mx-2">/</span>
          <h1 className="text-lg font-semibold inline">{template.name}</h1>
          {template.description && (
            <p className="text-on-muted text-sm mt-1">{template.description}</p>
          )}
        </div>
        <button
          onClick={() => runMutation.mutate()}
          disabled={runMutation.isPending}
          className="px-4 py-2 bg-blue-600 hover:bg-blue-500 disabled:bg-gray-400 dark:disabled:bg-gray-600 text-white text-sm rounded"
        >
          {runMutation.isPending ? 'Creating...' : 'New Run'}
        </button>
      </div>

      {/* Summary bar */}
      <div className="flex gap-6 text-sm mb-4 text-on-muted">
        <span>Scheduler: <span className="text-on">{template.scheduler}</span></span>
        <span>Tasks: <span className="text-on">{template.taskCount}</span></span>
        <span>Runs: <span className="text-on">{template.runCount}</span></span>
        {template.lastRunMakespan != null && template.lastRunMakespan > 0 && (
          <span>Last makespan: <span className="text-on">{template.lastRunMakespan.toFixed(1)}s</span></span>
        )}
        <span>Profiling: <span className={template.profilingEnabled ? 'text-green-600 dark:text-green-400' : 'text-on-faint'}>
          {template.profilingEnabled ? 'on' : 'off'}
        </span></span>
      </div>

      {/* Run result toast */}
      {runMutation.isSuccess && (
        <div className="mb-4 p-2 bg-green-100/30 dark:bg-green-900/30 border border-green-300 dark:border-green-800 rounded text-green-700 dark:text-green-300 text-sm">
          {runMutation.data.message}
        </div>
      )}

      {/* Tabs */}
      <div className="flex gap-1 border-b border-line mb-4">
        {tabs.map(t => (
          <button
            key={t.key}
            onClick={() => setTab(t.key)}
            className={`px-4 py-2 text-sm border-b-2 -mb-px ${
              tab === t.key
                ? 'border-blue-500 text-on'
                : 'border-transparent text-on-muted hover:text-on-secondary'
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      {tab === 'graph' && <TemplateGraph tasks={template.spec.tasks} type="odag" />}
      {tab === 'tasks' && <TasksTab spec={template.spec} />}
      {tab === 'runs' && <RunsTab runs={runs ?? []} namespace={namespace!} />}
      {tab === 'profile' && <ProfileTab profileSummary={template.profileSummary} />}
      {tab === 'spec' && <SpecTab spec={template.spec} />}
    </div>
  )
}

/* ---------- Tasks Tab ---------- */

function TasksTab({ spec }: { spec: { tasks: Array<{ name: string; image: string; dependencies: string[]; dataSize?: string; runtime?: number; constraints?: { nodeNames?: string[] } }> } }) {
  return (
    <table className="w-full text-sm border-collapse">
      <thead>
        <tr className="text-left text-on-muted border-b border-line">
          <th className="pb-2 pr-4">Name</th>
          <th className="pb-2 pr-4">Image</th>
          <th className="pb-2 pr-4">Runtime</th>
          <th className="pb-2 pr-4">Data Size</th>
          <th className="pb-2 pr-4">Dependencies</th>
          <th className="pb-2">Constraints</th>
        </tr>
      </thead>
      <tbody>
        {spec.tasks.map(t => (
          <tr key={t.name} className="border-b border-line-soft">
            <td className="py-2 pr-4 text-on">{t.name}</td>
            <td className="py-2 pr-4 text-on-muted text-xs">{t.image.split('/').pop()}</td>
            <td className="py-2 pr-4 text-on-muted">{t.runtime != null ? `${t.runtime}s` : '—'}</td>
            <td className="py-2 pr-4 text-on-muted">{t.dataSize || '—'}</td>
            <td className="py-2 pr-4 text-on-muted">{t.dependencies?.length ? t.dependencies.join(', ') : '—'}</td>
            <td className="py-2 text-on-muted">{t.constraints?.nodeNames?.join(', ') || '—'}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

/* ---------- Runs Tab ---------- */

function RunsTab({ runs, namespace }: { runs: TemplateRun[]; namespace: string }) {
  if (runs.length === 0) {
    return <p className="text-on-faint">No runs yet. Click "New Run" to create one.</p>
  }
  return (
    <table className="w-full text-sm border-collapse">
      <thead>
        <tr className="text-left text-on-muted border-b border-line">
          <th className="pb-2 pr-4">Name</th>
          <th className="pb-2 pr-4">Run</th>
          <th className="pb-2 pr-4">Phase</th>
          <th className="pb-2 pr-4">Makespan</th>
          <th className="pb-2">Age</th>
        </tr>
      </thead>
      <tbody>
        {runs.map(r => (
          <tr key={r.name} className="border-b border-line-soft hover:bg-surface-alt">
            <td className="py-2 pr-4">
              <Link
                to={`/odags/${namespace}/${r.name}`}
                className="text-accent hover:text-accent-hover"
              >
                {r.name}
              </Link>
            </td>
            <td className="py-2 pr-4 text-on-muted">#{r.run}</td>
            <td className="py-2 pr-4"><StatusBadge phase={r.phase} /></td>
            <td className="py-2 pr-4 text-on-muted">
              {r.makespan != null && r.makespan > 0 ? `${r.makespan.toFixed(1)}s` : '—'}
            </td>
            <td className="py-2 text-on-faint">{formatAge(r.createdAt)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

/* ---------- Profile Tab ---------- */

function ProfileTab({ profileSummary }: { profileSummary?: Record<string, Record<string, number>> }) {
  if (!profileSummary || Object.keys(profileSummary).length === 0) {
    return <p className="text-on-faint">No profiler data yet. Run the template a few times to build up profiles.</p>
  }

  // Collect all unique nodes across all tasks.
  const allNodes = new Set<string>()
  for (const nodeMap of Object.values(profileSummary)) {
    for (const node of Object.keys(nodeMap)) {
      allNodes.add(node)
    }
  }
  const nodes = [...allNodes].sort()
  const tasks = Object.keys(profileSummary).sort()

  // Find min/max for heatmap coloring.
  let min = Infinity, max = 0
  for (const nodeMap of Object.values(profileSummary)) {
    for (const val of Object.values(nodeMap)) {
      if (val < min) min = val
      if (val > max) max = val
    }
  }

  const heatColor = (val: number) => {
    if (max === min) return 'bg-blue-100/50 dark:bg-blue-900/50'
    const ratio = (val - min) / (max - min)
    if (ratio < 0.33) return 'bg-green-100/60 dark:bg-green-900/60 text-green-700 dark:text-green-300'
    if (ratio < 0.66) return 'bg-yellow-100/60 dark:bg-yellow-900/60 text-yellow-700 dark:text-yellow-300'
    return 'bg-red-100/60 dark:bg-red-900/60 text-red-700 dark:text-red-300'
  }

  return (
    <div>
      <h3 className="text-sm text-on-muted mb-3">Task x Node Runtime Matrix (EMA seconds)</h3>
      <div className="overflow-x-auto">
        <table className="text-sm border-collapse">
          <thead>
            <tr className="text-on-muted">
              <th className="pb-2 pr-4 text-left">Task</th>
              {nodes.map(n => (
                <th key={n} className="pb-2 px-3 text-center">{n}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {tasks.map(task => (
              <tr key={task} className="border-t border-line">
                <td className="py-2 pr-4 text-on">{task}</td>
                {nodes.map(node => {
                  const val = profileSummary[task]?.[node]
                  return (
                    <td key={node} className={`py-2 px-3 text-center ${val != null ? heatColor(val) : ''}`}>
                      {val != null ? val.toFixed(1) : <span className="text-on-faint">—</span>}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

/* ---------- Spec Tab ---------- */

function SpecTab({ spec }: { spec: Record<string, unknown> }) {
  return (
    <pre className="text-xs text-on-secondary bg-surface-alt p-4 rounded overflow-auto max-h-[600px]">
      {JSON.stringify(spec, null, 2)}
    </pre>
  )
}

/* ---------- Helpers ---------- */

function formatAge(iso: string): string {
  const seconds = Math.floor((Date.now() - new Date(iso).getTime()) / 1000)
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h`
  return `${Math.floor(seconds / 86400)}d`
}
