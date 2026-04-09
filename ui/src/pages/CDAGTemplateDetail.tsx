import { useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api, CDAGTemplateInstance } from '@/api/client'
import StatusBadge from '@/components/StatusBadge'
import TemplateGraph from '@/components/TemplateGraph'

type Tab = 'graph' | 'tasks' | 'instances' | 'spec'

export default function CDAGTemplateDetail() {
  const { namespace, name } = useParams<{ namespace: string; name: string }>()
  const [tab, setTab] = useState<Tab>('graph')
  const queryClient = useQueryClient()

  const { data: template, isLoading, error } = useQuery({
    queryKey: ['cdag-template', namespace, name],
    queryFn: () => api.getCDAGTemplate(namespace!, name!),
    enabled: !!namespace && !!name,
  })

  const { data: instances } = useQuery({
    queryKey: ['cdag-template-instances', namespace, name],
    queryFn: () => api.getCDAGTemplateInstances(namespace!, name!),
    enabled: !!namespace && !!name,
  })

  const deployMutation = useMutation({
    mutationFn: () => api.deployCDAGTemplate(namespace!, name!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['cdag-template', namespace, name] })
      queryClient.invalidateQueries({ queryKey: ['cdag-template-instances', namespace, name] })
      queryClient.invalidateQueries({ queryKey: ['cdags'] })
    },
  })

  if (isLoading) return <p className="text-on-muted">Loading...</p>
  if (error) return <p className="text-red-500 dark:text-red-400">Error: {String(error)}</p>
  if (!template) return <p className="text-on-muted">Not found</p>

  const tabs: { key: Tab; label: string }[] = [
    { key: 'graph', label: 'Graph' },
    { key: 'tasks', label: 'Tasks' },
    { key: 'instances', label: `Instances (${template.instanceCount})` },
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
          <span className="text-on-muted text-sm">CDAG</span>
          <span className="text-on-faint mx-2">/</span>
          <h1 className="text-lg font-semibold inline">{template.name}</h1>
          {template.description && (
            <p className="text-on-muted text-sm mt-1">{template.description}</p>
          )}
        </div>
        <button
          onClick={() => deployMutation.mutate()}
          disabled={deployMutation.isPending}
          className="px-4 py-2 bg-blue-600 hover:bg-blue-500 disabled:bg-gray-400 dark:disabled:bg-gray-600 text-white text-sm rounded"
        >
          {deployMutation.isPending ? 'Deploying...' : 'Deploy Instance'}
        </button>
      </div>

      {/* Summary bar */}
      <div className="flex gap-6 text-sm mb-4 text-on-muted">
        <span>Scheduler: <span className="text-on">{template.scheduler}</span></span>
        <span>Tasks: <span className="text-on">{template.taskCount}</span></span>
        <span>Instances: <span className="text-on">{template.instanceCount}</span></span>
        {template.lastInstancePhase && (
          <span>Last: <StatusBadge phase={template.lastInstancePhase} /></span>
        )}
      </div>

      {/* Deploy result toast */}
      {deployMutation.isSuccess && (
        <div className="mb-4 p-2 bg-green-100/30 dark:bg-green-900/30 border border-green-300 dark:border-green-800 rounded text-green-700 dark:text-green-300 text-sm">
          {deployMutation.data.message}
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
      {tab === 'graph' && <TemplateGraph tasks={template.spec.tasks} type="cdag" />}
      {tab === 'tasks' && <TasksTab spec={template.spec} />}
      {tab === 'instances' && <InstancesTab instances={instances ?? []} namespace={namespace!} />}
      {tab === 'spec' && <SpecTab spec={template.spec} />}
    </div>
  )
}

/* ---------- Tasks Tab ---------- */

function TasksTab({ spec }: { spec: { tasks: Array<{ name: string; image: string; dependencies: string[]; replicas?: number; constraints?: { nodeNames?: string[] } }> } }) {
  return (
    <table className="w-full text-sm border-collapse">
      <thead>
        <tr className="text-left text-on-muted border-b border-line">
          <th className="pb-2 pr-4">Name</th>
          <th className="pb-2 pr-4">Image</th>
          <th className="pb-2 pr-4">Replicas</th>
          <th className="pb-2 pr-4">Dependencies</th>
          <th className="pb-2">Constraints</th>
        </tr>
      </thead>
      <tbody>
        {spec.tasks.map(t => (
          <tr key={t.name} className="border-b border-line-soft">
            <td className="py-2 pr-4 text-on">{t.name}</td>
            <td className="py-2 pr-4 text-on-muted text-xs">{t.image.split('/').pop()}</td>
            <td className="py-2 pr-4 text-on-muted">{t.replicas ?? 1}</td>
            <td className="py-2 pr-4 text-on-muted">{t.dependencies?.length ? t.dependencies.join(', ') : '—'}</td>
            <td className="py-2 text-on-muted">{t.constraints?.nodeNames?.join(', ') || '—'}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

/* ---------- Instances Tab ---------- */

function InstancesTab({ instances, namespace }: { instances: CDAGTemplateInstance[]; namespace: string }) {
  if (instances.length === 0) {
    return <p className="text-on-faint">No instances yet. Click "Deploy Instance" to create one.</p>
  }
  return (
    <table className="w-full text-sm border-collapse">
      <thead>
        <tr className="text-left text-on-muted border-b border-line">
          <th className="pb-2 pr-4">Name</th>
          <th className="pb-2 pr-4">Instance</th>
          <th className="pb-2 pr-4">Phase</th>
          <th className="pb-2">Age</th>
        </tr>
      </thead>
      <tbody>
        {instances.map(inst => (
          <tr key={inst.name} className="border-b border-line-soft hover:bg-surface-alt">
            <td className="py-2 pr-4">
              <Link
                to={`/cdags/${namespace}/${inst.name}`}
                className="text-accent hover:text-accent-hover"
              >
                {inst.name}
              </Link>
            </td>
            <td className="py-2 pr-4 text-on-muted">#{inst.instance}</td>
            <td className="py-2 pr-4"><StatusBadge phase={inst.phase} /></td>
            <td className="py-2 text-on-faint">{formatAge(inst.createdAt)}</td>
          </tr>
        ))}
      </tbody>
    </table>
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
