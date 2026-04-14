import { useQuery } from '@tanstack/react-query'
import { api, ClusterNode } from '@/api/client'

function fmtBytes(b: number): string {
  if (b >= 1 << 30) return `${(b / (1 << 30)).toFixed(1)} GiB`
  if (b >= 1 << 20) return `${(b / (1 << 20)).toFixed(0)} MiB`
  return `${b} B`
}

export default function Cluster() {
  const { data: nodes, isLoading, error } = useQuery({
    queryKey: ['cluster-nodes'],
    queryFn: api.getClusterNodes,
    refetchInterval: 5000,
  })

  if (isLoading) return <p className="text-on-muted">Loading...</p>
  if (error) return <p className="text-red-500 dark:text-red-400">Error: {String(error)}</p>
  if (!nodes) return null

  const totals = nodes.reduce((acc, n) => {
    acc.nodes++
    if (n.ready && n.schedulable) acc.usable++
    acc.cpu += n.allocCPUMillis
    acc.mem += n.allocMemBytes
    acc.used += n.usedCPUMillis
    acc.usedMem += n.usedMemBytes
    acc.odag += n.runningOdagTasks
    acc.cdag += n.runningCdagTasks
    return acc
  }, { nodes: 0, usable: 0, cpu: 0, mem: 0, used: 0, usedMem: 0, odag: 0, cdag: 0 })

  return (
    <div>
      <h1 className="text-lg font-semibold mb-2">Cluster</h1>
      <div className="text-xs text-on-faint mb-4">Auto-refreshes every 5s</div>

      {/* Aggregate stats */}
      <div className="grid grid-cols-2 md:grid-cols-6 gap-3 text-sm mb-6">
        <Card label="Nodes" value={`${totals.usable}/${totals.nodes}`} sub="usable" />
        <Card label="Cluster CPU" value={`${(totals.cpu / 1000).toFixed(1)} cores`} sub={`${(totals.used / 1000).toFixed(2)} in use`} />
        <Card label="Cluster Memory" value={fmtBytes(totals.mem)} sub={`${fmtBytes(totals.usedMem)} in use`} />
        <Card label="CPU load" value={`${totals.cpu === 0 ? 0 : (totals.used / totals.cpu * 100).toFixed(1)}%`} sub="cluster-wide" />
        <Card label="Running ODAG tasks" value={String(totals.odag)} sub="across all nodes" />
        <Card label="Running CDAG tasks" value={String(totals.cdag)} sub="across all nodes" />
      </div>

      <table className="w-full text-sm border-collapse">
        <thead>
          <tr className="text-left text-on-muted border-b border-line">
            <th className="pb-2 pr-4">Node</th>
            <th className="pb-2 pr-4">Status</th>
            <th className="pb-2 pr-4">Role</th>
            <th className="pb-2 pr-4">IP</th>
            <th className="pb-2 pr-4 w-32">CPU</th>
            <th className="pb-2 pr-4 w-32">Memory</th>
            <th className="pb-2 pr-4">Pods</th>
            <th className="pb-2 pr-4">ODAG</th>
            <th className="pb-2">CDAG</th>
          </tr>
        </thead>
        <tbody>
          {nodes.map(n => <NodeRow key={n.name} n={n} />)}
        </tbody>
      </table>
    </div>
  )
}

function Card({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="bg-surface-alt border border-line rounded p-3">
      <div className="text-xs text-on-faint">{label}</div>
      <div className="text-on font-mono text-base">{value}</div>
      {sub && <div className="text-xs text-on-faint mt-0.5">{sub}</div>}
    </div>
  )
}

function NodeRow({ n }: { n: ClusterNode }) {
  const statusColor = !n.ready
    ? 'text-red-500 dark:text-red-400'
    : !n.schedulable
    ? 'text-amber-500 dark:text-amber-400'
    : 'text-green-600 dark:text-green-400'
  const statusText = !n.ready ? 'NotReady' : !n.schedulable ? 'Cordoned' : 'Ready'
  return (
    <tr className="border-b border-line-soft">
      <td className="py-2 pr-4 font-medium">{n.name}</td>
      <td className={`py-2 pr-4 text-xs ${statusColor}`}>{statusText}</td>
      <td className="py-2 pr-4 text-on-muted text-xs">{n.roles}</td>
      <td className="py-2 pr-4 text-on-muted text-xs font-mono">{n.internalIP || '—'}</td>
      <td className="py-2 pr-4">
        <Bar pct={n.cpuPct} />
        <div className="text-xs text-on-faint mt-0.5 font-mono">
          {(n.usedCPUMillis / 1000).toFixed(2)}/{(n.allocCPUMillis / 1000).toFixed(1)} cores ({n.cpuPct.toFixed(0)}%)
        </div>
      </td>
      <td className="py-2 pr-4">
        <Bar pct={n.memPct} />
        <div className="text-xs text-on-faint mt-0.5 font-mono">
          {fmtBytes(n.usedMemBytes)}/{fmtBytes(n.allocMemBytes)} ({n.memPct.toFixed(0)}%)
        </div>
      </td>
      <td className="py-2 pr-4 text-on-muted">{n.totalPods}</td>
      <td className="py-2 pr-4 text-on-muted">
        {n.runningOdagTasks > 0 ? <span className="text-on">{n.runningOdagTasks}</span> : <span className="text-on-faint">0</span>}
        <span className="text-on-faint"> / {n.odagTasks}</span>
      </td>
      <td className="py-2 text-on-muted">
        {n.runningCdagTasks > 0 ? <span className="text-on">{n.runningCdagTasks}</span> : <span className="text-on-faint">0</span>}
        <span className="text-on-faint"> / {n.cdagTasks}</span>
      </td>
    </tr>
  )
}

function Bar({ pct }: { pct: number }) {
  const clamped = Math.max(0, Math.min(100, pct))
  const color =
    clamped > 85 ? 'bg-red-500 dark:bg-red-400' :
    clamped > 65 ? 'bg-amber-500 dark:bg-amber-400' :
    'bg-blue-500 dark:bg-blue-400'
  return (
    <div className="w-full bg-surface-alt rounded h-2 overflow-hidden">
      <div className={`h-2 ${color}`} style={{ width: `${clamped}%` }} />
    </div>
  )
}
