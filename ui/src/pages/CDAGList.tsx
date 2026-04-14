import { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { api, CDAGSummary } from '@/api/client'
import StatusBadge from '@/components/StatusBadge'

type SortKey = 'name' | 'namespace' | 'phase' | 'scheduler' | 'taskCount' | 'age'
type SortDir = 'asc' | 'desc'

export default function CDAGList() {
  const { data: cdags, isLoading, error } = useQuery({
    queryKey: ['cdags'],
    queryFn: api.listCDAGs,
  })

  const [query, setQuery] = useState('')
  const [sortKey, setSortKey] = useState<SortKey>('age')
  const [sortDir, setSortDir] = useState<SortDir>('asc')

  const filtered = useMemo(() => {
    if (!cdags) return []
    const q = query.trim().toLowerCase()
    const out = q
      ? cdags.filter(c =>
          c.name.toLowerCase().includes(q) ||
          c.namespace.toLowerCase().includes(q) ||
          c.phase.toLowerCase().includes(q) ||
          c.scheduler.toLowerCase().includes(q))
      : [...cdags]

    const cmp = (a: CDAGSummary, b: CDAGSummary): number => {
      switch (sortKey) {
        case 'name':      return a.name.localeCompare(b.name)
        case 'namespace': return a.namespace.localeCompare(b.namespace)
        case 'phase':     return a.phase.localeCompare(b.phase)
        case 'scheduler': return a.scheduler.localeCompare(b.scheduler)
        case 'taskCount': return a.taskCount - b.taskCount
        case 'age':       return new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime()
      }
    }
    out.sort(cmp)
    if (sortDir === 'desc') out.reverse()
    return out
  }, [cdags, query, sortKey, sortDir])

  function toggleSort(k: SortKey) {
    if (sortKey === k) setSortDir(d => (d === 'asc' ? 'desc' : 'asc'))
    else { setSortKey(k); setSortDir('asc') }
  }

  if (isLoading) return <p className="text-on-muted">Loading...</p>
  if (error) return <p className="text-red-500 dark:text-red-400">Error: {String(error)}</p>

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-lg font-semibold">Continuous DAGs</h1>
        <input
          type="text"
          value={query}
          onChange={e => setQuery(e.target.value)}
          placeholder="Filter by name, namespace, phase…"
          className="px-3 py-1 text-sm bg-surface-alt border border-line rounded w-72 focus:outline-none focus:border-accent"
        />
      </div>
      <div className="text-xs text-on-faint mb-2">{filtered.length} of {cdags?.length ?? 0}</div>
      <table className="w-full text-sm border-collapse">
        <thead>
          <tr className="text-left text-on-muted border-b border-line">
            <SortHeader label="Name"      k="name"      sortKey={sortKey} sortDir={sortDir} onClick={toggleSort} />
            <SortHeader label="Namespace" k="namespace" sortKey={sortKey} sortDir={sortDir} onClick={toggleSort} />
            <SortHeader label="Phase"     k="phase"     sortKey={sortKey} sortDir={sortDir} onClick={toggleSort} />
            <SortHeader label="Scheduler" k="scheduler" sortKey={sortKey} sortDir={sortDir} onClick={toggleSort} />
            <SortHeader label="Tasks"     k="taskCount" sortKey={sortKey} sortDir={sortDir} onClick={toggleSort} />
            <SortHeader label="Age"       k="age"       sortKey={sortKey} sortDir={sortDir} onClick={toggleSort} />
          </tr>
        </thead>
        <tbody>
          {filtered.length === 0 && (
            <tr>
              <td colSpan={6} className="pt-4 text-on-faint text-center">
                {cdags?.length ? 'No CDAGs match the filter.' : (
                  <>No CDAGs found. Submit one with <code>dsf cdag submit -f cdag.yml</code></>
                )}
              </td>
            </tr>
          )}
          {filtered.map(cdag => (
            <tr key={`${cdag.namespace}/${cdag.name}`} className="border-b border-line-soft hover:bg-surface-alt">
              <td className="py-2 pr-4">
                <Link
                  to={`/cdags/${cdag.namespace}/${cdag.name}`}
                  className="text-accent hover:text-accent-hover"
                >
                  {cdag.name}
                </Link>
              </td>
              <td className="py-2 pr-4 text-on-muted">{cdag.namespace}</td>
              <td className="py-2 pr-4"><StatusBadge phase={cdag.phase} /></td>
              <td className="py-2 pr-4 text-on-muted">{cdag.scheduler}</td>
              <td className="py-2 pr-4 text-on-muted">{cdag.taskCount}</td>
              <td className="py-2 text-on-faint">{formatAge(cdag.createdAt)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function SortHeader({ label, k, sortKey, sortDir, onClick }: {
  label: string; k: SortKey; sortKey: SortKey; sortDir: SortDir; onClick: (k: SortKey) => void
}) {
  const active = k === sortKey
  const arrow = !active ? '' : sortDir === 'asc' ? ' ▲' : ' ▼'
  return (
    <th
      className={`pb-2 pr-4 cursor-pointer select-none ${active ? 'text-on-secondary' : 'hover:text-on-secondary'}`}
      onClick={() => onClick(k)}
    >
      {label}{arrow}
    </th>
  )
}

function formatAge(iso: string): string {
  const seconds = Math.floor((Date.now() - new Date(iso).getTime()) / 1000)
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h`
  return `${Math.floor(seconds / 86400)}d`
}
