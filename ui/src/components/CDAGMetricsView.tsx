import type { CDAGTaskMetrics } from '@/api/client'

interface Props {
  data: CDAGTaskMetrics[]
}

function fmtBytes(b: number): string {
  if (b >= 1 << 30) return `${(b / (1 << 30)).toFixed(1)} GiB`
  if (b >= 1 << 20) return `${(b / (1 << 20)).toFixed(1)} MiB`
  if (b >= 1 << 10) return `${(b / (1 << 10)).toFixed(1)} KiB`
  return `${b} B`
}

function fmtLat(s: number | null | undefined): string {
  if (s == null) return '—'
  if (s < 1e-3) return `${(s * 1e6).toFixed(0)} µs`
  if (s < 1) return `${(s * 1000).toFixed(1)} ms`
  return `${s.toFixed(2)} s`
}

export default function CDAGMetricsView({ data }: Props) {
  if (data.length === 0) {
    return <p className="text-on-faint text-sm">No running replicas. Metrics show once tasks come up.</p>
  }
  return (
    <div className="space-y-6">
      <div className="text-xs text-on-faint">
        Live metrics scraped from each replica's :8090/metrics endpoint. Rolling 10s window.
      </div>
      {data.map(t => (
        <div key={t.task}>
          <h3 className="text-sm font-semibold mb-2">{t.task}</h3>
          <table className="w-full text-sm border-collapse">
            <thead>
              <tr className="text-left text-on-muted border-b border-line text-xs">
                <th className="pb-1 pr-3">Replica</th>
                <th className="pb-1 pr-3">Node</th>
                <th className="pb-1 pr-3">Send msgs/s</th>
                <th className="pb-1 pr-3">Send B/s</th>
                <th className="pb-1 pr-3">Recv (peer → msgs/s | last lat | avg | max)</th>
              </tr>
            </thead>
            <tbody>
              {t.replicas.map(r => (
                <tr key={r.pod} className="border-b border-line-soft align-top">
                  <td className="py-1 pr-3 font-mono text-xs">{r.pod}</td>
                  <td className="py-1 pr-3 text-on-muted text-xs">{r.node}</td>
                  {r.error ? (
                    <td colSpan={3} className="py-1 text-red-500 dark:text-red-400 text-xs">{r.error}</td>
                  ) : r.metrics ? (
                    <>
                      <td className="py-1 pr-3 font-mono text-xs">
                        {r.metrics.send ? r.metrics.send.msgsPerSec.toFixed(1) : '—'}
                      </td>
                      <td className="py-1 pr-3 font-mono text-xs">
                        {r.metrics.send ? fmtBytes(r.metrics.send.bytesPerSec) : '—'}
                      </td>
                      <td className="py-1 pr-3 font-mono text-xs">
                        {r.metrics.recv && Object.keys(r.metrics.recv).length > 0
                          ? Object.entries(r.metrics.recv).map(([peer, m]) => (
                              <div key={peer}>
                                <span className="text-on-faint">{peer}</span>: {m.msgsPerSec.toFixed(1)} |{' '}
                                {fmtLat(m.lastLatencySeconds)} | {fmtLat(m.avgLatencySeconds)} | {fmtLat(m.maxLatencySeconds)}
                              </div>
                            ))
                          : <span className="text-on-faint">—</span>}
                      </td>
                    </>
                  ) : (
                    <td colSpan={3} className="py-1 text-on-faint text-xs">no data</td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ))}
    </div>
  )
}
