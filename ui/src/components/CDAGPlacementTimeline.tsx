import { useEffect, useState } from 'react'
import type { CDAGPlacement, CDAGDetail } from '@/api/client'

interface Props {
  cdag: CDAGDetail
  placements: CDAGPlacement[]
  windowSeconds?: number // how much history to display (default 10 min)
}

const COLORS = [
  '#60a5fa', '#34d399', '#f59e0b', '#f87171',
  '#a78bfa', '#fb923c', '#e879f9', '#2dd4bf',
]

function colorFor(name: string, names: string[]): string {
  return COLORS[names.indexOf(name) % COLORS.length]
}

// Rolling per-task timeline of replica placements. X axis is wall clock
// (now − windowSeconds → now); Y rows are tasks. Each bar is a (task, node)
// segment that held between sample events.
export default function CDAGPlacementTimeline({ cdag, placements, windowSeconds = 600 }: Props) {
  const [now, setNow] = useState(() => Date.now() / 1000)
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now() / 1000), 1000)
    return () => clearInterval(id)
  }, [])

  const tEnd = now
  const tStart = tEnd - windowSeconds

  const tasks = cdag.spec.tasks.map(t => t.name)

  // Build segments per task from sample events, sorted ascending.
  // For each task the series is a list of (node, tsStart); the next event
  // closes the previous segment.
  const byTask: Record<string, CDAGPlacement[]> = {}
  for (const p of placements) {
    ;(byTask[p.task] ??= []).push(p)
  }
  for (const k in byTask) byTask[k].sort((a, b) => a.ts - b.ts)

  // Overlay currently-live placement (from cdag.tasks[].node) as a final open segment.
  const liveByTask: Record<string, string> = {}
  for (const t of cdag.tasks) {
    if (t.node) liveByTask[t.name] = t.node
  }

  type Segment = { task: string; node: string; start: number; end: number }
  const segments: Segment[] = []
  for (const task of tasks) {
    const events = byTask[task] ?? []
    for (let i = 0; i < events.length; i++) {
      const seg: Segment = {
        task,
        node: events[i].node,
        start: events[i].ts,
        end: i + 1 < events.length ? events[i + 1].ts : tEnd,
      }
      segments.push(seg)
    }
    // If the latest in-memory live node differs from the last sample, open a new segment.
    const last = events[events.length - 1]
    const live = liveByTask[task]
    if (live && (!last || last.node !== live)) {
      segments.push({ task, node: live, start: last ? last.ts : tStart, end: tEnd })
    }
  }

  if (segments.length === 0) {
    return <p className="text-on-faint text-sm">No placement history yet. Sampling starts on first CDAG update.</p>
  }

  // Collect nodes for legend.
  const nodes = Array.from(new Set(segments.map(s => s.node))).sort()

  const W = 960
  const ML = 90
  const MR = 20
  const MT = 16
  const MB = 40
  const ROW = 22
  const rowGap = 6
  const H = MT + MB + tasks.length * (ROW + rowGap)
  const innerW = W - ML - MR
  const xs = (t: number) => ML + ((t - tStart) / windowSeconds) * innerW

  const tickCount = 6
  const ticks: number[] = []
  for (let i = 0; i <= tickCount; i++) ticks.push(tStart + (i * windowSeconds) / tickCount)

  function fmtAgo(t: number): string {
    const ago = Math.max(0, tEnd - t)
    if (ago < 60) return `${Math.round(ago)}s`
    if (ago < 3600) return `${Math.round(ago / 60)}m`
    return `${(ago / 3600).toFixed(1)}h`
  }

  return (
    <div>
      <div className="text-xs text-on-faint mb-2">
        Last {Math.round(windowSeconds / 60)} minutes — live updating
      </div>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" style={{ display: 'block' }}>
        {tasks.map((task, i) => {
          const y = MT + i * (ROW + rowGap)
          return (
            <g key={task}>
              <rect x={ML} y={y} width={innerW} height={ROW} fill={i % 2 === 0 ? '#11182710' : '#0f172a10'} />
              <text x={ML - 8} y={y + ROW / 2} textAnchor="end" dominantBaseline="middle" fontSize={11} fill="#9ca3af">
                {task}
              </text>
            </g>
          )
        })}

        {/* Segments */}
        {segments.map((s, i) => {
          const rowIdx = tasks.indexOf(s.task)
          if (rowIdx < 0) return null
          const y = MT + rowIdx * (ROW + rowGap) + 2
          const x1 = xs(Math.max(s.start, tStart))
          const x2 = xs(Math.min(s.end, tEnd))
          const w = Math.max(x2 - x1, 2)
          if (w <= 0) return null
          const color = colorFor(s.node, nodes)
          return (
            <g key={i}>
              <rect x={x1} y={y} width={w} height={ROW - 4} fill={color} fillOpacity={0.85} rx={2} />
              {w > 40 && (
                <text x={x1 + 4} y={y + (ROW - 4) / 2} dominantBaseline="middle" fontSize={9} fill="#0f172a" fontWeight="bold">
                  {s.node}
                </text>
              )}
              <title>{`${s.task} on ${s.node}\n${new Date(s.start * 1000).toLocaleTimeString()} – ${new Date(s.end * 1000).toLocaleTimeString()}`}</title>
            </g>
          )
        })}

        {/* X axis */}
        <line x1={ML} y1={H - MB + 4} x2={ML + innerW} y2={H - MB + 4} stroke="#9ca3af" strokeOpacity={0.4} />
        {ticks.map((t, i) => (
          <g key={i}>
            <line x1={xs(t)} y1={H - MB + 4} x2={xs(t)} y2={H - MB + 8} stroke="#9ca3af" />
            <text x={xs(t)} y={H - MB + 20} textAnchor="middle" fontSize={10} fill="#9ca3af">
              {i === tickCount ? 'now' : `-${fmtAgo(t)}`}
            </text>
          </g>
        ))}
      </svg>

      {/* Legend */}
      <div className="flex gap-4 flex-wrap mt-3 text-xs text-on-faint">
        {nodes.map(node => (
          <div key={node} className="flex items-center gap-1">
            <span
              style={{ backgroundColor: colorFor(node, nodes), width: 10, height: 10, borderRadius: 2, display: 'inline-block' }}
            />
            <span className="font-mono">{node}</span>
          </div>
        ))}
      </div>
    </div>
  )
}
