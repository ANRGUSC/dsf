import type { ODAGDetail } from '@/api/client'

function isDark() { return document.documentElement.classList.contains('dark') }
function rowEven() { return isDark() ? '#0f172a' : '#f9fafb' }
function rowOdd() { return isDark() ? '#111827' : '#f3f4f6' }
function gridStroke() { return isDark() ? '#1f2937' : '#e5e7eb' }
function axisStroke() { return isDark() ? '#374151' : '#d1d5db' }
function labelFill() { return isDark() ? '#9ca3af' : '#6b7280' }
function tickFill() { return isDark() ? '#6b7280' : '#9ca3af' }
function barTextDark() { return isDark() ? '#0f172a' : '#ffffff' }
function legendFill() { return isDark() ? '#6b7280' : '#9ca3af' }
function dashStroke() { return isDark() ? '#374151' : '#d1d5db' }

const TASK_COLORS = [
  '#60a5fa', '#34d399', '#f59e0b', '#f87171',
  '#a78bfa', '#fb923c', '#e879f9', '#2dd4bf',
]

function taskColor(name: string, names: string[]): string {
  return TASK_COLORS[names.indexOf(name) % TASK_COLORS.length]
}

interface Props {
  dag: ODAGDetail
}

export default function GanttChart({ dag }: Props) {
  const predicted = dag.predictedTasks ?? []
  const taskNames = dag.spec.tasks.map(t => t.name)

  // Actual bars: tasks with a startTime
  const activeTasks = dag.tasks.filter(t => t.startTime && t.node)
  const refMs = activeTasks.length > 0
    ? Math.min(...activeTasks.map(t => new Date(t.startTime!).getTime()))
    : null

  const actualBars = activeTasks.map(t => ({
    name: t.name,
    node: t.node!,
    start: (new Date(t.startTime!).getTime() - refMs!) / 1000,
    end: t.completionTime
      ? (new Date(t.completionTime).getTime() - refMs!) / 1000
      : (Date.now() - refMs!) / 1000,
  }))

  // Collect all nodes
  const nodeSet = new Set<string>()
  predicted.forEach(p => nodeSet.add(p.node))
  actualBars.forEach(b => nodeSet.add(b.node))
  const nodes = Array.from(nodeSet).sort()

  if (nodes.length === 0) {
    return <p className="text-on-faint text-sm">No schedule data yet.</p>
  }

  // Count max parallel tasks per node (for row height).
  // Group tasks by node and check how many overlap in time.
  const predictedByNode: Record<string, typeof predicted> = {}
  for (const p of predicted) {
    ;(predictedByNode[p.node] ??= []).push(p)
  }
  const actualByNode: Record<string, typeof actualBars> = {}
  for (const b of actualBars) {
    ;(actualByNode[b.node] ??= []).push(b)
  }

  // Count max overlapping bars for each node.
  function maxOverlap(bars: Array<{ start: number; end: number }>): number {
    if (bars.length <= 1) return bars.length
    const events: Array<{ t: number; delta: number }> = []
    for (const b of bars) {
      events.push({ t: b.start, delta: 1 })
      events.push({ t: b.end, delta: -1 })
    }
    events.sort((a, b) => a.t - b.t || a.delta - b.delta)
    let cur = 0, mx = 0
    for (const e of events) {
      cur += e.delta
      mx = Math.max(mx, cur)
    }
    return mx
  }

  // For each node, compute how many sub-rows are needed (predicted + actual).
  const nodeSlots: Record<string, number> = {}
  for (const node of nodes) {
    const pOverlap = maxOverlap((predictedByNode[node] ?? []).map(p => ({ start: p.estStart, end: p.estEnd })))
    const aOverlap = maxOverlap(actualByNode[node] ?? [])
    nodeSlots[node] = Math.max(pOverlap, aOverlap, 1)
  }

  const maxTime = Math.max(
    ...actualBars.map(b => b.end),
    ...predicted.map(p => p.estEnd),
    1,
  )

  // Layout
  const ML = 90
  const MR = 20
  const MT = 16
  const MB = 44
  const BAR = 14
  const BAR_GAP = 2
  const NODE_PAD = 8 // padding top/bottom within node row
  const W = 960

  // Each node row height depends on max parallel tasks.
  function nodeRowHeight(node: string): number {
    const slots = nodeSlots[node]
    // Two sets of bars (predicted + actual), each with `slots` sub-rows.
    return NODE_PAD * 2 + slots * (BAR + BAR_GAP) * 2 + 4
  }

  const innerW = W - ML - MR
  let totalInnerH = 0
  const nodeYOffset: Record<string, number> = {}
  for (const node of nodes) {
    nodeYOffset[node] = MT + totalInnerH
    totalInnerH += nodeRowHeight(node)
  }
  const totalH = totalInnerH + MT + MB

  const xs = (t: number) => (t / maxTime) * innerW

  // Assign sub-row indices to overlapping bars on the same node.
  function assignSubRows(bars: Array<{ name: string; start: number; end: number }>): Record<string, number> {
    const sorted = [...bars].sort((a, b) => a.start - b.start)
    const rows: number[] = [] // end time of each sub-row
    const assignment: Record<string, number> = {}
    for (const bar of sorted) {
      let placed = false
      for (let r = 0; r < rows.length; r++) {
        if (bar.start >= rows[r]) {
          rows[r] = bar.end
          assignment[bar.name] = r
          placed = true
          break
        }
      }
      if (!placed) {
        assignment[bar.name] = rows.length
        rows.push(bar.end)
      }
    }
    return assignment
  }

  // Pre-compute sub-row assignments per node.
  const predSubRows: Record<string, Record<string, number>> = {}
  const actSubRows: Record<string, Record<string, number>> = {}
  for (const node of nodes) {
    predSubRows[node] = assignSubRows(
      (predictedByNode[node] ?? []).map(p => ({ name: p.name, start: p.estStart, end: p.estEnd }))
    )
    actSubRows[node] = assignSubRows(actualByNode[node] ?? [])
  }

  // Nice tick values
  const tickCount = 7
  const rawStep = maxTime / tickCount
  const step = Math.ceil(rawStep)
  const ticks: number[] = []
  for (let t = 0; t <= maxTime + step; t += step) ticks.push(Math.round(t))

  return (
    <div className="overflow-x-auto">
      <svg
        viewBox={`0 0 ${W} ${totalH}`}
        width="100%"
        style={{ display: 'block', fontFamily: 'inherit' }}
      >
        {/* Alternating row backgrounds */}
        {nodes.map((node, i) => (
          <rect
            key={node}
            x={ML} y={nodeYOffset[node]}
            width={innerW} height={nodeRowHeight(node)}
            fill={i % 2 === 0 ? rowEven() : rowOdd()}
          />
        ))}

        {/* Dashed horizontal lines between node rows */}
        {nodes.map((node, i) => {
          if (i === 0) return null
          return (
            <line
              key={`sep-${node}`}
              x1={ML} y1={nodeYOffset[node]}
              x2={ML + innerW} y2={nodeYOffset[node]}
              stroke={dashStroke()} strokeWidth={1} strokeDasharray="6 4"
            />
          )
        })}

        {/* Vertical grid lines */}
        {ticks.filter(t => t <= maxTime).map(t => (
          <line
            key={t}
            x1={ML + xs(t)} y1={MT}
            x2={ML + xs(t)} y2={MT + totalInnerH}
            stroke={gridStroke()} strokeWidth={1}
          />
        ))}

        {/* Node labels */}
        {nodes.map(node => (
          <text
            key={node}
            x={ML - 8}
            y={nodeYOffset[node] + nodeRowHeight(node) / 2}
            textAnchor="end"
            dominantBaseline="middle"
            fill={labelFill()}
            fontSize={11}
          >
            {node}
          </text>
        ))}

        {/* Predicted bars (dashed, stacked by sub-row) */}
        {predicted.map(p => {
          const color = taskColor(p.name, taskNames)
          const x = ML + xs(p.estStart)
          const w = Math.max(xs(p.estEnd - p.estStart), 3)
          const subRow = predSubRows[p.node]?.[p.name] ?? 0
          const y = nodeYOffset[p.node] + NODE_PAD + subRow * (BAR + BAR_GAP)
          return (
            <g key={`pred-${p.name}`}>
              <rect
                x={x} y={y} width={w} height={BAR}
                fill={color} fillOpacity={0.18}
                stroke={color} strokeWidth={1.5} strokeDasharray="5 3"
                rx={2}
              />
              <text
                x={x + 4} y={y + BAR / 2}
                dominantBaseline="middle"
                fill={color} fillOpacity={0.8}
                fontSize={9}
                style={{ pointerEvents: 'none' }}
              >
                {p.name}
              </text>
              <title>{`${p.name} predicted: ${p.estStart.toFixed(1)}s – ${p.estEnd.toFixed(1)}s (${p.node})`}</title>
            </g>
          )
        })}

        {/* Actual bars (solid, stacked by sub-row) */}
        {actualBars.map(b => {
          const color = taskColor(b.name, taskNames)
          const x = ML + xs(b.start)
          const w = Math.max(xs(b.end - b.start), 3)
          const slots = nodeSlots[b.node]
          const subRow = actSubRows[b.node]?.[b.name] ?? 0
          // Actual bars go below predicted bars.
          const predHeight = slots * (BAR + BAR_GAP)
          const y = nodeYOffset[b.node] + NODE_PAD + predHeight + 4 + subRow * (BAR + BAR_GAP)
          return (
            <g key={`actual-${b.name}`}>
              <rect
                x={x} y={y} width={w} height={BAR}
                fill={color} fillOpacity={0.9}
                rx={2}
              />
              <text
                x={x + 4} y={y + BAR / 2}
                dominantBaseline="middle"
                fill={barTextDark()}
                fontWeight="bold"
                fontSize={9}
                style={{ pointerEvents: 'none' }}
              >
                {b.name}
              </text>
              <title>{`${b.name} actual: ${b.start.toFixed(1)}s – ${b.end.toFixed(1)}s (${b.node})`}</title>
            </g>
          )
        })}

        {/* X axis line */}
        <line
          x1={ML} y1={MT + totalInnerH}
          x2={ML + innerW} y2={MT + totalInnerH}
          stroke={axisStroke()} strokeWidth={1}
        />

        {/* X axis ticks + labels */}
        {ticks.filter(t => t <= maxTime + step).map(t => (
          <g key={`xtick-${t}`}>
            <line
              x1={ML + xs(t)} y1={MT + totalInnerH}
              x2={ML + xs(t)} y2={MT + totalInnerH + 5}
              stroke={axisStroke()}
            />
            <text
              x={ML + xs(t)} y={MT + totalInnerH + 16}
              textAnchor="middle"
              fill={tickFill()}
              fontSize={10}
            >
              {t}s
            </text>
          </g>
        ))}

        {/* Legend */}
        <g transform={`translate(${ML}, ${MT + totalInnerH + 30})`}>
          <rect x={0} y={0} width={14} height={10} fill="#9ca3af" fillOpacity={0.18}
            stroke="#9ca3af" strokeDasharray="5 3" strokeWidth={1.5} rx={1} />
          <text x={20} y={5} dominantBaseline="middle" fill={legendFill()} fontSize={11}>Predicted</text>
          <rect x={90} y={0} width={14} height={10} fill="#9ca3af" fillOpacity={0.9} rx={1} />
          <text x={110} y={5} dominantBaseline="middle" fill={legendFill()} fontSize={11}>Actual</text>
        </g>
      </svg>
    </div>
  )
}
