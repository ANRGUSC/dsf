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

  const maxTime = Math.max(
    ...actualBars.map(b => b.end),
    ...predicted.map(p => p.estEnd),
    1,
  )

  // Layout
  const ML = 90   // left margin for node labels
  const MR = 20
  const MT = 16
  const MB = 44   // bottom margin for x-axis + legend
  const ROW = 52  // height per node row
  const BAR = 15  // bar height
  const W = 860
  const innerW = W - ML - MR
  const innerH = nodes.length * ROW
  const totalH = innerH + MT + MB

  const xs = (t: number) => (t / maxTime) * innerW
  const rowY = (node: string) => MT + nodes.indexOf(node) * ROW

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
            x={ML} y={rowY(node)}
            width={innerW} height={ROW}
            fill={i % 2 === 0 ? rowEven() : rowOdd()}
          />
        ))}

        {/* Vertical grid lines */}
        {ticks.filter(t => t <= maxTime).map(t => (
          <line
            key={t}
            x1={ML + xs(t)} y1={MT}
            x2={ML + xs(t)} y2={MT + innerH}
            stroke={gridStroke()} strokeWidth={1}
          />
        ))}

        {/* Node labels */}
        {nodes.map(node => (
          <text
            key={node}
            x={ML - 8}
            y={rowY(node) + ROW / 2}
            textAnchor="end"
            dominantBaseline="middle"
            fill={labelFill()}
            fontSize={11}
          >
            {node}
          </text>
        ))}

        {/* Predicted bars (dashed border, translucent fill) */}
        {predicted.map(p => {
          const color = taskColor(p.name, taskNames)
          const x = ML + xs(p.estStart)
          const w = Math.max(xs(p.estEnd - p.estStart), 3)
          const y = rowY(p.node) + ROW / 2 - BAR - 2
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
              <title>{`${p.name} predicted: ${p.estStart.toFixed(1)}s – ${p.estEnd.toFixed(1)}s`}</title>
            </g>
          )
        })}

        {/* Actual bars (solid) */}
        {actualBars.map(b => {
          const color = taskColor(b.name, taskNames)
          const x = ML + xs(b.start)
          const w = Math.max(xs(b.end - b.start), 3)
          const y = rowY(b.node) + ROW / 2 + 2
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
              <title>{`${b.name} actual: ${b.start.toFixed(1)}s – ${b.end.toFixed(1)}s`}</title>
            </g>
          )
        })}

        {/* X axis line */}
        <line
          x1={ML} y1={MT + innerH}
          x2={ML + innerW} y2={MT + innerH}
          stroke={axisStroke()} strokeWidth={1}
        />

        {/* X axis ticks + labels */}
        {ticks.filter(t => t <= maxTime + step).map(t => (
          <g key={`xtick-${t}`}>
            <line
              x1={ML + xs(t)} y1={MT + innerH}
              x2={ML + xs(t)} y2={MT + innerH + 5}
              stroke={axisStroke()}
            />
            <text
              x={ML + xs(t)} y={MT + innerH + 16}
              textAnchor="middle"
              fill={tickFill()}
              fontSize={10}
            >
              {t}s
            </text>
          </g>
        ))}

        {/* Legend */}
        <g transform={`translate(${ML}, ${MT + innerH + 30})`}>
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
