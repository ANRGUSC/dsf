/**
 * CDAGGraph: interactive CDAG visualization using React Flow.
 *
 * Each node shows: task name, replica status (ready/desired), assigned node.
 * On hover: a tooltip with replica details, assigned node, and allowed nodes.
 */

import { useMemo, useCallback } from 'react'
import {
  ReactFlow,
  Node,
  Edge,
  Background,
  Controls,
  MiniMap,
  Position,
  NodeProps,
  Handle,
  useReactFlow,
  ReactFlowProvider,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import type { CDAGDetail, CDAGTaskStatus } from '@/api/client'

// ─── colour palette ──────────────────────────────────────────────────────────

function taskPhase(status?: CDAGTaskStatus): string {
  if (!status) return 'Pending'
  if (status.readyReplicas === 0) return 'Pending'
  if (status.readyReplicas < status.desiredReplicas) return 'Degraded'
  return 'Running'
}

const phaseBg: Record<string, string> = {
  Pending:  '#1f2937',
  Running:  '#78350f',
  Degraded: '#4a1d96',
  Failed:   '#7f1d1d',
}
const phaseBorder: Record<string, string> = {
  Pending:  '#4b5563',
  Running:  '#f59e0b',
  Degraded: '#a78bfa',
  Failed:   '#ef4444',
}
const phaseText: Record<string, string> = {
  Pending:  '#9ca3af',
  Running:  '#fcd34d',
  Degraded: '#c4b5fd',
  Failed:   '#f87171',
}

function bg(phase: string)     { return phaseBg[phase]     ?? phaseBg.Pending }
function border(phase: string) { return phaseBorder[phase] ?? phaseBorder.Pending }
function txt(phase: string)    { return phaseText[phase]   ?? phaseText.Pending }

// ─── custom node ─────────────────────────────────────────────────────────────

interface CDAGNodeData {
  taskName: string
  phase: string
  nodeName?: string
  readyReplicas: number
  desiredReplicas: number
  image?: string
  constraints?: string[]
  hasDeps: boolean
  hasDownstream: boolean
  [key: string]: unknown
}

function CDAGTaskNode({ data }: NodeProps) {
  const d = data as CDAGNodeData
  const phase = d.phase ?? 'Pending'

  return (
    <div
      className="relative group"
      style={{
        background: bg(phase),
        border: `2px solid ${border(phase)}`,
        borderRadius: 10,
        minWidth: 160,
        padding: '10px 14px',
        color: '#f9fafb',
        boxShadow: `0 0 12px ${border(phase)}44`,
        cursor: 'default',
      }}
    >
      {d.hasDeps       && <Handle type="target" position={Position.Left}  style={{ background: border(phase), border: 'none', width: 10, height: 10 }} />}
      {d.hasDownstream && <Handle type="source" position={Position.Right} style={{ background: border(phase), border: 'none', width: 10, height: 10 }} />}

      <div className="font-semibold text-sm text-white mb-1 truncate" style={{ maxWidth: 180 }}>
        {d.taskName}
      </div>
      <div className="text-xs font-medium mb-1" style={{ color: txt(phase) }}>
        {d.readyReplicas}/{d.desiredReplicas} ready
      </div>
      {d.nodeName && (
        <div className="text-xs" style={{ color: '#6b7280' }}>
          {d.nodeName}
        </div>
      )}
      {d.constraints && d.constraints.length > 0 && (
        <div className="text-xs mt-1" style={{ color: '#4b5563' }}>
          {'↦ '}{d.constraints.join(', ')}
        </div>
      )}

      {/* Hover tooltip */}
      <div
        className="absolute left-1/2 z-50 pointer-events-none opacity-0 group-hover:opacity-100 transition-opacity duration-150"
        style={{
          bottom: 'calc(100% + 10px)',
          transform: 'translateX(-50%)',
          minWidth: 220,
          background: '#111827',
          border: `1px solid ${border(phase)}`,
          borderRadius: 8,
          padding: '10px 14px',
          boxShadow: '0 8px 24px rgba(0,0,0,0.6)',
        }}
      >
        <div
          style={{
            position: 'absolute',
            bottom: -6,
            left: '50%',
            transform: 'translateX(-50%)',
            width: 12,
            height: 6,
            overflow: 'hidden',
          }}
        >
          <div style={{
            width: 12, height: 12, background: border(phase),
            transform: 'rotate(45deg)', transformOrigin: 'top left',
            marginTop: -6,
          }} />
        </div>

        <div className="text-xs space-y-1.5">
          <div className="font-semibold text-white text-sm mb-2">{d.taskName}</div>
          <Row label="Status"   value={phase}             color={txt(phase)} />
          <Row label="Replicas" value={`${d.readyReplicas} / ${d.desiredReplicas}`} />
          {d.nodeName && <Row label="Node" value={d.nodeName} />}

          {d.image && (
            <div className="border-t border-gray-700 pt-1.5 mt-1.5">
              <span className="text-gray-500">image: </span>
              <span className="text-gray-300 break-all">{d.image.split('/').pop()}</span>
            </div>
          )}

          {d.constraints && d.constraints.length > 0 && (
            <div className="border-t border-gray-700 pt-1.5 mt-1.5">
              <div className="text-gray-500 mb-1">allowed nodes:</div>
              <div className="flex flex-wrap gap-1">
                {d.constraints.map((n: string) => (
                  <span key={n} style={{ background: '#1f2937', border: '1px solid #374151', borderRadius: 4, padding: '1px 6px', color: '#9ca3af', fontSize: 11 }}>{n}</span>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

function Row({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div className="flex justify-between gap-3">
      <span style={{ color: '#6b7280' }}>{label}</span>
      <span style={{ color: color ?? '#e5e7eb', fontVariantNumeric: 'tabular-nums' }}>{value}</span>
    </div>
  )
}

// ─── layer layout ─────────────────────────────────────────────────────────────

function computeLayers(tasks: Array<{ name: string; dependencies: string[] }>): Record<string, number> {
  const layers: Record<string, number> = {}
  const deps: Record<string, string[]> = {}
  for (const t of tasks) deps[t.name] = t.dependencies

  function layer(name: string): number {
    if (name in layers) return layers[name]
    const d = deps[name] ?? []
    layers[name] = d.length === 0 ? 0 : Math.max(...d.map(layer)) + 1
    return layers[name]
  }

  for (const t of tasks) layer(t.name)
  return layers
}

const nodeTypes = { task: CDAGTaskNode }

// ─── main component ───────────────────────────────────────────────────────────

interface Props { cdag: CDAGDetail }

function CDAGGraphInner({ cdag }: Props) {
  const statusMap = useMemo(() => {
    const m: Record<string, CDAGTaskStatus> = {}
    for (const t of cdag.tasks ?? []) m[t.name] = t
    return m
  }, [cdag.tasks])

  const hasDownstream = useMemo(() => {
    const s = new Set<string>()
    for (const t of cdag.spec.tasks) for (const d of t.dependencies) s.add(d)
    return s
  }, [cdag.spec.tasks])

  const layers = useMemo(() => computeLayers(cdag.spec.tasks), [cdag.spec.tasks])

  const layerGroups = useMemo(() => {
    const g: Record<number, string[]> = {}
    for (const t of cdag.spec.tasks) {
      const l = layers[t.name] ?? 0
      ;(g[l] ??= []).push(t.name)
    }
    return g
  }, [cdag.spec.tasks, layers])

  const NODE_W = 210
  const NODE_H = 100
  const COL_GAP = 100
  const ROW_GAP = 30

  const nodes: Node[] = useMemo(() =>
    cdag.spec.tasks.map((task) => {
      const layer = layers[task.name] ?? 0
      const group = layerGroups[layer] ?? []
      const posInLayer = group.indexOf(task.name)
      const totalInLayer = group.length
      const status = statusMap[task.name]
      const phase = taskPhase(status)

      const x = layer * (NODE_W + COL_GAP)
      const totalHeight = totalInLayer * NODE_H + (totalInLayer - 1) * ROW_GAP
      const y = posInLayer * (NODE_H + ROW_GAP) - totalHeight / 2

      return {
        id: task.name,
        type: 'task',
        position: { x, y },
        data: {
          taskName: task.name,
          phase,
          nodeName: status?.node,
          readyReplicas: status?.readyReplicas ?? 0,
          desiredReplicas: status?.desiredReplicas ?? (task.replicas ?? 1),
          image: task.image,
          constraints: task.constraints?.nodeNames,
          hasDeps: task.dependencies.length > 0,
          hasDownstream: hasDownstream.has(task.name),
        } satisfies CDAGNodeData,
      }
    }),
  [cdag.spec.tasks, statusMap, layers, layerGroups, hasDownstream])

  const edges: Edge[] = useMemo(() =>
    cdag.spec.tasks.flatMap(task =>
      task.dependencies.map(dep => {
        const depStatus = statusMap[dep]
        const depPhase = taskPhase(depStatus)
        return {
          id: `${dep}->${task.name}`,
          source: dep,
          target: task.name,
          animated: depPhase === 'Running',
          style: {
            stroke: depPhase === 'Running'   ? '#f59e0b'
                  : depPhase === 'Degraded'  ? '#a78bfa'
                  : depPhase === 'Failed'    ? '#ef4444'
                  : '#4b5563',
            strokeWidth: 2,
          },
        }
      })
    ),
  [cdag.spec.tasks, statusMap])

  const { fitView } = useReactFlow()
  const onInit = useCallback(() => { fitView({ padding: 0.2 }) }, [fitView])

  return (
    <div style={{ height: 480 }} className="rounded-xl overflow-hidden border border-gray-800 bg-gray-950">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onInit={onInit}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        minZoom={0.3}
        maxZoom={2}
        proOptions={{ hideAttribution: true }}
      >
        <Background color="#1f2937" gap={20} size={1} />
        <Controls style={{ background: '#111827', border: '1px solid #374151' }} />
        <MiniMap
          nodeColor={n => bg((n.data as CDAGNodeData).phase)}
          style={{ background: '#111827', border: '1px solid #374151' }}
          maskColor="rgba(0,0,0,0.5)"
        />
      </ReactFlow>
    </div>
  )
}

export default function CDAGGraph({ cdag }: Props) {
  return (
    <ReactFlowProvider>
      <CDAGGraphInner cdag={cdag} />
    </ReactFlowProvider>
  )
}
