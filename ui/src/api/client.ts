/**
 * API client for the DSF ui-server.
 * All requests go to /api/* — proxied to the Go server in dev, served directly in prod.
 */

export interface TaskStatus {
  name: string
  phase: 'Pending' | 'Running' | 'Succeeded' | 'Failed'
  state?: string
  sending?: boolean
  node?: string
  podName?: string
  startTime?: string
  completionTime?: string
  retries?: number
  message?: string
  dataSize?: string
}

export interface ODAGSummary {
  name: string
  namespace: string
  phase: 'Pending' | 'Scheduling' | 'Running' | 'Succeeded' | 'Failed' | 'Degraded'
  scheduler: string
  taskCount: number
  makespan?: number
  startTime?: string
  completionTime?: string
  createdAt: string
}

export interface PredictedTask {
  name: string
  node: string
  estStart: number
  estEnd: number
}

export interface ODAGDetail extends ODAGSummary {
  tasks: TaskStatus[]
  predictedTasks?: PredictedTask[]
  spec: {
    tasks: Array<{
      name: string
      image: string
      dependencies: string[]
      dataSize?: string
      runtime?: number
      resources?: { cpu?: string; memory?: string }
      constraints?: { nodeNames?: string[] }
    }>
  }
}

export interface HistoryEntry {
  runId: string
  phase: string
  makespan?: number
  startTime: string
  completionTime?: string
}

export interface CDAGTaskStatus {
  name: string
  node?: string
  desiredReplicas: number
  readyReplicas: number
  podNames: string[]
}

export interface CDAGSummary {
  name: string
  namespace: string
  phase: 'Pending' | 'Running' | 'Degraded' | 'Failed'
  scheduler: string
  taskCount: number
  createdAt: string
}

export interface CDAGDetail extends CDAGSummary {
  tasks: CDAGTaskStatus[]
  spec: {
    tasks: Array<{
      name: string
      image: string
      dependencies: string[]
      replicas?: number
      resources?: { cpu?: string; memory?: string }
      constraints?: { nodeNames?: string[] }
    }>
  }
}

export interface TemplateSummary {
  name: string
  namespace: string
  description: string
  scheduler: string
  taskCount: number
  runCount: number
  lastRunMakespan?: number
  lastRunName?: string
  lastRunPhase?: string
  profilingEnabled: boolean
  createdAt: string
}

export interface TemplateDetail extends TemplateSummary {
  profileSummary?: Record<string, Record<string, number>>
  spec: {
    tasks: Array<{
      name: string
      image: string
      dependencies: string[]
      dataSize?: string
      runtime?: number
      resources?: { cpu?: string; memory?: string }
      constraints?: { nodeNames?: string[] }
    }>
    profiling?: {
      enabled?: boolean
      warmupRuns?: number
      minSamples?: number
      emaAlpha?: number
      maxSamples?: number
    }
    defaults?: {
      runtime?: number
      dataSize?: string
    }
    retention?: {
      maxRuns?: number
    }
  }
}

export interface TemplateRun {
  name: string
  namespace: string
  run: string
  phase: string
  makespan?: number
  startTime?: string
  completionTime?: string
  createdAt: string
}

export interface BatchODAGEntry {
  name: string
  delay: number
  spec: Record<string, unknown>
}

async function get<T>(path: string): Promise<T> {
  const res = await fetch(path)
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  return res.json() as Promise<T>
}

export const api = {
  listODAGs: (): Promise<ODAGSummary[]> =>
    get('/api/odags'),

  getODAG: (namespace: string, name: string): Promise<ODAGDetail> =>
    get(`/api/odags/${namespace}/${name}`),

  getODAGHistory: (namespace: string, name: string): Promise<HistoryEntry[]> =>
    get(`/api/odags/${namespace}/${name}/history`),

  retryODAG: (namespace: string, name: string): Promise<{ status: string }> => {
    return fetch(`/api/odags/${namespace}/${name}/retry`, { method: 'POST' })
      .then(res => { if (!res.ok) throw new Error(`${res.status} ${res.statusText}`); return res.json() })
  },

  listCDAGs: (): Promise<CDAGSummary[]> =>
    get('/api/cdags'),

  getCDAG: (namespace: string, name: string): Promise<CDAGDetail> =>
    get(`/api/cdags/${namespace}/${name}`),

  submitBatch: (namespace: string, odags: BatchODAGEntry[]): Promise<{ status: string; count: number }> =>
    fetch('/api/batch', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ namespace, odags }),
    }).then(res => {
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
      return res.json()
    }),

  // Templates
  listTemplates: (): Promise<TemplateSummary[]> =>
    get('/api/templates'),

  getTemplate: (namespace: string, name: string): Promise<TemplateDetail> =>
    get(`/api/templates/${namespace}/${name}`),

  getTemplateRuns: (namespace: string, name: string): Promise<TemplateRun[]> =>
    get(`/api/templates/${namespace}/${name}/runs`),

  runTemplate: (namespace: string, name: string): Promise<{ name: string; run: number; message: string }> =>
    fetch(`/api/templates/${namespace}/${name}/run`, { method: 'POST' })
      .then(res => { if (!res.ok) throw new Error(`${res.status} ${res.statusText}`); return res.json() }),
}
