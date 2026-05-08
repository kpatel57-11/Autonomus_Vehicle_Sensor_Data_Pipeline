import { create } from 'zustand'

export type PipelineStatus = 'idle' | 'running' | 'success' | 'error'
export type StageName = 'sources' | 'validators' | 'transforms' | 'processors' | 'sink' | 'checkpoint'

export interface StageState {
  name: StageName; label: string; status: PipelineStatus
  recordsIn: number; recordsOut: number; durationMs: number; error?: string
}
export interface PipelineRun {
  id: string; mode: 'batch' | 'stream'; recipe: string
  status: PipelineStatus; startedAt: number; finishedAt?: number; metrics: Record<string, any>
}
export interface PipelineMetrics {
  totalRuns: number; successfulRuns: number; failedRuns: number
  totalRecordsProcessed: number; totalRecordsRejected: number
  avgRunDurationS: number; throughputRecordsPerRun: number
}

interface Store {
  status: PipelineStatus; currentRunId: string | null
  stages: StageState[]; runs: PipelineRun[]; metrics: PipelineMetrics | null
  streamMetrics: any[]
  setStatus: (s: PipelineStatus) => void
  setStageStatus: (name: StageName, u: Partial<StageState>) => void
  addRun: (r: PipelineRun) => void; updateRun: (id: string, u: Partial<PipelineRun>) => void
  setMetrics: (m: PipelineMetrics) => void; addStreamMetric: (m: any) => void; resetStages: () => void
}

const DEFAULT_STAGES: StageState[] = [
  { name:'sources',    label:'1. SOURCES',        status:'idle', recordsIn:0, recordsOut:0, durationMs:0 },
  { name:'validators', label:'2. VALIDATORS',     status:'idle', recordsIn:0, recordsOut:0, durationMs:0 },
  { name:'transforms', label:'3. SQL TRANSFORMS', status:'idle', recordsIn:0, recordsOut:0, durationMs:0 },
  { name:'processors', label:'4. PROCESSORS',     status:'idle', recordsIn:0, recordsOut:0, durationMs:0 },
  { name:'sink',       label:'5. SINK',            status:'idle', recordsIn:0, recordsOut:0, durationMs:0 },
  { name:'checkpoint', label:'6. CHECKPOINT',     status:'idle', recordsIn:0, recordsOut:0, durationMs:0 },
]

export const usePipelineStore = create<Store>((set) => ({
  status:'idle', currentRunId:null, stages:DEFAULT_STAGES, runs:[], metrics:null, streamMetrics:[],
  setStatus: (status) => set({ status }),
  setStageStatus: (name, u) => set(s => ({ stages: s.stages.map(st => st.name===name ? {...st,...u} : st) })),
  addRun: (r) => set(s => ({ runs: [r, ...s.runs].slice(0,50) })),
  updateRun: (id, u) => set(s => ({ runs: s.runs.map(r => r.id===id ? {...r,...u} : r) })),
  setMetrics: (metrics) => set({ metrics }),
  addStreamMetric: (m) => set(s => ({ streamMetrics: [...s.streamMetrics, m].slice(-60) })),
  resetStages: () => set({ stages: DEFAULT_STAGES }),
}))
