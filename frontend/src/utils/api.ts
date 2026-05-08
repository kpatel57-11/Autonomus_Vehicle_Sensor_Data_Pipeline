const BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000'

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, {
    headers: { 'Content-Type': 'application/json' }, ...options
  })
  if (!res.ok) throw new Error(`API ${path} failed: ${res.status}`)
  return res.json()
}

export const api = {
  getStatus:     () => request<any>('/api/status'),
  getMetrics:    () => request<any>('/api/metrics'),
  getRuns:       (limit=20) => request<any>(`/api/pipeline/runs?limit=${limit}`),
  getRun:        (id: string) => request<any>(`/api/pipeline/runs/${id}`),
  startRun:      (mode: string, recipe='av_full_pipeline') =>
    request<any>('/api/pipeline/run', { method:'POST', body: JSON.stringify({mode, recipe_name:recipe}) }),
  cancelRun:     (id: string) => request<any>(`/api/pipeline/runs/${id}`, { method:'DELETE' }),
  getRecipe:     () => request<any>('/api/config/recipe'),
  saveRecipe:    (recipe: any) => request<any>('/api/config/recipe', { method:'POST', body: JSON.stringify(recipe) }),
  getSources:    () => request<any>('/api/config/sources'),
  getValidators: () => request<any>('/api/config/validators'),
  getProcessors: () => request<any>('/api/config/processors'),
  getSinks:      () => request<any>('/api/config/sinks'),
  getCatalog:    () => request<any>('/api/catalog'),
  getCheckpoints:() => request<any>('/api/checkpoints'),
  resetCheckpoint:(source: string) => request<any>(`/api/checkpoints/${source}`, { method:'DELETE' }),
  health:        () => request<any>('/health'),
}

// Mock data for demo when API is unavailable
export const MOCK_METRICS = {
  totalRuns: 847, successfulRuns: 831, failedRuns: 16,
  totalRecordsProcessed: 423500000, totalRecordsRejected: 847000,
  avgRunDurationS: 142.3, throughputRecordsPerRun: 500000,
}
export const MOCK_RUNS = Array.from({length:10}, (_,i) => ({
  id: `run-${String(i+1).padStart(3,'0')}`,
  mode: i%3===0 ? 'stream' : 'batch',
  recipe: 'av_full_pipeline',
  status: i===0 ? 'running' : i%8===0 ? 'error' : 'success',
  startedAt: Date.now() - (i * 7200000),
  finishedAt: i===0 ? undefined : Date.now() - (i * 7200000) + 142000,
  metrics: { records_read: 520000, records_valid: 518000, records_processed: 517000, elapsed_s: 142 }
}))
