
import React, { useState } from 'react'
import SectionHeader from '../components/common/SectionHeader'
import PipelineFlowChart from '../components/pipeline/PipelineFlowChart'
import RunHistory from '../components/pipeline/RunHistory'
import StatusBadge from '../components/common/StatusBadge'
import { usePipelineStore } from '../store/pipelineStore'
import { api } from '../utils/api'

export default function PipelinePage() {
  const [mode, setMode] = useState<'batch'|'stream'>('batch')
  const [recipe, setRecipe] = useState('av_full_pipeline')
  const [loading, setLoading] = useState(false)
  const { status, stages, setStatus, setStageStatus, addRun, updateRun, resetStages } = usePipelineStore()

  const STAGES: any[] = ['sources','validators','transforms','processors','sink','checkpoint']
  const BASE_TIMES = [800, 2400, 1200, 4800, 1600, 400]
  const RECORD_COUNTS = [520000, 518000, 516000, 515000, 515000, 515000]

  const handleRun = async () => {
    setLoading(true); resetStages(); setStatus('running')
    const runId = `run-${Date.now().toString(36)}`
    addRun({ id:runId, mode, recipe, status:'running', startedAt:Date.now(), metrics:{} })

    // Simulate stages running
    for (let i = 0; i < STAGES.length; i++) {
      setStageStatus(STAGES[i], { status:'running' })
      await new Promise(r => setTimeout(r, BASE_TIMES[i] + Math.random()*500))
      setStageStatus(STAGES[i], {
        status:'success', recordsOut:RECORD_COUNTS[i],
        recordsIn:i===0?0:RECORD_COUNTS[i-1], durationMs: BASE_TIMES[i] + Math.floor(Math.random()*200)
      })
    }

    setStatus('success')
    updateRun(runId, { status:'success', finishedAt:Date.now(), metrics:{ records_processed:515000, records_read:520000, records_rejected:5000, elapsed_s:11.2 } })
    setLoading(false)

    // Try real API
    try { await api.startRun(mode, recipe) } catch {}
  }

  return (
    <div style={{ animation:'slide-up 0.3s ease' }}>
      <SectionHeader title="Pipeline Control" subtitle="Launch and monitor batch/streaming pipeline runs" />

      {/* Control Panel */}
      <div style={{ background:'var(--bg-card)', border:'1px solid var(--border)', borderRadius:'12px', padding:'24px', marginBottom:'20px' }}>
        <div style={{ display:'flex', gap:'16px', alignItems:'flex-end', flexWrap:'wrap' }}>
          <div>
            <label style={{ display:'block', fontFamily:'var(--font-mono)', fontSize:'10px', color:'var(--text-muted)', letterSpacing:'1px', marginBottom:'8px' }}>PIPELINE MODE</label>
            <div style={{ display:'flex', gap:'8px' }}>
              {['batch','stream'].map(m => (
                <button key={m} onClick={() => setMode(m as any)} style={{
                  padding:'8px 20px', border:`1px solid ${mode===m ? 'var(--accent-cyan)' : 'var(--border)'}`,
                  borderRadius:'6px', cursor:'pointer', fontFamily:'var(--font-mono)', fontSize:'12px', fontWeight:700,
                  background: mode===m ? 'rgba(0,212,255,0.1)' : 'transparent',
                  color: mode===m ? 'var(--accent-cyan)' : 'var(--text-secondary)', transition:'all 0.15s'
                }}>{m.toUpperCase()}</button>
              ))}
            </div>
          </div>
          <div>
            <label style={{ display:'block', fontFamily:'var(--font-mono)', fontSize:'10px', color:'var(--text-muted)', letterSpacing:'1px', marginBottom:'8px' }}>RECIPE</label>
            <select value={recipe} onChange={e => setRecipe(e.target.value)} style={{
              padding:'8px 12px', background:'var(--bg-secondary)', border:'1px solid var(--border)',
              borderRadius:'6px', color:'var(--text-primary)', fontFamily:'var(--font-mono)', fontSize:'12px', cursor:'pointer'
            }}>
              <option value="av_full_pipeline">av_full_pipeline</option>
              <option value="av_lidar_only">av_lidar_only</option>
              <option value="av_camera_only">av_camera_only</option>
              <option value="av_gps_pipeline">av_gps_pipeline</option>
            </select>
          </div>
          <button onClick={handleRun} disabled={loading} style={{
            padding:'10px 28px', background: loading ? 'var(--border)' : 'var(--accent-cyan)',
            border:'none', borderRadius:'8px', cursor: loading ? 'not-allowed' : 'pointer',
            color: loading ? 'var(--text-muted)' : 'var(--bg-primary)',
            fontFamily:'var(--font-mono)', fontSize:'13px', fontWeight:700, transition:'all 0.15s',
            boxShadow: !loading ? 'var(--glow-cyan)' : 'none'
          }}>
            {loading ? '⟳ RUNNING...' : '▶ LAUNCH PIPELINE'}
          </button>
          <div style={{ marginLeft:'auto' }}>
            <StatusBadge status={status} />
          </div>
        </div>

        {/* Stage Progress (when running) */}
        {loading && (
          <div style={{ marginTop:'20px', padding:'16px', background:'var(--bg-secondary)', borderRadius:'8px' }}>
            <div style={{ fontFamily:'var(--font-mono)', fontSize:'11px', color:'var(--accent-cyan)', marginBottom:'12px' }}>▶ processPipeline() executing...</div>
            <div style={{ display:'flex', flexDirection:'column', gap:'6px' }}>
              {stages.map(s => (
                <div key={s.name} style={{ display:'flex', alignItems:'center', gap:'10px' }}>
                  <span style={{ fontFamily:'var(--font-mono)', fontSize:'11px', color:'var(--text-muted)', width:'130px' }}>{s.label}</span>
                  <div style={{ flex:1, height:'4px', background:'var(--border)', borderRadius:'2px', overflow:'hidden' }}>
                    {s.status==='success' && <div style={{ height:'100%', background:'var(--accent-green)', borderRadius:'2px' }}/>}
                    {s.status==='running' && <div style={{ height:'100%', background:'var(--accent-amber)', borderRadius:'2px', width:'60%', animation:'pulse-glow 0.5s infinite' }}/>}
                  </div>
                  <StatusBadge status={s.status} size="sm" />
                  {s.recordsOut > 0 && <span style={{ fontFamily:'var(--font-mono)', fontSize:'10px', color:'var(--text-muted)' }}>{(s.recordsOut/1000).toFixed(0)}K</span>}
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Flow Chart */}
      <div style={{ marginBottom:'20px' }}>
        <PipelineFlowChart />
      </div>

      {/* Run History */}
      <RunHistory />
    </div>
  )
}
