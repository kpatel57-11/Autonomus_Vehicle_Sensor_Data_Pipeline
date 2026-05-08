
import React from 'react'
import { usePipelineStore } from '../../store/pipelineStore'
import StatusBadge from '../common/StatusBadge'

const STAGE_COLORS: Record<string, string> = {
  sources:'var(--accent-cyan)', validators:'var(--accent-amber)',
  transforms:'var(--accent-purple)', processors:'var(--accent-orange)',
  sink:'var(--accent-green)', checkpoint:'var(--accent-red)',
}

const STAGE_ICONS: Record<string, string> = {
  sources:'⬟', validators:'✓', transforms:'⟨SQL⟩', processors:'⚙', sink:'⬇', checkpoint:'◉',
}

export default function PipelineFlowChart() {
  const { stages, status } = usePipelineStore()
  return (
    <div style={{
      background:'var(--bg-card)', border:'1px solid var(--border)', borderRadius:'12px',
      padding:'24px', overflowX:'auto',
    }}>
      <div style={{ fontFamily:'var(--font-mono)', fontSize:'11px', color:'var(--text-muted)', marginBottom:'20px', letterSpacing:'1px' }}>
        PIPELINE CONTAINER CHAIN — processPipeline() iterates 6 stages
      </div>
      <div style={{ display:'flex', alignItems:'center', gap:'0', minWidth:'800px' }}>
        {stages.map((stage, i) => {
          const color = STAGE_COLORS[stage.name]
          const isRunning = stage.status === 'running'
          const isDone = stage.status === 'success'
          const isError = stage.status === 'error'
          return (
            <React.Fragment key={stage.name}>
              <div style={{
                flex:1, background:'var(--bg-secondary)', border:`1px solid ${color}44`,
                borderRadius:'10px', padding:'14px 12px', textAlign:'center',
                boxShadow: isRunning ? `0 0 16px ${color}44` : isDone ? `0 0 8px ${color}22` : 'none',
                transition:'all 0.3s', position:'relative',
                borderTop: isRunning || isDone ? `2px solid ${color}` : '2px solid transparent',
              }}>
                {isRunning && (
                  <div style={{
                    position:'absolute', top:'-1px', left:0, right:0, height:'2px',
                    background:`linear-gradient(90deg, transparent, ${color}, transparent)`,
                    animation:'flow 1.5s linear infinite', backgroundSize:'200% auto',
                  }}/>
                )}
                <div style={{ fontSize:'20px', marginBottom:'6px', opacity: stage.status==='idle' ? 0.4 : 1 }}>
                  {isRunning ? '⟳' : isDone ? '✓' : isError ? '✗' : STAGE_ICONS[stage.name]}
                </div>
                <div style={{ fontFamily:'var(--font-mono)', fontSize:'10px', color, fontWeight:700, marginBottom:'4px' }}>
                  {stage.label}
                </div>
                <div style={{ marginBottom:'6px' }}>
                  <StatusBadge status={stage.status} size="sm" />
                </div>
                {isDone && (
                  <div style={{ fontFamily:'var(--font-mono)', fontSize:'10px', color:'var(--text-muted)' }}>
                    {(stage.recordsOut/1000).toFixed(0)}K recs
                  </div>
                )}
                {stage.durationMs > 0 && (
                  <div style={{ fontFamily:'var(--font-mono)', fontSize:'9px', color:'var(--text-muted)', marginTop:'2px' }}>
                    {stage.durationMs}ms
                  </div>
                )}
              </div>
              {i < stages.length-1 && (
                <div style={{ padding:'0 4px', color: isDone ? color : 'var(--border)', fontSize:'18px', flexShrink:0, transition:'color 0.3s' }}>
                  →
                </div>
              )}
            </React.Fragment>
          )
        })}
      </div>
    </div>
  )
}
