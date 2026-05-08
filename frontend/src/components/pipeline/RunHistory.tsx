
import React from 'react'
import { usePipelineStore } from '../../store/pipelineStore'
import StatusBadge from '../common/StatusBadge'
import { MOCK_RUNS } from '../../utils/api'

function fmt(ms: number) {
  if (!ms) return '-'
  const s = Math.floor(ms/1000)
  return s > 60 ? `${Math.floor(s/60)}m ${s%60}s` : `${s}s`
}
function fmtDate(ts: number) {
  return new Date(ts).toLocaleString('en-US',{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'})
}

export default function RunHistory() {
  const { runs } = usePipelineStore()
  const data = runs.length > 0 ? runs : MOCK_RUNS as any[]
  return (
    <div style={{ background:'var(--bg-card)', border:'1px solid var(--border)', borderRadius:'12px', overflow:'hidden' }}>
      <div style={{ padding:'16px 20px', borderBottom:'1px solid var(--border)', display:'flex', justifyContent:'space-between', alignItems:'center' }}>
        <span style={{ fontFamily:'var(--font-mono)', fontSize:'12px', color:'var(--text-secondary)', letterSpacing:'1px' }}>RECENT RUNS</span>
        <span style={{ fontFamily:'var(--font-mono)', fontSize:'11px', color:'var(--text-muted)' }}>{data.length} total</span>
      </div>
      <table style={{ width:'100%', borderCollapse:'collapse' }}>
        <thead>
          <tr style={{ borderBottom:'1px solid var(--border)' }}>
            {['RUN ID','MODE','RECIPE','STATUS','RECORDS','DURATION','STARTED'].map(h => (
              <th key={h} style={{ padding:'10px 16px', textAlign:'left', fontFamily:'var(--font-mono)', fontSize:'10px', color:'var(--text-muted)', letterSpacing:'1px', fontWeight:400 }}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.slice(0,10).map((run, i) => (
            <tr key={run.id} style={{ borderBottom:'1px solid var(--border)22', transition:'background 0.15s' }}
              onMouseEnter={e => (e.currentTarget.style.background='var(--bg-elevated)')}
              onMouseLeave={e => (e.currentTarget.style.background='transparent')}>
              <td style={{ padding:'12px 16px', fontFamily:'var(--font-mono)', fontSize:'12px', color:'var(--accent-cyan)' }}>{run.id}</td>
              <td style={{ padding:'12px 16px' }}>
                <span style={{ fontFamily:'var(--font-mono)', fontSize:'11px', color: run.mode==='stream' ? 'var(--accent-purple)' : 'var(--accent-cyan)', background: run.mode==='stream' ? 'rgba(168,85,247,0.1)' : 'rgba(0,212,255,0.1)', padding:'2px 8px', borderRadius:'4px' }}>{run.mode.toUpperCase()}</span>
              </td>
              <td style={{ padding:'12px 16px', fontSize:'12px', color:'var(--text-secondary)' }}>{run.recipe}</td>
              <td style={{ padding:'12px 16px' }}><StatusBadge status={run.status} size="sm" /></td>
              <td style={{ padding:'12px 16px', fontFamily:'var(--font-mono)', fontSize:'12px', color:'var(--text-secondary)' }}>
                {run.metrics?.records_processed ? `${(run.metrics.records_processed/1000).toFixed(0)}K` : '-'}
              </td>
              <td style={{ padding:'12px 16px', fontFamily:'var(--font-mono)', fontSize:'12px', color:'var(--text-secondary)' }}>
                {run.finishedAt ? fmt(run.finishedAt - run.startedAt) : (run.status==='running' ? '⟳ Running' : '-')}
              </td>
              <td style={{ padding:'12px 16px', fontSize:'12px', color:'var(--text-muted)' }}>{fmtDate(run.startedAt)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
