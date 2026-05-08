
import React from 'react'
interface Props { title: string; value: string; sub?: string; color?: string; glow?: boolean }
export default function MetricCard({ title, value, sub, color='var(--accent-cyan)', glow=false }: Props) {
  return (
    <div style={{
      background:'var(--bg-card)', border:'1px solid var(--border)', borderRadius:'12px',
      padding:'20px', transition:'all 0.2s',
      boxShadow: glow ? `0 0 24px ${color}22` : 'none',
    }}>
      <div style={{ fontSize:'11px', color:'var(--text-muted)', fontFamily:'var(--font-mono)', letterSpacing:'1px', marginBottom:'8px', textTransform:'uppercase' }}>{title}</div>
      <div style={{ fontSize:'28px', fontWeight:700, color, fontFamily:'var(--font-mono)', lineHeight:1 }}>{value}</div>
      {sub && <div style={{ fontSize:'12px', color:'var(--text-secondary)', marginTop:'6px' }}>{sub}</div>}
    </div>
  )
}
