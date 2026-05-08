
import React from 'react'
type Status = 'idle'|'running'|'success'|'error'
const MAP: Record<Status,{color:string;bg:string;label:string}> = {
  idle:    { color:'var(--text-muted)',    bg:'rgba(74,122,155,0.15)',   label:'IDLE' },
  running: { color:'var(--accent-amber)',  bg:'rgba(255,184,0,0.15)',    label:'RUNNING' },
  success: { color:'var(--accent-green)',  bg:'rgba(0,255,136,0.15)',    label:'SUCCESS' },
  error:   { color:'var(--accent-red)',    bg:'rgba(255,64,96,0.15)',    label:'ERROR' },
}
interface Props { status: Status; size?: 'sm'|'md' }
export default function StatusBadge({ status, size='md' }: Props) {
  const s = MAP[status] || MAP.idle
  return (
    <span style={{
      display:'inline-flex', alignItems:'center', gap:'5px',
      padding: size==='sm' ? '2px 8px' : '4px 10px',
      borderRadius:'20px', background:s.bg,
      color:s.color, fontFamily:'var(--font-mono)',
      fontSize: size==='sm' ? '10px' : '11px', fontWeight:700,
    }}>
      {status==='running' && <span style={{ width:'5px', height:'5px', borderRadius:'50%', background:s.color, animation:'pulse-glow 1s infinite' }}/>}
      {s.label}
    </span>
  )
}
