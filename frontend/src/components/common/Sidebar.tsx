import React from 'react'
import { Page } from '../../App'
const NAV = [
  { id:'dashboard',  icon:'⬡', label:'Dashboard' },
  { id:'pipeline',   icon:'⟶', label:'Pipeline' },
  { id:'monitoring', icon:'◎', label:'Monitoring' },
  { id:'config',     icon:'⚙', label:'Config' },
  { id:'catalog',    icon:'⊞', label:'Catalog' },
]
interface Props { activePage: Page; onNavigate: (p: Page) => void }
export default function Sidebar({ activePage, onNavigate }: Props) {
  return (
    <aside style={{ width:'220px', minWidth:'220px', background:'var(--bg-secondary)', borderRight:'1px solid var(--border)', display:'flex', flexDirection:'column', height:'100vh' }}>
      <div style={{ padding:'20px 16px 16px', borderBottom:'1px solid var(--border)' }}>
        <div style={{ fontFamily:'var(--font-mono)', fontSize:'11px', color:'var(--accent-cyan)', letterSpacing:'2px', marginBottom:'4px' }}>AV PIPELINE</div>
        <div style={{ fontSize:'13px', fontWeight:600, color:'var(--text-primary)' }}>Sensor Data Platform</div>
        <div style={{ display:'flex', gap:'6px', marginTop:'8px', alignItems:'center' }}>
          <span style={{ width:'6px', height:'6px', borderRadius:'50%', background:'var(--accent-green)', animation:'pulse-glow 2s infinite' }}/>
          <span style={{ fontSize:'11px', color:'var(--accent-green)', fontFamily:'var(--font-mono)' }}>LIVE</span>
        </div>
      </div>
      <nav style={{ flex:1, padding:'12px 8px' }}>
        {NAV.map(item => {
          const active = activePage === item.id
          return (
            <button key={item.id} onClick={() => onNavigate(item.id as Page)} style={{
              display:'flex', alignItems:'center', gap:'10px', width:'100%', padding:'10px 12px', margin:'2px 0',
              border:'none', cursor:'pointer', borderRadius:'8px', textAlign:'left', transition:'all 0.15s',
              background: active ? 'rgba(0,212,255,0.1)' : 'transparent',
              borderLeft: active ? '2px solid var(--accent-cyan)' : '2px solid transparent',
              color: active ? 'var(--accent-cyan)' : 'var(--text-secondary)',
              fontFamily:'var(--font-sans)', fontSize:'13px', fontWeight: active ? 600 : 400,
            }}>
              <span style={{ fontSize:'16px', lineHeight:1 }}>{item.icon}</span>
              {item.label}
            </button>
          )
        })}
      </nav>
      <div style={{ padding:'12px 16px', borderTop:'1px solid var(--border)', fontSize:'11px', color:'var(--text-muted)', fontFamily:'var(--font-mono)' }}>
        <div>500M+ records/day</div>
        <div style={{ marginTop:'2px' }}>Exactly-Once | Self-Healing</div>
        <div style={{ marginTop:'4px' }}>v2.0.0</div>
      </div>
    </aside>
  )
}