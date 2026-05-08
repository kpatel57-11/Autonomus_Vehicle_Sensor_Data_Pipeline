
import React, { ReactNode } from 'react'
interface Props { title: string; subtitle?: string; action?: ReactNode }
export default function SectionHeader({ title, subtitle, action }: Props) {
  return (
    <div style={{ display:'flex', justifyContent:'space-between', alignItems:'flex-start', marginBottom:'20px' }}>
      <div>
        <h2 style={{ fontSize:'18px', fontWeight:700, color:'var(--text-primary)', fontFamily:'var(--font-mono)', letterSpacing:'-0.5px' }}>{title}</h2>
        {subtitle && <p style={{ fontSize:'13px', color:'var(--text-secondary)', marginTop:'4px' }}>{subtitle}</p>}
      </div>
      {action && <div>{action}</div>}
    </div>
  )
}
