
import React from 'react'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Cell, ResponsiveContainer } from 'recharts'

const DATA = [
  { name:'GPS Bounds',       passed:99.2, rejected:0.8 },
  { name:'Timestamp',        passed:99.8, rejected:0.2 },
  { name:'LIDAR Intensity',  passed:97.4, rejected:2.6 },
  { name:'IMU Drift',        passed:98.9, rejected:1.1 },
  { name:'Camera Exposure',  passed:99.5, rejected:0.5 },
  { name:'Radar Freq',       passed:99.9, rejected:0.1 },
  { name:'Speed Check',      passed:99.6, rejected:0.4 },
  { name:'Point Density',    passed:96.8, rejected:3.2 },
  { name:'CAN Bus',          passed:99.7, rejected:0.3 },
  { name:'Heading',          passed:99.4, rejected:0.6 },
]

export default function ValidatorStats() {
  return (
    <div style={{ background:'var(--bg-card)', border:'1px solid var(--border)', borderRadius:'12px', padding:'20px' }}>
      <div style={{ fontFamily:'var(--font-mono)', fontSize:'11px', color:'var(--text-muted)', letterSpacing:'1px', marginBottom:'16px' }}>VALIDATOR PASS RATE — 20+ IMPLEMENTATIONS</div>
      <ResponsiveContainer width="100%" height={240}>
        <BarChart data={DATA} layout="vertical">
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" strokeOpacity={0.4} horizontal={false}/>
          <XAxis type="number" domain={[90,100]} stroke="var(--text-muted)" tick={{fill:'var(--text-muted)',fontSize:10,fontFamily:'var(--font-mono)'}} tickFormatter={v => `${v}%`}/>
          <YAxis type="category" dataKey="name" width={110} stroke="var(--text-muted)" tick={{fill:'var(--text-muted)',fontSize:10,fontFamily:'var(--font-mono)'}}/>
          <Tooltip contentStyle={{background:'var(--bg-elevated)',border:'1px solid var(--border)',borderRadius:'8px',fontFamily:'var(--font-mono)',fontSize:11}} formatter={(v:any) => [`${v.toFixed(1)}%`,'Pass Rate']}/>
          <Bar dataKey="passed" radius={[0,4,4,0]}>
            {DATA.map((entry, i) => (
              <Cell key={i} fill={entry.passed > 99 ? 'var(--accent-green)' : entry.passed > 98 ? 'var(--accent-amber)' : 'var(--accent-red)'}/>
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
