
import React, { useState, useEffect } from 'react'
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'

function generatePoint(i: number) {
  const base = 500000
  return {
    time: new Date(Date.now() - (59-i)*30000).toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit',second:'2-digit'}),
    lidar: Math.floor(base * 0.4 + Math.random() * 50000),
    camera: Math.floor(base * 0.2 + Math.random() * 30000),
    gps: Math.floor(base * 0.1 + Math.random() * 10000),
    radar: Math.floor(base * 0.3 + Math.random() * 40000),
  }
}

export default function ThroughputChart() {
  const [data, setData] = useState(() => Array.from({length:30}, (_, i) => generatePoint(i+29)))
  useEffect(() => {
    const t = setInterval(() => {
      setData(prev => [...prev.slice(1), generatePoint(59)])
    }, 2000)
    return () => clearInterval(t)
  }, [])
  return (
    <div style={{ background:'var(--bg-card)', border:'1px solid var(--border)', borderRadius:'12px', padding:'20px' }}>
      <div style={{ fontFamily:'var(--font-mono)', fontSize:'11px', color:'var(--text-muted)', letterSpacing:'1px', marginBottom:'16px' }}>SENSOR THROUGHPUT — RECORDS/SEC (LIVE)</div>
      <ResponsiveContainer width="100%" height={220}>
        <AreaChart data={data}>
          <defs>
            {['cyan','amber','green','purple'].map((c,i) => (
              <linearGradient key={c} id={`grad-${c}`} x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor={`var(--accent-${c})`} stopOpacity={0.3}/>
                <stop offset="95%" stopColor={`var(--accent-${c})`} stopOpacity={0}/>
              </linearGradient>
            ))}
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" strokeOpacity={0.4}/>
          <XAxis dataKey="time" stroke="var(--text-muted)" tick={{fill:'var(--text-muted)',fontSize:10,fontFamily:'var(--font-mono)'}} tickFormatter={v => v.split(' ')[1] || v} interval={4}/>
          <YAxis stroke="var(--text-muted)" tick={{fill:'var(--text-muted)',fontSize:10,fontFamily:'var(--font-mono)'}} tickFormatter={v => `${(v/1000).toFixed(0)}K`}/>
          <Tooltip contentStyle={{background:'var(--bg-elevated)',border:'1px solid var(--border)',borderRadius:'8px',fontFamily:'var(--font-mono)',fontSize:11}} labelStyle={{color:'var(--text-secondary)'}} formatter={(v:any) => [`${(v/1000).toFixed(1)}K`,'']}/>
          <Area type="monotone" dataKey="lidar"  stroke="var(--accent-cyan)"   fill="url(#grad-cyan)"   strokeWidth={2} dot={false} name="LIDAR"/>
          <Area type="monotone" dataKey="radar"  stroke="var(--accent-purple)" fill="url(#grad-purple)" strokeWidth={2} dot={false} name="Radar"/>
          <Area type="monotone" dataKey="camera" stroke="var(--accent-amber)"  fill="url(#grad-amber)"  strokeWidth={2} dot={false} name="Camera"/>
          <Area type="monotone" dataKey="gps"    stroke="var(--accent-green)"  fill="url(#grad-green)"  strokeWidth={2} dot={false} name="GPS"/>
        </AreaChart>
      </ResponsiveContainer>
    </div>
  )
}
