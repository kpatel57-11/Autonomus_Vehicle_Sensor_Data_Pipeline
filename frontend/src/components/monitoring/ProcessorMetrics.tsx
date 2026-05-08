
import React from 'react'
import { RadarChart, PolarGrid, PolarAngleAxis, Radar, ResponsiveContainer, Tooltip } from 'recharts'

const DATA = [
  { subject:'PointCloud', score: 92 }, { subject:'FrameAlign', score: 88 },
  { subject:'SensorFusion', score: 95 }, { subject:'Anomaly', score: 78 },
  { subject:'Trajectory', score: 85 }, { subject:'OccupancyGrid', score: 91 },
  { subject:'Velocity', score: 89 }, { subject:'ObjDetect', score: 76 },
  { subject:'LaneDetect', score: 82 }, { subject:'Weather', score: 94 },
  { subject:'HDMap', score: 87 }, { subject:'Motion', score: 80 },
]

export default function ProcessorMetrics() {
  return (
    <div style={{ background:'var(--bg-card)', border:'1px solid var(--border)', borderRadius:'12px', padding:'20px' }}>
      <div style={{ fontFamily:'var(--font-mono)', fontSize:'11px', color:'var(--text-muted)', letterSpacing:'1px', marginBottom:'4px' }}>PROCESSOR PERFORMANCE — 12+ IProcessor</div>
      <div style={{ fontSize:'11px', color:'var(--text-muted)', marginBottom:'12px' }}>Quality score per processor (last 24h)</div>
      <ResponsiveContainer width="100%" height={220}>
        <RadarChart data={DATA}>
          <PolarGrid stroke="var(--border)"/>
          <PolarAngleAxis dataKey="subject" tick={{fill:'var(--text-muted)',fontSize:9,fontFamily:'var(--font-mono)'}}/>
          <Radar name="Score" dataKey="score" stroke="var(--accent-cyan)" fill="var(--accent-cyan)" fillOpacity={0.15} strokeWidth={2}/>
          <Tooltip contentStyle={{background:'var(--bg-elevated)',border:'1px solid var(--border)',borderRadius:'8px',fontFamily:'var(--font-mono)',fontSize:11}} formatter={(v:any) => [`${v}%`,'Score']}/>
        </RadarChart>
      </ResponsiveContainer>
    </div>
  )
}
