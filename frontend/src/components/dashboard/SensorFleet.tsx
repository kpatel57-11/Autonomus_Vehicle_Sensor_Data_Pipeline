
import React, { useState, useEffect } from 'react'

const SENSORS = [
  { name:'LIDAR', icon:'⬟', detail:'3D point clouds ~300K pts/frame', rate:300000, unit:'pts/s', color:'var(--accent-cyan)' },
  { name:'Camera', icon:'⬛', detail:'8 surround cams 30 fps each', rate:240, unit:'fps', color:'var(--accent-amber)' },
  { name:'GPS/IMU', icon:'◎', detail:'Position + motion 100 Hz', rate:100, unit:'Hz', color:'var(--accent-green)' },
  { name:'Radar', icon:'◌', detail:'Object detection 76-81 GHz', rate:77, unit:'GHz', color:'var(--accent-purple)' },
  { name:'CAN Bus', icon:'⇄', detail:'Speed, steering, brake, throttle', rate:500, unit:'msg/s', color:'var(--accent-orange)' },
  { name:'Ultrasonic', icon:'))', detail:'Proximity sensors 12 units', rate:12, unit:'sensors', color:'var(--accent-red)' },
]

export default function SensorFleet() {
  const [ticks, setTicks] = useState(0)
  useEffect(() => { const t = setInterval(() => setTicks(x => x+1), 1000); return () => clearInterval(t) }, [])
  return (
    <div style={{ background:'var(--bg-card)', border:'1px solid var(--border)', borderRadius:'12px', padding:'20px' }}>
      <div style={{ fontFamily:'var(--font-mono)', fontSize:'11px', color:'var(--text-muted)', letterSpacing:'1px', marginBottom:'16px' }}>VEHICLE FLEET — ONBOARD SENSORS</div>
      <div style={{ display:'grid', gridTemplateColumns:'repeat(3, 1fr)', gap:'10px' }}>
        {SENSORS.map(s => (
          <div key={s.name} style={{
            background:'var(--bg-secondary)', border:`1px solid ${s.color}33`,
            borderRadius:'8px', padding:'12px', cursor:'default', transition:'all 0.2s'
          }}
          onMouseEnter={e => e.currentTarget.style.borderColor = s.color}
          onMouseLeave={e => e.currentTarget.style.borderColor = `${s.color}33`}>
            <div style={{ display:'flex', justifyContent:'space-between', marginBottom:'6px' }}>
              <span style={{ fontSize:'18px', color:s.color }}>{s.icon}</span>
              <span style={{ width:'6px', height:'6px', borderRadius:'50%', background:s.color, alignSelf:'center', animation:'pulse-glow 1.5s infinite' }}/>
            </div>
            <div style={{ fontFamily:'var(--font-mono)', fontSize:'12px', fontWeight:700, color:s.color, marginBottom:'2px' }}>{s.name}</div>
            <div style={{ fontSize:'11px', color:'var(--text-muted)', marginBottom:'6px', lineHeight:1.4 }}>{s.detail}</div>
            <div style={{ fontFamily:'var(--font-mono)', fontSize:'13px', color:s.color, fontWeight:700 }}>
              {s.rate.toLocaleString()} <span style={{ fontSize:'10px', color:'var(--text-muted)', fontWeight:400 }}>{s.unit}</span>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
