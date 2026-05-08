
import React, { useEffect, useState } from 'react'
import MetricCard from '../components/common/MetricCard'
import SectionHeader from '../components/common/SectionHeader'
import SensorFleet from '../components/dashboard/SensorFleet'
import ThroughputChart from '../components/monitoring/ThroughputChart'
import { MOCK_METRICS } from '../utils/api'

const DOWNSTREAM = [
  { name:'ML Training',       sub:'PyTorch/TF reads HUDI',           color:'var(--accent-cyan)',   icon:'⬡' },
  { name:'Perception Model',  sub:'Object detection inference',      color:'var(--accent-green)',  icon:'◎' },
  { name:'Simulation',        sub:'Replay sensor data in sim',       color:'var(--accent-purple)', icon:'▷' },
  { name:'Data Quality',      sub:'Dashboards & alerts Grafana+Trino',color:'var(--accent-amber)',  icon:'✓' },
  { name:'Map Building',      sub:'HD map gen from point clouds',    color:'var(--accent-orange)', icon:'⊞' },
  { name:'Fleet Analytics',   sub:'Vehicle health driving patterns', color:'var(--accent-red)',    icon:'⬟' },
]

const PATTERNS = [
  { name:'Template Method', desc:'PipelineDriver → Batch/Stream',       color:'var(--accent-cyan)' },
  { name:'Factory',         desc:'4 Factories: Proc, Source, Sink, UDF', color:'var(--accent-green)' },
  { name:'Strategy',        desc:'IProcessor, IValidator, ISinkWriter',  color:'var(--accent-amber)' },
  { name:'Observer',        desc:'QueryLifecycleMonitor',               color:'var(--accent-purple)' },
]

export default function Dashboard() {
  const m = MOCK_METRICS
  return (
    <div style={{ animation:'slide-up 0.3s ease' }}>
      <div style={{ marginBottom:'24px' }}>
        <h1 style={{ fontFamily:'var(--font-mono)', fontSize:'22px', fontWeight:700, color:'var(--text-primary)', letterSpacing:'-0.5px', marginBottom:'6px' }}>
          Autonomous Vehicle Sensor Pipeline
        </h1>
        <div style={{ display:'flex', gap:'16px', flexWrap:'wrap' }}>
          {['Batch + Streaming','500M+ records/day','Exactly-Once','Self-Healing','Config-Driven'].map(t => (
            <span key={t} style={{ fontFamily:'var(--font-mono)', fontSize:'11px', color:'var(--accent-cyan)', background:'rgba(0,212,255,0.08)', padding:'3px 10px', borderRadius:'20px', border:'1px solid rgba(0,212,255,0.2)' }}>{t}</span>
          ))}
        </div>
      </div>

      {/* KPIs */}
      <div style={{ display:'grid', gridTemplateColumns:'repeat(4, 1fr)', gap:'16px', marginBottom:'24px' }}>
        <MetricCard title="Total Runs"          value={m.totalRuns.toLocaleString()}                  sub={`${m.successfulRuns} successful`}              color="var(--accent-cyan)"   glow />
        <MetricCard title="Records Processed"   value={`${(m.totalRecordsProcessed/1e6).toFixed(0)}M`} sub="Total across all runs"                         color="var(--accent-green)"  glow />
        <MetricCard title="Avg Run Duration"    value={`${m.avgRunDurationS.toFixed(1)}s`}             sub="Batch pipeline wall time"                      color="var(--accent-amber)" />
        <MetricCard title="Rejection Rate"      value={`${(m.totalRecordsRejected/m.totalRecordsProcessed*100).toFixed(2)}%`} sub="Sent to Dead Letter Queue" color="var(--accent-red)" />
      </div>

      {/* Sensor Fleet + Throughput */}
      <div style={{ display:'grid', gridTemplateColumns:'1fr 1.5fr', gap:'16px', marginBottom:'24px' }}>
        <SensorFleet />
        <ThroughputChart />
      </div>

      {/* Downstream Consumers */}
      <div style={{ marginBottom:'24px' }}>
        <SectionHeader title="Downstream Consumers" subtitle="Systems consuming processed sensor data" />
        <div style={{ display:'grid', gridTemplateColumns:'repeat(6, 1fr)', gap:'10px' }}>
          {DOWNSTREAM.map(d => (
            <div key={d.name} style={{
              background:'var(--bg-card)', border:`1px solid ${d.color}33`, borderRadius:'10px',
              padding:'14px', textAlign:'center', cursor:'default', transition:'all 0.2s'
            }}
            onMouseEnter={e => { e.currentTarget.style.borderColor=d.color; e.currentTarget.style.transform='translateY(-2px)' }}
            onMouseLeave={e => { e.currentTarget.style.borderColor=`${d.color}33`; e.currentTarget.style.transform='translateY(0)' }}>
              <div style={{ fontSize:'22px', color:d.color, marginBottom:'6px' }}>{d.icon}</div>
              <div style={{ fontFamily:'var(--font-mono)', fontSize:'11px', fontWeight:700, color:d.color, marginBottom:'4px' }}>{d.name}</div>
              <div style={{ fontSize:'10px', color:'var(--text-muted)', lineHeight:1.4 }}>{d.sub}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Design Patterns */}
      <div>
        <SectionHeader title="Design Patterns Applied" />
        <div style={{ display:'grid', gridTemplateColumns:'repeat(4, 1fr)', gap:'10px' }}>
          {PATTERNS.map(p => (
            <div key={p.name} style={{ background:'var(--bg-card)', border:`1px solid ${p.color}33`, borderRadius:'10px', padding:'16px' }}>
              <div style={{ fontFamily:'var(--font-mono)', fontSize:'13px', fontWeight:700, color:p.color, marginBottom:'6px' }}>{p.name}</div>
              <div style={{ fontSize:'12px', color:'var(--text-secondary)', lineHeight:1.5 }}>{p.desc}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
