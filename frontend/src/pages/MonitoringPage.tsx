
import React, { useState, useEffect } from 'react'
import SectionHeader from '../components/common/SectionHeader'
import ThroughputChart from '../components/monitoring/ThroughputChart'
import ValidatorStats from '../components/monitoring/ValidatorStats'
import ProcessorMetrics from '../components/monitoring/ProcessorMetrics'
import MetricCard from '../components/common/MetricCard'

const OPS_ITEMS = [
  { name:'Airflow',              status:'healthy', detail:'DAG scheduling sensors → pipeline → DQ',        color:'var(--accent-green)' },
  { name:'Kubernetes/YARN',      status:'healthy', detail:'Resource management, dynamic allocation',       color:'var(--accent-green)' },
  { name:'Grafana + Prometheus',  status:'healthy', detail:'Pipeline metrics, latency dashboards',          color:'var(--accent-green)' },
  { name:'CI/CD',                status:'healthy', detail:'Maven → Docker → staging → prod',               color:'var(--accent-green)' },
  { name:'Dead Letter Queue',     status:'warning', detail:'847 bad records pending investigation',         color:'var(--accent-amber)' },
  { name:'Data Catalog',          status:'healthy', detail:'Schema versioning, lineage tracking',           color:'var(--accent-green)' },
  { name:'Spark on YARN/K8s',    status:'healthy', detail:'15-50 executors | 12GB each | Kryo Serializer', color:'var(--accent-green)' },
  { name:'Schema Registry',       status:'healthy', detail:'Avro/Protobuf schemas versioned',               color:'var(--accent-green)' },
]

export default function MonitoringPage() {
  const [dlqCount, setDlqCount] = useState(847)
  useEffect(() => { const t = setInterval(() => setDlqCount(c => c + Math.floor(Math.random()*3)), 5000); return () => clearInterval(t) }, [])
  return (
    <div style={{ animation:'slide-up 0.3s ease' }}>
      <SectionHeader title="Operations & Monitoring" subtitle="Grafana + Prometheus | Airflow | Kubernetes | DLQ | Data Catalog" />

      {/* KPIs */}
      <div style={{ display:'grid', gridTemplateColumns:'repeat(4, 1fr)', gap:'16px', marginBottom:'24px' }}>
        <MetricCard title="Spark Executors"    value="15–50"       sub="Dynamic allocation on YARN/K8s"  color="var(--accent-cyan)"   glow />
        <MetricCard title="Executor Memory"    value="12GB each"   sub="Kryo Serializer enabled"         color="var(--accent-green)" />
        <MetricCard title="DLQ Records"        value={dlqCount.toLocaleString()} sub="Bad records pending investigation" color="var(--accent-amber)" glow />
        <MetricCard title="Pipeline Uptime"    value="99.97%"      sub="Last 30 days SLA"                color="var(--accent-green)"  glow />
      </div>

      {/* Charts */}
      <div style={{ display:'grid', gridTemplateColumns:'1.6fr 1fr', gap:'16px', marginBottom:'24px' }}>
        <ThroughputChart />
        <ProcessorMetrics />
      </div>

      <div style={{ marginBottom:'24px' }}>
        <ValidatorStats />
      </div>

      {/* Ops Services */}
      <SectionHeader title="Operations & Scheduling" />
      <div style={{ display:'grid', gridTemplateColumns:'repeat(4, 1fr)', gap:'10px' }}>
        {OPS_ITEMS.map(op => (
          <div key={op.name} style={{ background:'var(--bg-card)', border:`1px solid ${op.color}33`, borderRadius:'10px', padding:'14px' }}>
            <div style={{ display:'flex', justifyContent:'space-between', marginBottom:'6px' }}>
              <span style={{ fontFamily:'var(--font-mono)', fontSize:'12px', fontWeight:700, color:op.color }}>{op.name}</span>
              <span style={{ width:'7px', height:'7px', borderRadius:'50%', background:op.color, animation:'pulse-glow 2s infinite' }}/>
            </div>
            <div style={{ fontSize:'11px', color:'var(--text-muted)', lineHeight:1.5 }}>{op.detail}</div>
          </div>
        ))}
      </div>

      {/* Self-Healing */}
      <div style={{ marginTop:'20px', background:'var(--bg-card)', border:'1px solid var(--border)', borderRadius:'12px', padding:'20px' }}>
        <div style={{ fontFamily:'var(--font-mono)', fontSize:'11px', color:'var(--text-muted)', letterSpacing:'1px', marginBottom:'14px' }}>SELF-HEALING — QueryLifecycleMonitor (Observer Pattern)</div>
        <div style={{ display:'grid', gridTemplateColumns:'repeat(3, 1fr)', gap:'12px' }}>
          {[
            { event:'onQueryStarted',    desc:'log → metrics',                   color:'var(--accent-cyan)' },
            { event:'onQueryProgress',   desc:'metrics + alert if lag > threshold', color:'var(--accent-amber)' },
            { event:'onQueryTerminated', desc:'alert → auto-restart (30 attempts)', color:'var(--accent-red)' },
          ].map(h => (
            <div key={h.event} style={{ background:'var(--bg-secondary)', borderRadius:'8px', padding:'12px' }}>
              <div style={{ fontFamily:'var(--font-mono)', fontSize:'11px', color:h.color, marginBottom:'4px' }}>{h.event}</div>
              <div style={{ fontSize:'12px', color:'var(--text-secondary)' }}>{h.desc}</div>
            </div>
          ))}
        </div>
        <div style={{ marginTop:'12px', display:'flex', gap:'8px', flexWrap:'wrap' }}>
          {['Publish to Message Queue','Watchdog auto-restart (30s)','PagerDuty / Slack alerts','Dead letter queue for bad data','Spark checkpoints + Kafka offset tracking'].map(a => (
            <span key={a} style={{ fontFamily:'var(--font-mono)', fontSize:'10px', color:'var(--accent-green)', background:'rgba(0,255,136,0.08)', padding:'3px 8px', borderRadius:'4px' }}>→ {a}</span>
          ))}
        </div>
      </div>
    </div>
  )
}
