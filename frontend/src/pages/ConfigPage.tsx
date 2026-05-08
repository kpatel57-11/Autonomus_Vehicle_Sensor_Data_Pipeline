
import React, { useState } from 'react'
import SectionHeader from '../components/common/SectionHeader'

const SOURCES = ['kafka_lidar','kafka_camera','kafka_gps','s3_radar','s3_lidar','ros_bag','jdbc_metadata']
const VALIDATORS = ['GPSBoundsCheck','TimestampMonotonicity','LIDARIntensityRange','IMUDriftDetector','CameraExposureValidator','RadarFrequencyValidator','SpeedPlausibilityCheck','PointCloudDensityValidator','CANBusMessageValidator','HeadingValidator']
const PROCESSORS = ['PointCloudStitcher','FrameAligner','SensorFusion','AnomalyDetector','TrajectoryInterpolator','OccupancyGridBuilder','VelocityEstimator','ObjectDetectionEnricher','LaneDetectionProcessor','WeatherConditionClassifier','HDMapMatcher','PredictiveMotionModel']
const SINKS = ['hudi_data_lake','delta_lake','parquet','api_publisher','rabbitmq_sink','hive']

interface SelectableListProps { items: string[]; selected: string[]; onToggle: (s: string) => void; color: string }
function SelectableList({ items, selected, onToggle, color }: SelectableListProps) {
  return (
    <div style={{ display:'flex', flexWrap:'wrap', gap:'6px' }}>
      {items.map(item => {
        const active = selected.includes(item)
        return (
          <button key={item} onClick={() => onToggle(item)} style={{
            padding:'4px 10px', border:`1px solid ${active ? color : 'var(--border)'}`,
            borderRadius:'4px', cursor:'pointer', fontFamily:'var(--font-mono)', fontSize:'11px',
            background: active ? `${color}18` : 'transparent',
            color: active ? color : 'var(--text-muted)', transition:'all 0.15s'
          }}>{item}</button>
        )
      })}
    </div>
  )
}

export default function ConfigPage() {
  const [selSources, setSelSources] = useState(['kafka_lidar','kafka_camera','kafka_gps','s3_radar'])
  const [selValidators, setSelValidators] = useState(['GPSBoundsCheck','TimestampMonotonicity','LIDARIntensityRange','IMUDriftDetector','CameraExposureValidator'])
  const [selProcessors, setSelProcessors] = useState(['PointCloudStitcher','FrameAligner','SensorFusion','AnomalyDetector','TrajectoryInterpolator','OccupancyGridBuilder'])
  const [selSinks, setSelSinks] = useState(['hudi_data_lake','delta_lake','api_publisher','rabbitmq_sink'])
  const [saved, setSaved] = useState(false)
  const [watermark, setWatermark] = useState('10 minutes')
  const [mode, setMode] = useState('batch')

  const toggle = (list: string[], setList: (l:string[])=>void, item: string) => {
    setList(list.includes(item) ? list.filter(x=>x!==item) : [...list, item])
    setSaved(false)
  }

  const handleSave = () => { setSaved(true); setTimeout(() => setSaved(false), 3000) }

  const recipe = { name:'av_full_pipeline', version:'2.0', mode, sources:selSources, validators:selValidators, processors:selProcessors, sinks:selSinks, transforms:[{sql:'coordinate_transform',params:{}},{sql:'temporal_alignment',params:{}},{sql:'filter_valid',params:{}}], extra:{watermark_delay:watermark} }

  return (
    <div style={{ animation:'slide-up 0.3s ease' }}>
      <SectionHeader title="Pipeline Configuration" subtitle="Configure recipe — loaded from MongoDB, metadata from PostgreSQL"
        action={
          <button onClick={handleSave} style={{
            padding:'8px 20px', background: saved ? 'var(--accent-green)' : 'var(--accent-cyan)',
            border:'none', borderRadius:'6px', cursor:'pointer', fontFamily:'var(--font-mono)', fontSize:'12px', fontWeight:700,
            color:'var(--bg-primary)', transition:'all 0.2s'
          }}>{saved ? '✓ SAVED' : 'SAVE RECIPE'}</button>
        }
      />

      <div style={{ display:'grid', gridTemplateColumns:'1.5fr 1fr', gap:'16px' }}>
        <div style={{ display:'flex', flexDirection:'column', gap:'16px' }}>
          {[
            { title:'1. SOURCES',       subtitle:'ISourceReader implementations — Kafka, S3/HDFS, ROSBag, JDBC', items:SOURCES, sel:selSources, set:setSelSources, color:'var(--accent-cyan)' },
            { title:'2. VALIDATORS',    subtitle:'20+ IValidator implementations', items:VALIDATORS, sel:selValidators, set:setSelValidators, color:'var(--accent-amber)' },
            { title:'4. PROCESSORS',    subtitle:'12+ IProcessor implementations — reflection-based dynamic loading', items:PROCESSORS, sel:selProcessors, set:setSelProcessors, color:'var(--accent-orange)' },
            { title:'5. SINKS',         subtitle:'SinkWriterFactory — at-least-once + idempotent = exactly-once', items:SINKS, sel:selSinks, set:setSelSinks, color:'var(--accent-green)' },
          ].map(s => (
            <div key={s.title} style={{ background:'var(--bg-card)', border:'1px solid var(--border)', borderRadius:'12px', padding:'18px' }}>
              <div style={{ fontFamily:'var(--font-mono)', fontSize:'12px', fontWeight:700, color:s.color, marginBottom:'4px' }}>{s.title}</div>
              <div style={{ fontSize:'11px', color:'var(--text-muted)', marginBottom:'12px' }}>{s.subtitle}</div>
              <SelectableList items={s.items} selected={s.sel} onToggle={item => toggle(s.sel, s.set, item)} color={s.color}/>
            </div>
          ))}
        </div>

        {/* Recipe Preview */}
        <div style={{ display:'flex', flexDirection:'column', gap:'16px' }}>
          <div style={{ background:'var(--bg-card)', border:'1px solid var(--border)', borderRadius:'12px', padding:'18px' }}>
            <div style={{ fontFamily:'var(--font-mono)', fontSize:'11px', color:'var(--text-muted)', letterSpacing:'1px', marginBottom:'12px' }}>PIPELINE SETTINGS</div>
            <div style={{ display:'flex', flexDirection:'column', gap:'12px' }}>
              <div>
                <div style={{ fontFamily:'var(--font-mono)', fontSize:'10px', color:'var(--text-muted)', marginBottom:'6px' }}>MODE</div>
                <div style={{ display:'flex', gap:'8px' }}>
                  {['batch','stream'].map(m => (
                    <button key={m} onClick={() => setMode(m)} style={{
                      padding:'6px 14px', border:`1px solid ${mode===m ? 'var(--accent-cyan)' : 'var(--border)'}`,
                      borderRadius:'4px', cursor:'pointer', fontFamily:'var(--font-mono)', fontSize:'11px', fontWeight:700,
                      background: mode===m ? 'rgba(0,212,255,0.1)' : 'transparent',
                      color: mode===m ? 'var(--accent-cyan)' : 'var(--text-secondary)'
                    }}>{m}</button>
                  ))}
                </div>
              </div>
              <div>
                <div style={{ fontFamily:'var(--font-mono)', fontSize:'10px', color:'var(--text-muted)', marginBottom:'6px' }}>WATERMARK DELAY</div>
                <select value={watermark} onChange={e => setWatermark(e.target.value)} style={{
                  padding:'6px 10px', background:'var(--bg-secondary)', border:'1px solid var(--border)',
                  borderRadius:'4px', color:'var(--text-primary)', fontFamily:'var(--font-mono)', fontSize:'11px', width:'100%'
                }}>
                  {['1 minute','5 minutes','10 minutes','30 minutes','1 hour'].map(w => <option key={w}>{w}</option>)}
                </select>
              </div>
            </div>
          </div>

          {/* JSON Preview */}
          <div style={{ background:'var(--bg-card)', border:'1px solid var(--border)', borderRadius:'12px', padding:'18px', flex:1 }}>
            <div style={{ fontFamily:'var(--font-mono)', fontSize:'11px', color:'var(--text-muted)', letterSpacing:'1px', marginBottom:'12px' }}>RECIPE JSON (MongoDB)</div>
            <pre style={{ fontFamily:'var(--font-mono)', fontSize:'10px', color:'var(--accent-green)', background:'var(--bg-secondary)', padding:'12px', borderRadius:'8px', overflow:'auto', maxHeight:'400px', lineHeight:1.6 }}>
              {JSON.stringify(recipe, null, 2)}
            </pre>
          </div>
        </div>
      </div>
    </div>
  )
}
