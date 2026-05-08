
import React, { useState } from 'react'
import SectionHeader from '../components/common/SectionHeader'

const DATASETS = [
  { name:'lidar_raw',        path:'s3://av-sensor-data/lidar/',     format:'Parquet',     size:'2.4 TB', records:'~300M',  schema:['vehicle_id:str','timestamp_ms:long','x_local:double','y_local:double','intensity:float','point_count:int'], partitions:['sensor_type','vehicle_id'], freshness:'15 min' },
  { name:'camera_meta',       path:'s3://av-sensor-data/camera/',    format:'Parquet',     size:'890 GB', records:'~80M',   schema:['vehicle_id:str','timestamp_ms:long','frame_id:int','exposure:float','detected_objects:str','detection_confidence:double'], partitions:['vehicle_id','date'], freshness:'5 min' },
  { name:'gps_stream',        path:'s3://av-sensor-data/gps/',       format:'Delta Lake',  size:'45 GB',  records:'~10M',   schema:['vehicle_id:str','timestamp_ms:long','lat:double','lon:double','altitude:float','speed:float','heading:float'], partitions:['vehicle_id'], freshness:'1 min' },
  { name:'radar_points',      path:'s3://av-sensor-data/radar/',     format:'ORC',         size:'320 GB', records:'~50M',   schema:['vehicle_id:str','timestamp_ms:long','frequency_ghz:float','range_m:float','velocity_ms:float'], partitions:['sensor_type'], freshness:'30 min' },
  { name:'fused_perception',   path:'s3://av-sensor-data/fused/',    format:'Hudi ACID',   size:'1.1 TB', records:'~200M',  schema:['vehicle_id:str','time_bucket:long','fusion_quality:double','fused:bool','lane_id:int','road_segment_id:long'], partitions:['time_bucket'], freshness:'30 min' },
  { name:'occupancy_grid',     path:'s3://av-sensor-data/occupancy/',format:'Delta Lake',  size:'670 GB', records:'~120M',  schema:['vehicle_id:str','time_bucket:long','grid_x:int','grid_y:int','point_density:long','occupied:bool'], partitions:['vehicle_id','time_bucket'], freshness:'60 min' },
]

const FORMAT_COLORS: Record<string, string> = {
  'Parquet':    'var(--accent-cyan)',
  'Delta Lake': 'var(--accent-purple)',
  'Hudi ACID':  'var(--accent-green)',
  'ORC':        'var(--accent-amber)',
}

export default function CatalogPage() {
  const [selected, setSelected] = useState<typeof DATASETS[0] | null>(null)
  const [search, setSearch] = useState('')
  const filtered = DATASETS.filter(d => d.name.includes(search) || d.format.toLowerCase().includes(search))
  return (
    <div style={{ animation:'slide-up 0.3s ease' }}>
      <SectionHeader title="Data Catalog" subtitle="Schema versioning and lineage tracking — PostgreSQL metadata store" />
      <div style={{ display:'grid', gridTemplateColumns:'1.2fr 1fr', gap:'16px' }}>
        <div>
          <input placeholder="Search datasets..." value={search} onChange={e => setSearch(e.target.value)} style={{
            width:'100%', padding:'10px 14px', marginBottom:'12px',
            background:'var(--bg-card)', border:'1px solid var(--border)', borderRadius:'8px',
            color:'var(--text-primary)', fontFamily:'var(--font-mono)', fontSize:'12px', outline:'none'
          }}/>
          <div style={{ display:'flex', flexDirection:'column', gap:'8px' }}>
            {filtered.map(ds => {
              const fc = FORMAT_COLORS[ds.format] || 'var(--accent-cyan)'
              const active = selected?.name === ds.name
              return (
                <div key={ds.name} onClick={() => setSelected(ds)} style={{
                  background: active ? 'var(--bg-elevated)' : 'var(--bg-card)',
                  border:`1px solid ${active ? fc : 'var(--border)'}`,
                  borderRadius:'10px', padding:'14px', cursor:'pointer', transition:'all 0.15s'
                }}>
                  <div style={{ display:'flex', justifyContent:'space-between', marginBottom:'6px' }}>
                    <span style={{ fontFamily:'var(--font-mono)', fontSize:'13px', fontWeight:700, color:fc }}>{ds.name}</span>
                    <span style={{ fontFamily:'var(--font-mono)', fontSize:'10px', color:fc, background:`${fc}18`, padding:'2px 8px', borderRadius:'4px' }}>{ds.format}</span>
                  </div>
                  <div style={{ display:'flex', gap:'16px', fontSize:'11px', color:'var(--text-muted)' }}>
                    <span>Size: {ds.size}</span>
                    <span>Records: {ds.records}</span>
                    <span>Freshness: {ds.freshness}</span>
                  </div>
                  <div style={{ fontSize:'11px', color:'var(--text-muted)', marginTop:'4px', fontFamily:'var(--font-mono)' }}>{ds.path}</div>
                </div>
              )
            })}
          </div>
        </div>

        {/* Schema Detail */}
        <div>
          {selected ? (
            <div style={{ background:'var(--bg-card)', border:'1px solid var(--border)', borderRadius:'12px', padding:'20px', position:'sticky', top:0 }}>
              <div style={{ fontFamily:'var(--font-mono)', fontSize:'14px', fontWeight:700, color:(FORMAT_COLORS[selected.format]||'var(--accent-cyan)'), marginBottom:'4px' }}>{selected.name}</div>
              <div style={{ fontSize:'12px', color:'var(--text-muted)', marginBottom:'16px' }}>{selected.path}</div>
              <div style={{ fontFamily:'var(--font-mono)', fontSize:'10px', color:'var(--text-muted)', letterSpacing:'1px', marginBottom:'8px' }}>SCHEMA</div>
              <div style={{ display:'flex', flexDirection:'column', gap:'4px', marginBottom:'16px' }}>
                {selected.schema.map(f => {
                  const [name, type] = f.split(':')
                  return (
                    <div key={f} style={{ display:'flex', justifyContent:'space-between', padding:'6px 10px', background:'var(--bg-secondary)', borderRadius:'4px' }}>
                      <span style={{ fontFamily:'var(--font-mono)', fontSize:'12px', color:'var(--text-primary)' }}>{name}</span>
                      <span style={{ fontFamily:'var(--font-mono)', fontSize:'11px', color:'var(--accent-amber)' }}>{type}</span>
                    </div>
                  )
                })}
              </div>
              <div style={{ fontFamily:'var(--font-mono)', fontSize:'10px', color:'var(--text-muted)', letterSpacing:'1px', marginBottom:'8px' }}>PARTITIONS</div>
              <div style={{ display:'flex', gap:'6px', flexWrap:'wrap' }}>
                {selected.partitions.map(p => (
                  <span key={p} style={{ fontFamily:'var(--font-mono)', fontSize:'11px', color:'var(--accent-purple)', background:'rgba(168,85,247,0.1)', padding:'2px 8px', borderRadius:'4px' }}>{p}</span>
                ))}
              </div>
            </div>
          ) : (
            <div style={{ background:'var(--bg-card)', border:'1px solid var(--border)', borderRadius:'12px', padding:'40px', textAlign:'center', color:'var(--text-muted)', fontFamily:'var(--font-mono)', fontSize:'13px' }}>
              ← Select a dataset to view schema
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
