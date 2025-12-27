import React, { useState, useMemo } from 'react'
import { ComposedChart, Line, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import Icon from './Icon'
import resampleData from '../utils/resample'

export default function ChartModal({ chart, onClose, domain, formatDate, onSelectSermon, searchTerm = '' }){
  const fmt = formatDate || ((ts) => new Date(ts).toLocaleDateString(undefined, { month: 'short', year: 'numeric' }))
  const raw = chart.raw || chart.data || []
  const [smoothing, setSmoothing] = useState(6)
  const [chartType, setChartType] = useState('bar')
  // initialize showRaw from incoming chart so ChannelChart can open raw-per-sermon view
  const [showRaw, setShowRaw] = useState(Boolean(chart && chart.showRaw))

  // bucketize raw into same buckets as resample so aggregated points can reference sermons
  const buckets = useMemo(()=>{
    if(!raw || raw.length===0) return []
    const sorted = [...raw].sort((a,b)=>a.timestamp - b.timestamp)
    const useMonth = sorted.length > 3000
    const map = {}
    sorted.forEach(item=>{
      const date = new Date(item.timestamp)
      let key, ts
      if(useMonth){ key = `${date.getFullYear()}-${date.getMonth()}`; ts = new Date(date.getFullYear(), date.getMonth(), 1).getTime() }
      else { const d = new Date(date); const day = d.getDay(), diff = d.getDate() - day + (day==0? -6 : 1); const monday = new Date(d.setDate(diff)); monday.setHours(0,0,0,0); key = monday.getTime(); ts = key }
      if(!map[key]) map[key] = { timestamp: ts, mentionCount:0, rollingSum:0, count:0, sermons: [] }
      map[key].mentionCount += item.mentionCount || 0
      map[key].rollingSum += (item.rollingAvg || 0)
      map[key].count++
      map[key].sermons.push(item)
    })
    return Object.values(map).sort((a,b)=>a.timestamp-b.timestamp).map(b=>({ ...b, rollingAvg: parseFloat((b.rollingSum / Math.max(1,b.count)).toFixed(1)) }))
  }, [raw])

  const computed = useMemo(()=>{
    if(showRaw) return { data: raw, min: 0, max: Math.max(0, ...raw.map(r=>r.mentionCount||0)) }
    const sampled = buckets
    if(!sampled || !sampled.length) return { data: sampled, min:0, max:0 }
    const out = sampled.map((item, idx, arr)=>{
      let sum=0, cnt=0
      for(let i=Math.max(0, idx - smoothing); i<=idx; i++){ sum += arr[i].mentionCount; cnt++ }
      return { ...item, rollingAvg: cnt>0?parseFloat((sum/cnt).toFixed(1)):0 }
    })
    const vals = out.map(o=>o.mentionCount)
    return { data: out, min: Math.min(...vals), max: Math.max(...vals) }
  }, [raw, buckets, smoothing, showRaw])

  const containerRef = React.useRef(null)

  const exportCSV = () => {
    const rows = computed.data || []
    if(!rows.length) return alert('No data to export')
    let csv
    if(showRaw){
      const header = ['id','timestamp','date','church','title','speaker','mentionCount','path']
      csv = [header.join(',')].concat(rows.map(r => [r.id || '', r.timestamp || '', new Date(r.timestamp||0).toISOString(), (r.church||'').replace(/,/g,' '), (r.title||'').replace(/,/g,' '), (r.speaker||'').replace(/,/g,' '), r.mentionCount||0, r.path||''].join(','))).join('\n')
    } else {
      const header = ['timestamp','date','mentionCount','rollingAvg']
      csv = [header.join(',')].concat(rows.map(r => [r.timestamp, new Date(r.timestamp).toISOString(), r.mentionCount, r.rollingAvg].join(','))).join('\n')
    }
    const blob = new Blob([csv], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a'); a.href = url; a.download = `${chart.church || 'chart'}-data.csv`; a.click(); URL.revokeObjectURL(url)
  }

  const exportPNG = async () => {
    try{
      const node = containerRef.current
      if(!node) return alert('Chart not found')
      const svg = node.querySelector('svg')
      if(!svg) return alert('SVG not found for export')
      const serializer = new XMLSerializer(); let svgString = serializer.serializeToString(svg)
      // inline css hack: add xmlns if missing
      if(!svgString.match(/^<svg[^>]+xmlns="http:\/\/www.w3.org\/2000\/svg"/)){
        svgString = svgString.replace(/^<svg/, '<svg xmlns="http://www.w3.org/2000/svg"')
      }
      const svgBlob = new Blob([svgString], { type: 'image/svg+xml;charset=utf-8' })
      const url = URL.createObjectURL(svgBlob)
      const img = new Image()
      const rect = svg.getBoundingClientRect()
      img.onload = () => {
        const canvas = document.createElement('canvas')
        canvas.width = rect.width * 2
        canvas.height = rect.height * 2
        const ctx = canvas.getContext('2d')
        ctx.fillStyle = '#fff'
        ctx.fillRect(0,0,canvas.width,canvas.height)
        ctx.drawImage(img, 0, 0, canvas.width, canvas.height)
        canvas.toBlob((blob)=>{
          const u = URL.createObjectURL(blob)
          const a = document.createElement('a'); a.href = u; a.download = `${chart.church || 'chart'}.png`; a.click(); URL.revokeObjectURL(u)
        }, 'image/png')
        URL.revokeObjectURL(url)
      }
      img.onerror = () => { URL.revokeObjectURL(url); alert('Export failed') }
      img.src = url
    }catch(e){ console.error(e); alert('Export failed: ' + e.message) }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm p-4" onClick={onClose}>
      <div ref={containerRef} className="bg-white rounded-2xl shadow-2xl w-full max-w-6xl h-[85vh] flex flex-col" onClick={e=>e.stopPropagation()}>
        <div className="p-6 border-b flex justify-between items-center bg-gray-50 rounded-t-2xl">
          <div>
            <h2 className="text-2xl font-bold text-gray-900">{chart.church}</h2>
            {chart.url && <div className="text-sm mt-1"><a href={chart.url} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:text-blue-800 hover:underline">{chart.url}</a></div>}
            {searchTerm ? <div className="text-sm text-blue-600">Search: <span className="font-semibold text-blue-800">{searchTerm}</span></div> : null}
            <div className="text-sm text-gray-500">Min: {computed.min} • Max: {computed.max}</div>
          </div>
            <div className="flex items-center gap-3">
            <label className="text-xs text-gray-600">Smoothing</label>
            <select value={smoothing} onChange={(e)=>setSmoothing(parseInt(e.target.value))} className="text-sm border rounded px-2 py-1">
              <option value={1}>1</option>
              <option value={3}>3</option>
              <option value={6}>6</option>
              <option value={12}>12</option>
            </select>
            <label className="text-xs text-gray-600">Type</label>
            <select value={chartType} onChange={(e)=>setChartType(e.target.value)} className="text-sm border rounded px-2 py-1">
              <option value="bar">Bar</option>
              <option value="area">Area+Line</option>
            </select>
              <label className="text-xs text-gray-600">View</label>
              <select value={showRaw? 'raw' : 'agg'} onChange={(e)=>setShowRaw(e.target.value === 'raw')} className="text-sm border rounded px-2 py-1">
                <option value="agg">Aggregated</option>
                <option value="raw">Show Sermons</option>
              </select>
            <button onClick={exportPNG} className="text-sm bg-gray-100 hover:bg-gray-200 px-3 py-2 rounded">Export PNG</button>
            <button onClick={exportCSV} className="text-sm bg-gray-100 hover:bg-gray-200 px-3 py-2 rounded">Export CSV</button>
            <button onClick={onClose} className="p-2 bg-gray-200 hover:bg-gray-300 rounded-full transition"><Icon name="x" /></button>
          </div>
        </div>
        <div className="flex-1 p-6">
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={computed.data} margin={{ bottom: 120, left: 50, right: 20, top: 20 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e5e7eb" />
              <XAxis dataKey="timestamp" type="number" scale="time" domain={domain} tickFormatter={fmt} tick={{fontSize:10, angle: -45, textAnchor: 'end', height: 100}} interval={Math.ceil(computed.data.length / 4)} />
              <YAxis tick={{fontSize:12}} label={{ value: '# mentions', angle: -90, position: 'insideLeft', offset: -10, style: { fontSize: 12, fill: '#6b7280' } }} />
              <Tooltip content={({ active, payload, label }) => {
                if(!active || !payload || !payload.length) return null
                const p = payload[0].payload
                if(showRaw){
                  // payload is a sermon
                  return (
                    <div className="bg-white p-3 rounded shadow text-sm max-w-sm">
                      <div className="font-semibold truncate">{p.title}</div>
                      <div className="text-xs text-gray-500">{p.church} • {new Date(p.timestamp).toLocaleDateString()}</div>
                      <div className="mt-2 text-xs">Speaker: <strong>{p.speaker || 'Unknown'}</strong></div>
                      <div className="mt-1 text-xs">Duration: <strong>{p.durationHrs ? p.durationHrs.toFixed(2) + ' hrs' : 'n/a'}</strong> • Rate/hr: <strong>{p.mentionsPerHour || 0}</strong></div>
                      <div className="mt-2 text-xs">Mentions: <strong>{p.mentionCount}</strong></div>
                      <div className="mt-3"><button onClick={(e)=>{ e.stopPropagation(); if(onSelectSermon) onSelectSermon(p, 0) }} className="text-blue-600 text-xs">Open Transcript & Jump</button></div>
                    </div>
                  )
                } else {
                  // aggregated bucket - find matching bucket by timestamp
                  const bucket = computed.data.find(d=>d.timestamp === p.timestamp)
                  if(!bucket) return null
                  return (
                    <div className="bg-white p-3 rounded shadow text-sm max-w-xs">
                      <div className="font-semibold">{new Date(bucket.timestamp).toLocaleDateString()}</div>
                      <div className="text-xs text-gray-500">Mentions: <strong>{bucket.mentionCount}</strong></div>
                      <div className="mt-2 text-xs">Sermons in bucket:</div>
                      <ul className="max-h-40 overflow-auto mt-2">
                        {bucket.sermons.slice(0,20).map((s,i)=> (
                          <li key={s.id || i} className="py-1 border-b border-gray-100"><button onClick={()=>{ if(onSelectSermon) onSelectSermon(s, 0) }} className="text-left text-sm text-blue-600 truncate">{s.date} • {s.title} — {s.speaker}</button></li>
                        ))}
                        {bucket.sermons.length > 20 && <li className="text-xs text-gray-500">... {bucket.sermons.length - 20} more</li>}
                      </ul>
                    </div>
                  )
                }
              }} labelFormatter={fmt} contentStyle={{borderRadius:'8px'}} />
              <Legend verticalAlign="top" height={36} />
              {chartType === 'bar' ? (
                <>
                  <Bar dataKey="mentionCount" name="Mentions" fill="#94a3b8" barSize={6} onClick={(p)=>{ if(showRaw){ if(onSelectSermon) onSelectSermon(p.payload, 0) } }} />
                  {/* overlay trend line on top for bar view */}
                  <Line type="monotone" dataKey="rollingAvg" name="Trend" stroke={chart.color} strokeWidth={2} dot={false} connectNulls={true} />
                </>
              ) : (
                <>
                  <Area type="monotone" dataKey="mentionCount" name="Volume" stroke="#93c5fd" fill="#bfdbfe" fillOpacity={1} isAnimationActive={false} />
                  <Line type="monotone" dataKey="rollingAvg" name="Trend" stroke={chart.color} strokeWidth={3} dot={false} connectNulls={true} />
                </>
              )}
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  )
}
