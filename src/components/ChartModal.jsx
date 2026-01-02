import React, { useState, useMemo, useRef, useEffect } from 'react'
import { ComposedChart, Line, Bar, Area, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import html2canvas from 'html2canvas'
import Icon from './Icon'
import resampleData from '../utils/resample'

export default function ChartModal({ chart, onClose, domain, formatDate, onSelectSermon, searchTerm = '' }){
  const fmt = formatDate || ((ts) => new Date(ts).toLocaleDateString(undefined, { month: 'short', year: 'numeric' }))
  const raw = chart.raw || chart.data || []
  // Trend window in weeks: 4 ≈ 1 month, 12 ≈ 3 months, 26 ≈ 6 months (matches main chart)
  const [trendWindow, setTrendWindow] = useState(26)
  const [chartType, setChartType] = useState('bar')
  // initialize showRaw from incoming chart so ChannelChart can open raw-per-sermon view
  const [showRaw, setShowRaw] = useState(Boolean(chart && chart.showRaw))
  // Pinned popup state - when user clicks a bar/point, freeze the popup so they can interact with it
  const [pinnedBucket, setPinnedBucket] = useState(null)
  const [pinnedSermon, setPinnedSermon] = useState(null)
  const [pinnedPosition, setPinnedPosition] = useState({ x: 0, y: 0 })
  const pinnedRef = useRef(null)

  // Close pinned popup when clicking outside
  useEffect(() => {
    if (!pinnedBucket && !pinnedSermon) return
    const handleClickOutside = (e) => {
      if (pinnedRef.current && !pinnedRef.current.contains(e.target)) {
        setPinnedBucket(null)
        setPinnedSermon(null)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [pinnedBucket, pinnedSermon])

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

  // Calculate directly without useMemo to ensure trendWindow changes trigger recalc
  const computed = (() => {
    if(showRaw) {
      return { data: raw, min: 0, max: Math.max(0, ...raw.map(r=>r.mentionCount||0)) }
    }
    
    // Recalculate buckets inline
    if(!raw || raw.length===0) {
      return { data: [], min:0, max:0 }
    }
    const sorted = [...raw].sort((a,b)=>a.timestamp - b.timestamp)
    const useMonth = sorted.length > 3000
    const map = {}
    sorted.forEach(item=>{
      const date = new Date(item.timestamp)
      let key, ts
      if(useMonth){ key = `${date.getFullYear()}-${date.getMonth()}`; ts = new Date(date.getFullYear(), date.getMonth(), 1).getTime() }
      else { const d = new Date(date); const day = d.getDay(), diff = d.getDate() - day + (day==0? -6 : 1); const monday = new Date(d.setDate(diff)); monday.setHours(0,0,0,0); key = monday.getTime(); ts = key }
      if(!map[key]) map[key] = { timestamp: ts, mentionCount:0, count:0, sermons: [] }
      map[key].mentionCount += item.mentionCount || 0
      map[key].count++
      map[key].sermons.push(item)
    })
    const sampled = Object.values(map).sort((a,b)=>a.timestamp-b.timestamp)
    
    if(!sampled || !sampled.length) {
      return { data: sampled, min:0, max:0 }
    }
    
    // Time-based rolling average window in milliseconds
    // trendWindow is in weeks: 4 weeks ≈ 1 month, 12 weeks ≈ 3 months, 26 weeks ≈ 6 months
    const windowMs = trendWindow * 7 * 24 * 60 * 60 * 1000
    
    const out = sampled.map((item, idx, arr)=>{
      let sum = 0, cnt = 0
      // Look back through all buckets within the time window
      for(let i = idx; i >= 0; i--){
        if(item.timestamp - arr[i].timestamp > windowMs) break
        sum += arr[i].mentionCount
        cnt++
      }
      return { ...item, rollingAvg: cnt > 0 ? parseFloat((sum / cnt).toFixed(1)) : 0 }
    })
    
    const vals = out.map(o => o.mentionCount)
    return { data: out, min: Math.min(...vals), max: Math.max(...vals) }
  })()

  const containerRef = React.useRef(null)

  const exportCSV = () => {
    // Export sermon-level data with full details and search context
    const sermons = raw || []
    if(!sermons.length) return alert('No data to export')
    
    // Format date as YYYY-MM-DD
    const formatDateCSV = (ts) => {
      if (!ts) return ''
      const d = new Date(ts)
      return d.toISOString().split('T')[0]
    }
    
    // Escape CSV values (handle commas, quotes, newlines)
    const escapeCSV = (val) => {
      if (val === null || val === undefined) return ''
      const str = String(val).trim()
      if (str.includes(',') || str.includes('"') || str.includes('\n')) {
        return '"' + str.replace(/"/g, '""') + '"'
      }
      return str
    }
    
    // Build CSV with comprehensive data
    // Note: searchTerm prop indicates what the mentionCount is counting
    const searchContext = searchTerm || 'William Branham (default)'
    const header = ['date','church','speaker','title','youtube_url','mention_count','mention_term']
    const rows = sermons
      .sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0))
      .map(s => [
        formatDateCSV(s.timestamp),
        escapeCSV(s.church || chart.church || ''),
        escapeCSV(s.speaker || ''),
        escapeCSV(s.title || ''),
        s.videoUrl || s.url || '',
        s.mentionCount || 0,
        escapeCSV(searchContext)
      ].join(','))
    
    const csv = [header.join(','), ...rows].join('\n')
    const blob = new Blob([csv], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    // Include search term in filename for context
    const termSlug = (searchTerm || 'branham').replace(/[^a-zA-Z0-9]+/g, '_').substring(0, 30)
    a.download = `${(chart.church || 'chart').replace(/[^a-zA-Z0-9]+/g, '_')}-${termSlug}-sermons.csv`
    a.click()
    URL.revokeObjectURL(url)
  }

  const exportPNG = async () => {
    try {
      const modalContent = containerRef.current
      if (!modalContent) {
        alert('Chart not found')
        return
      }
      
      // Hide select elements and show text values instead (html2canvas has issues with selects)
      const selects = modalContent.querySelectorAll('select')
      const tempSpans = []
      selects.forEach(sel => {
        sel.style.display = 'none'
        const span = document.createElement('span')
        span.textContent = sel.options[sel.selectedIndex]?.text || ''
        span.className = 'text-sm border rounded px-2 py-1 bg-white inline-block'
        sel.parentNode.insertBefore(span, sel.nextSibling)
        tempSpans.push(span)
      })
      
      // Hide export buttons during capture
      const exportBtns = modalContent.querySelectorAll('button')
      exportBtns.forEach(btn => {
        if (btn.textContent.includes('Export') || btn.querySelector('svg')) {
          btn.dataset.wasVisible = btn.style.visibility
          btn.style.visibility = 'hidden'
        }
      })
      
      // Use html2canvas to capture the entire modal
      const canvas = await html2canvas(modalContent, {
        backgroundColor: '#ffffff',
        scale: 2,
        logging: false,
        useCORS: true,
        allowTaint: true,
      })
      
      // Restore select elements
      selects.forEach(sel => { sel.style.display = '' })
      tempSpans.forEach(span => span.remove())
      exportBtns.forEach(btn => {
        if (btn.dataset.wasVisible !== undefined) {
          btn.style.visibility = btn.dataset.wasVisible || ''
          delete btn.dataset.wasVisible
        }
      })
      
      // Convert to blob and download
      canvas.toBlob((blob) => {
        if (!blob) {
          alert('Failed to create image')
          return
        }
        const url = URL.createObjectURL(blob)
        const a = document.createElement('a')
        a.href = url
        a.download = `${chart.church || 'chart'}-${new Date().toISOString().split('T')[0]}.png`
        document.body.appendChild(a)
        a.click()
        document.body.removeChild(a)
        URL.revokeObjectURL(url)
      }, 'image/png')
    } catch (e) {
      console.error('Export failed:', e)
      alert('Export failed: ' + (e?.message || e?.toString() || 'Unknown error'))
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm p-2 sm:p-4" onClick={onClose}>
      <div ref={containerRef} className="bg-white rounded-2xl shadow-2xl w-full max-w-6xl h-[95vh] sm:h-[85vh] flex flex-col overflow-hidden" onClick={e=>e.stopPropagation()}>
        {/* Mobile close button - fixed at top */}
        <button onClick={onClose} className="sm:hidden absolute top-4 right-4 z-50 p-3 bg-gray-800/80 hover:bg-gray-900 text-white rounded-full shadow-lg transition" aria-label="Close">
          <Icon name="x" size={24} />
        </button>
        
        <div className="p-4 sm:p-6 border-b bg-gray-50 rounded-t-2xl flex-shrink-0 overflow-x-auto">
          {/* Header row with title and close button */}
          <div className="flex justify-between items-start gap-4 mb-3 sm:mb-0">
            <div className="flex-1 min-w-0">
              <h2 className="text-lg sm:text-2xl font-bold text-gray-900 truncate pr-12 sm:pr-0">{chart.church}</h2>
              {chart.url && <div className="text-xs sm:text-sm mt-1 truncate"><a href={chart.url} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:text-blue-800 hover:underline">{chart.url}</a></div>}
              {searchTerm ? <div className="text-xs sm:text-sm text-blue-600">Search: <span className="font-semibold text-blue-800">{searchTerm}</span></div> : null}
              <div className="text-xs sm:text-sm text-gray-500">Min: {computed.min} • Max: {computed.max}{!showRaw && ` • Trend: ${trendWindow === 4 ? '1 Mo' : trendWindow === 12 ? '3 Mo' : '6 Mo'}`}</div>
            </div>
            {/* Desktop close button */}
            <button onClick={onClose} className="hidden sm:flex p-2 bg-gray-200 hover:bg-gray-300 rounded-full transition flex-shrink-0"><Icon name="x" /></button>
          </div>
          
          {/* Controls row - scrollable on mobile */}
          <div className="flex flex-wrap items-center gap-2 sm:gap-3 mt-3 sm:mt-4">
            {!showRaw && (
              <>
                <label className="text-xs text-gray-600 hidden sm:inline" title="Rolling average window for trend line smoothing">Trend</label>
                <div className="flex bg-gray-100 rounded-lg p-1" title="Shorter = more responsive, Longer = smoother trend">
                  {[{v:4,l:'1Mo',t:'1-month rolling average'},{v:12,l:'3Mo',t:'3-month rolling average'},{v:26,l:'6Mo',t:'6-month rolling average'}].map(({v,l,t})=> 
                    <button key={v} onClick={()=>setTrendWindow(v)} title={t} className={`px-2 py-1 text-xs font-medium rounded ${trendWindow===v ? 'bg-white shadow text-blue-600' : 'text-gray-500'}`}>{l}</button>
                  )}
                </div>
              </>
            )}
            <label className="text-xs text-gray-600 hidden sm:inline">Type</label>
            <select value={chartType} onChange={(e)=>setChartType(e.target.value)} className="text-xs sm:text-sm border rounded px-2 py-1">
              <option value="bar">Bar</option>
              <option value="area">Area</option>
            </select>
            <label className="text-xs text-gray-600 hidden sm:inline">View</label>
            <select value={showRaw? 'raw' : 'agg'} onChange={(e)=>setShowRaw(e.target.value === 'raw')} className="text-xs sm:text-sm border rounded px-2 py-1" title="Weekly Totals shows aggregated data with trends; Individual Sermons shows each sermon as a point">
              <option value="agg">Weekly</option>
              <option value="raw">Sermons</option>
            </select>
            <div className="flex gap-1 sm:gap-2 ml-auto">
              <button onClick={exportPNG} className="text-xs sm:text-sm bg-gray-100 hover:bg-gray-200 px-2 sm:px-3 py-1 sm:py-2 rounded" title="Export as PNG"><span className="hidden sm:inline">Export </span>PNG</button>
              <button onClick={exportCSV} className="text-xs sm:text-sm bg-gray-100 hover:bg-gray-200 px-2 sm:px-3 py-1 sm:py-2 rounded" title="Export as CSV"><span className="hidden sm:inline">Export </span>CSV</button>
            </div>
          </div>
        </div>
        <div className="flex-1 p-6 relative">
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart key={`chart-${trendWindow}-${chartType}`} data={computed.data} margin={{ bottom: 120, left: 50, right: 20, top: 20 }}
              onClick={(e) => {
                // Handle click on chart area to pin tooltip
                if (!showRaw && e && e.activePayload && e.activePayload[0]) {
                  const bucket = computed.data.find(d => d.timestamp === e.activePayload[0].payload.timestamp)
                  if (bucket) {
                    setPinnedBucket(bucket)
                    setPinnedSermon(null)
                    setPinnedPosition({ x: e.chartX || 200, y: e.chartY || 100 })
                  }
                } else if (showRaw && e && e.activePayload && e.activePayload[0]) {
                  // Pin the individual sermon popup
                  setPinnedSermon(e.activePayload[0].payload)
                  setPinnedBucket(null)
                  setPinnedPosition({ x: e.chartX || 200, y: e.chartY || 100 })
                }
              }}
            >
              <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e5e7eb" />
              <XAxis dataKey="timestamp" type="number" scale="time" domain={domain} tickFormatter={fmt} tick={{fontSize:10, angle: -45, textAnchor: 'end', height: 100}} interval={Math.ceil(computed.data.length / 4)} />
              <YAxis tick={{fontSize:12}} label={{ value: '# mentions', angle: -90, position: 'insideLeft', offset: -10, style: { fontSize: 12, fill: '#6b7280' } }} />
              <Tooltip 
                content={({ active, payload, label }) => {
                  // Don't show hover tooltip if we have a pinned popup
                  if (pinnedBucket || pinnedSermon) return null
                  if(!active || !payload || !payload.length) return null
                  const p = payload[0].payload
                  if(showRaw){
                    // payload is a sermon - show summary tooltip
                    return (
                      <div className="bg-white p-3 rounded shadow text-sm max-w-sm">
                        <div className="font-semibold truncate">{p.title}</div>
                        <div className="text-xs text-gray-500">{p.church} • {new Date(p.timestamp).toLocaleDateString()}</div>
                        <div className="mt-2 text-xs">Mentions: <strong>{p.mentionCount}</strong></div>
                        <div className="mt-2 text-blue-600 text-xs font-medium">Click for details →</div>
                      </div>
                    )
                  } else {
                    // aggregated bucket - show summary, click to pin full popup
                    const bucket = computed.data.find(d=>d.timestamp === p.timestamp)
                    if(!bucket) return null
                    const weekEnd = new Date(bucket.timestamp + 6*24*60*60*1000)
                    const dateRange = `${new Date(bucket.timestamp).toLocaleDateString(undefined, {month:'short', day:'numeric'})} - ${weekEnd.toLocaleDateString(undefined, {month:'short', day:'numeric', year:'numeric'})}`
                    return (
                      <div className="bg-white p-3 rounded-lg shadow-lg text-sm border border-gray-200">
                        <div className="font-bold text-gray-900">Week of {dateRange}</div>
                        <div className="text-xs text-gray-500 mt-1">{bucket.mentionCount} mentions • {bucket.sermons.length} sermon{bucket.sermons.length !== 1 ? 's' : ''}</div>
                        <div className="mt-2 text-blue-600 text-xs font-medium">Click to browse sermons →</div>
                      </div>
                    )
                  }
                }} 
                labelFormatter={fmt} 
                contentStyle={{borderRadius:'8px'}} 
              />
              <Legend verticalAlign="top" height={36} />
              {chartType === 'bar' ? (
                <>
                  <Bar 
                    dataKey="mentionCount" 
                    name="Mentions" 
                    fill="#94a3b8" 
                    barSize={6} 
                    isAnimationActive={false} 
                    cursor="pointer"
                  />
                  {/* overlay trend line on top for bar view */}
                  <Line type="monotone" dataKey="rollingAvg" name="Trend" stroke={chart.color} strokeWidth={2} dot={false} connectNulls={true} isAnimationActive={false} />
                </>
              ) : (
                <>
                  <Area 
                    type="monotone" 
                    dataKey="mentionCount" 
                    name="Volume" 
                    stroke="#93c5fd" 
                    fill="#bfdbfe" 
                    fillOpacity={1} 
                    isAnimationActive={false}
                    cursor="pointer"
                  />
                  <Line type="monotone" dataKey="rollingAvg" name="Trend" stroke={chart.color} strokeWidth={3} dot={false} connectNulls={true} isAnimationActive={false} />
                </>
              )}
            </ComposedChart>
          </ResponsiveContainer>

          {/* Pinned popup for browsing sermons - appears when clicking a bar */}
          {pinnedBucket && !showRaw && (
            <div 
              ref={pinnedRef}
              className="fixed sm:absolute inset-x-2 sm:inset-x-auto bottom-2 sm:bottom-auto bg-white rounded-xl shadow-2xl border border-gray-200 sm:w-96 max-h-[60vh] sm:max-h-[500px] flex flex-col z-50"
              style={{ 
                left: typeof window !== 'undefined' && window.innerWidth >= 640 ? Math.min(pinnedPosition.x, window.innerWidth - 450) : undefined, 
                top: typeof window !== 'undefined' && window.innerWidth >= 640 ? Math.max(20, Math.min(pinnedPosition.y - 50, window.innerHeight - 550)) : undefined
              }}
            >
              <div className="p-4 border-b border-gray-100">
                <div className="flex justify-between items-start">
                  <div>
                    <div className="font-bold text-gray-900">
                      Week of {new Date(pinnedBucket.timestamp).toLocaleDateString(undefined, {month:'short', day:'numeric'})} - {new Date(pinnedBucket.timestamp + 6*24*60*60*1000).toLocaleDateString(undefined, {month:'short', day:'numeric', year:'numeric'})}
                    </div>
                    <div className="text-sm text-gray-500 mt-1">{pinnedBucket.sermons.length} sermon{pinnedBucket.sermons.length !== 1 ? 's' : ''}</div>
                  </div>
                  <button onClick={()=>setPinnedBucket(null)} className="p-1 hover:bg-gray-100 rounded-full"><Icon name="x" /></button>
                </div>
                <div className="flex gap-4 mt-3 text-sm">
                  <div className="text-center">
                    <div className="text-xl font-bold text-blue-600">{pinnedBucket.mentionCount}</div>
                    <div className="text-xs text-gray-500">mentions</div>
                  </div>
                  <div className="text-center">
                    <div className="text-xl font-bold text-gray-700">{(pinnedBucket.mentionCount / Math.max(1, pinnedBucket.sermons.length)).toFixed(1)}</div>
                    <div className="text-xs text-gray-500">avg/sermon</div>
                  </div>
                  <div className="text-center">
                    <div className="text-xl font-bold text-emerald-600">{pinnedBucket.rollingAvg || 'n/a'}</div>
                    <div className="text-xs text-gray-500">trend</div>
                  </div>
                </div>
              </div>
              <div className="text-xs font-medium text-gray-600 px-4 pt-3 pb-2">Click a sermon to view transcript:</div>
              <div className="flex-1 overflow-auto px-3 pb-3">
                <div className="space-y-2">
                  {pinnedBucket.sermons.map((s,i)=> (
                    <button 
                      key={s.id || i}
                      onClick={()=>{ if(onSelectSermon) onSelectSermon(s, 0) }} 
                      className="w-full text-left p-3 bg-gray-50 rounded-lg border border-gray-200 hover:bg-blue-50 hover:border-blue-300 transition-all group"
                    >
                      <div className="font-medium text-gray-900 group-hover:text-blue-600 line-clamp-2">{s.title || 'Untitled'}</div>
                      <div className="flex justify-between items-center mt-2 text-xs">
                        <span className="text-gray-500">{s.speaker || 'Unknown'}</span>
                        <span className="text-gray-400">{new Date(s.timestamp).toLocaleDateString()}</span>
                      </div>
                      <div className="flex justify-between items-center mt-1 text-xs">
                        <span className="text-blue-600 font-medium">{s.mentionCount || 0} mentions</span>
                        {s.durationHrs && <span className="text-gray-400">{s.durationHrs.toFixed(1)} hrs</span>}
                      </div>
                    </button>
                  ))}
                </div>
              </div>
            </div>
          )}

          {/* Pinned popup for individual sermon details */}
          {pinnedSermon && showRaw && (
            <div 
              ref={pinnedRef}
              className="fixed sm:absolute inset-x-2 sm:inset-x-auto bottom-2 sm:bottom-auto bg-white rounded-xl shadow-2xl border border-gray-200 sm:w-80 z-50"
              style={{ 
                left: typeof window !== 'undefined' && window.innerWidth >= 640 ? Math.min(pinnedPosition.x, window.innerWidth - 400) : undefined, 
                top: typeof window !== 'undefined' && window.innerWidth >= 640 ? Math.max(20, Math.min(pinnedPosition.y - 50, window.innerHeight - 400)) : undefined
              }}
            >
              <div className="p-4">
                <div className="flex justify-between items-start gap-2">
                  <div className="flex-1 min-w-0">
                    <div className="font-bold text-gray-900 line-clamp-3">{pinnedSermon.title || 'Untitled'}</div>
                    <div className="text-sm text-gray-500 mt-1">{pinnedSermon.speaker || 'Unknown Speaker'}</div>
                    <div className="text-xs text-gray-400 mt-1">{pinnedSermon.church} • {new Date(pinnedSermon.timestamp).toLocaleDateString()}</div>
                  </div>
                  <button onClick={()=>setPinnedSermon(null)} className="p-1 hover:bg-gray-100 rounded-full flex-shrink-0"><Icon name="x" /></button>
                </div>
                
                <div className="flex gap-4 mt-4 pt-4 border-t border-gray-100">
                  <div className="text-center flex-1">
                    <div className="text-2xl font-bold text-blue-600">{pinnedSermon.mentionCount || 0}</div>
                    <div className="text-xs text-gray-500">mentions</div>
                  </div>
                  <div className="text-center flex-1">
                    <div className="text-2xl font-bold text-gray-700">{pinnedSermon.durationHrs ? pinnedSermon.durationHrs.toFixed(1) : 'n/a'}</div>
                    <div className="text-xs text-gray-500">hours</div>
                  </div>
                  <div className="text-center flex-1">
                    <div className="text-2xl font-bold text-emerald-600">{pinnedSermon.mentionsPerHour || 0}</div>
                    <div className="text-xs text-gray-500">per hour</div>
                  </div>
                </div>

                <button 
                  onClick={()=>{ if(onSelectSermon) onSelectSermon(pinnedSermon, 0) }} 
                  className="w-full mt-4 py-3 bg-blue-600 hover:bg-blue-700 text-white font-medium rounded-lg transition-colors"
                >
                  Open Transcript
                </button>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
