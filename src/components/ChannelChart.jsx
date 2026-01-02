import React, { useMemo } from 'react'
import { ResponsiveContainer, ComposedChart, Area, Line, XAxis, YAxis, Tooltip } from 'recharts'

const formatDate = (ts) => new Date(ts).toLocaleDateString(undefined, { month: 'short', year: '2-digit' })

export default function ChannelChart({ church, data = [], raw = [], color = '#60a5fa', domain = ['auto','auto'], transcriptCounts: propTranscriptCounts = null, onExpand = () => {} }){
  const stats = useMemo(() => {
    if(!data || data.length===0) return { min: 0, max: 0 }
    let min = Infinity, max = -Infinity
    data.forEach(d => { if(typeof d.mentionCount === 'number'){ min = Math.min(min, d.mentionCount); max = Math.max(max, d.mentionCount) } })
    if(min === Infinity) min = 0
    if(max === -Infinity) max = 0
    return { min, max }
  }, [data])

  const transcriptCounts = useMemo(() => {
    if(propTranscriptCounts) return propTranscriptCounts
    const items = (raw && raw.length) ? raw : (data && data.length ? data : [])
    const total = items.length
    const withTranscript = items.filter(it => it && it.path).length
    const withoutTranscript = total - withTranscript
    return { total, withTranscript, withoutTranscript }
  }, [raw, data, propTranscriptCounts])

  // Debug timing removed - was misleading (measured time to next paint across all charts, not this chart's render)

  return (
    <div className="relative cursor-pointer" onClick={()=>onExpand({ church, data: raw.length?raw:data, color, showRaw: true })}>
      <div className="flex justify-between items-center mb-2">
        <div className="text-sm font-medium text-gray-800 truncate">{church}</div>
        <div className="flex items-center gap-3">
          <div className="text-xs text-gray-500">min: <span className="font-semibold">{stats.min}</span></div>
          <div className="text-xs text-gray-500">max: <span className="font-semibold">{stats.max}</span></div>
          <button onClick={(e)=>{ e.stopPropagation(); onExpand({ church, data: raw.length?raw:data, color, showRaw: true }) }} className="text-xs text-blue-600 hover:underline">Expand</button>
        </div>
      </div>
      <div className="flex items-end gap-4">
        <div className="text-xs text-gray-500">{transcriptCounts.total.toLocaleString()} videos</div>
        <div className="text-xs text-green-600">With transcript: <span className="font-semibold text-gray-800">{transcriptCounts.withTranscript.toLocaleString()}</span></div>
        <div className="text-xs text-red-500">Without transcript: <span className="font-semibold text-gray-800">{transcriptCounts.withoutTranscript.toLocaleString()}</span></div>
      </div>
      <div style={{ width: '100%', height: 120 }}>
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={data} margin={{ left: 12 }}>
            <XAxis dataKey="timestamp" type="number" domain={domain} tickFormatter={formatDate} tick={{fontSize:10}} tickCount={4} />
            <YAxis tick={{fontSize:10}} label={{ value: '# mentions', angle: -90, position: 'left', offset: 0, style: { fontSize: 11, fill: '#6b7280' } }} />
            <Tooltip labelFormatter={formatDate} formatter={(v)=>v} />
            <Area type="monotone" dataKey="mentionCount" stroke={color} fill={color} fillOpacity={0.12} isAnimationActive={false} />
            <Line type="monotone" dataKey="rollingAvg" stroke={color} strokeWidth={2} dot={false} isAnimationActive={false} />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
