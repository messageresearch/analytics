import React from 'react'
import Icon from './Icon'

export default function HeatmapDetails({ data, onClose, onSelect }){
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50" onClick={onClose}>
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-md overflow-hidden" onClick={e=>e.stopPropagation()}>
        <div className="p-4 border-b bg-gray-50 flex justify-between items-center">
          <div>
            <h3 className="font-bold text-gray-900">Activity Details</h3>
            <p className="text-xs text-gray-500">{data.label}</p>
          </div>
          <button onClick={onClose} className="p-1 hover:bg-gray-200 rounded"><Icon name="x" /></button>
        </div>
        <div className="max-h-80 overflow-y-auto p-2 custom-scrollbar">
          {data.sermons && data.sermons.length > 0 ? (
            <div className="space-y-2">{data.sermons.map((s,i)=> (
              <div key={i} onClick={()=>onSelect(s)} className="p-3 border rounded-lg hover:bg-blue-50 cursor-pointer transition">
                <h4 className="font-bold text-sm text-blue-700">{s.title}</h4>
                <div className="flex justify-between items-center mt-1 text-xs text-gray-600"><span>{s.church}</span><span className="bg-green-100 text-green-800 px-2 py-0.5 rounded font-bold">{s.mentionCount} Mentions</span></div>
              </div>
            ))}</div>
          ) : (<div className="p-4 text-center text-gray-500">No sermons found for this period.</div>)}
        </div>
      </div>
    </div>
  )
}
