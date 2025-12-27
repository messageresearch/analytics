import React, { useState, useRef, useEffect } from 'react'
import Icon from './Icon'

export default function MultiSelect({ label, options, selected, onChange }){
  const [isOpen, setIsOpen] = useState(false)
  const ref = useRef(null)
  useEffect(()=>{
    const handleClickOutside = (e)=>{ if(ref.current && !ref.current.contains(e.target)) setIsOpen(false) }
    document.addEventListener('mousedown', handleClickOutside)
    return ()=>document.removeEventListener('mousedown', handleClickOutside)
  },[])
  const toggleOption = (opt)=>{ selected.includes(opt) ? onChange(selected.filter(s=>s!==opt)) : onChange([...selected,opt]) }
  return (
    <div className="relative" ref={ref}>
      <label className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-1 block">{label}</label>
      <button onClick={()=>setIsOpen(!isOpen)} className="w-full bg-white border border-gray-300 text-left text-sm text-gray-700 rounded-lg p-2.5 shadow-sm flex justify-between items-center hover:border-blue-400 transition">
        <span className="truncate">{selected.length===0 ? 'Select...' : selected.length===options.length ? 'All Selected' : `${selected.length} Selected`}</span>
        <Icon name="chevronDown" size={14} className="text-gray-400" />
      </button>
      {isOpen && (
        <div className="absolute top-full left-0 w-full mt-1 bg-white border border-gray-200 shadow-xl rounded-lg z-50 max-h-60 flex flex-col">
          <div className="p-2 border-b flex justify-between bg-gray-50 rounded-t-lg">
            <button onClick={()=>onChange(options)} className="text-xs text-blue-600 hover:text-blue-800 font-medium">Select All</button>
            <button onClick={()=>onChange([])} className="text-xs text-red-500 hover:text-red-700 font-medium">Clear</button>
          </div>
          <div className="overflow-y-auto p-1 custom-scrollbar">
            {options.map(opt=> (
              <label key={opt} className="flex items-center gap-2 p-2 hover:bg-gray-50 rounded cursor-pointer"><input type="checkbox" checked={selected.includes(opt)} onChange={()=>toggleOption(opt)} className="rounded text-blue-600 focus:ring-blue-500"/><span className="text-sm text-gray-700 truncate">{opt}</span></label>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
