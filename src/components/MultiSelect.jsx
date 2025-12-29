import React, { useState, useRef, useEffect, useMemo } from 'react'
import Icon from './Icon'

export default function MultiSelect({ label, options, selected, onChange, wide }){
  const [isOpen, setIsOpen] = useState(false)
  const [search, setSearch] = useState('')
  const ref = useRef(null)
  const searchRef = useRef(null)

  useEffect(()=>{
    const handleClickOutside = (e)=>{ if(ref.current && !ref.current.contains(e.target)) setIsOpen(false) }
    document.addEventListener('mousedown', handleClickOutside)
    return ()=>document.removeEventListener('mousedown', handleClickOutside)
  },[])

  useEffect(()=>{
    if(isOpen && searchRef.current) searchRef.current.focus()
    if(!isOpen) setSearch('')
  },[isOpen])

  const filteredOptions = useMemo(()=>{
    if(!search.trim()) return options
    const lower = search.toLowerCase()
    return options.filter(opt => opt.toLowerCase().includes(lower))
  },[options, search])

  const toggleOption = (opt)=>{ selected.includes(opt) ? onChange(selected.filter(s=>s!==opt)) : onChange([...selected,opt]) }

  const selectAllFiltered = ()=> onChange([...new Set([...selected, ...filteredOptions])])
  const clearFiltered = ()=> onChange(selected.filter(s => !filteredOptions.includes(s)))

  return (
    <div className={"relative" + (wide ? " max-w-2xl w-full" : "") } ref={ref}>
      <label className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-1 block">{label}</label>
      <button onClick={()=>setIsOpen(!isOpen)} className={"w-full bg-white border border-gray-300 text-left text-sm text-gray-700 rounded-lg p-2.5 shadow-sm flex justify-between items-center hover:border-blue-400 transition" + (wide ? " !max-w-2xl" : "") }>
        <span className="truncate">{selected.length===0 ? 'Select...' : selected.length===options.length ? 'All Selected' : `${selected.length} Selected`}</span>
        <Icon name="chevronDown" size={14} className="text-gray-400" />
      </button>
      {isOpen && (
        <div className={"absolute top-full left-0 w-full mt-1 bg-white border border-gray-200 shadow-xl rounded-lg z-50 max-h-80 flex flex-col" + (wide ? " !max-w-2xl" : "") }>
          <div className="p-2 border-b bg-gray-50 rounded-t-lg">
            <input
              ref={searchRef}
              type="text"
              value={search}
              onChange={(e)=>setSearch(e.target.value)}
              placeholder="Search..."
              className="w-full px-2 py-1.5 text-sm border border-gray-300 rounded focus:outline-none focus:ring-1 focus:ring-blue-400 focus:border-blue-400"
            />
            <div className="flex justify-between mt-2">
              <button onClick={selectAllFiltered} className="text-xs text-blue-600 hover:text-blue-800 font-medium">
                {search ? 'Select Matches' : 'Select All'}
              </button>
              <button onClick={clearFiltered} className="text-xs text-red-500 hover:text-red-700 font-medium">
                {search ? 'Clear Matches' : 'Clear'}
              </button>
            </div>
          </div>
          <div className="overflow-y-auto p-1 custom-scrollbar">
            {filteredOptions.length === 0 ? (
              <div className="p-3 text-sm text-gray-500 text-center">No matches found</div>
            ) : (
              filteredOptions.map(opt=> (
                <label key={opt} className="flex items-center gap-2 p-2 hover:bg-gray-50 rounded cursor-pointer"><input type="checkbox" checked={selected.includes(opt)} onChange={()=>toggleOption(opt)} className="rounded text-blue-600 focus:ring-blue-500"/><span className="text-sm text-gray-700 truncate">{opt}</span></label>
              ))
            )}
          </div>
        </div>
      )}
    </div>
  )
}
