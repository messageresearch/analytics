import React, { useState, useRef, useEffect, useMemo, useCallback } from 'react'
import { FixedSizeList as List } from 'react-window'
import Icon from './Icon'

export default function MultiSelect({ label, options, selected, onChange, wide }){
  const [isOpen, setIsOpen] = useState(false)
  const [search, setSearch] = useState('')
  const [debouncedSearch, setDebouncedSearch] = useState('')
  const ref = useRef(null)
  const searchRef = useRef(null)

  // Debounce search input (150ms)
  useEffect(()=>{
    const timer = setTimeout(()=> setDebouncedSearch(search), 150)
    return ()=> clearTimeout(timer)
  },[search])

  useEffect(()=>{
    const handleClickOutside = (e)=>{ if(ref.current && !ref.current.contains(e.target)) setIsOpen(false) }
    document.addEventListener('mousedown', handleClickOutside)
    return ()=>document.removeEventListener('mousedown', handleClickOutside)
  },[])

  useEffect(()=>{
    if(isOpen && searchRef.current) searchRef.current.focus()
    if(!isOpen){ setSearch(''); setDebouncedSearch('') }
  },[isOpen])

  // Convert selected to Set for O(1) lookups
  const selectedSet = useMemo(()=> new Set(selected), [selected])

  const filteredOptions = useMemo(()=>{
    if(!debouncedSearch.trim()) return options
    const lower = debouncedSearch.toLowerCase()
    return options.filter(opt => opt.toLowerCase().includes(lower))
  },[options, debouncedSearch])

  const toggleOption = useCallback((opt)=>{
    selectedSet.has(opt) ? onChange(selected.filter(s=>s!==opt)) : onChange([...selected,opt])
  },[selected, selectedSet, onChange])

  const selectAllFiltered = useCallback(()=> {
    const newSet = new Set(selected)
    filteredOptions.forEach(o => newSet.add(o))
    onChange([...newSet])
  },[selected, filteredOptions, onChange])

  const clearFiltered = useCallback(()=> {
    const toRemove = new Set(filteredOptions)
    onChange(selected.filter(s => !toRemove.has(s)))
  },[selected, filteredOptions, onChange])

  // Virtualized row renderer
  const Row = useCallback(({ index, style })=>{
    const opt = filteredOptions[index]
    const isChecked = selectedSet.has(opt)
    return (
      <div style={style} className="px-1">
        <label className="flex items-center gap-2 p-2 hover:bg-gray-50 rounded cursor-pointer">
          <input
            type="checkbox"
            checked={isChecked}
            onChange={()=>toggleOption(opt)}
            className="rounded text-blue-600 focus:ring-blue-500"
          />
          <span className="text-sm text-gray-700 truncate">{opt}</span>
        </label>
      </div>
    )
  },[filteredOptions, selectedSet, toggleOption])

  const listHeight = Math.min(filteredOptions.length * 36, 280)

  return (
    <div className={"relative" + (wide ? " max-w-2xl w-full" : "") } ref={ref}>
      <label className="text-xs font-bold text-gray-500 uppercase tracking-wide mb-1 block">{label}</label>
      <button onClick={()=>setIsOpen(!isOpen)} className={"w-full bg-white border border-gray-300 text-left text-sm text-gray-700 rounded-lg p-2.5 shadow-sm flex justify-between items-center hover:border-blue-400 transition" + (wide ? " !max-w-2xl" : "") }>
        <span className="truncate">{selected.length===0 ? 'Select...' : selected.length===options.length ? 'All Selected' : `${selected.length} Selected`}</span>
        <Icon name="chevronDown" size={14} className="text-gray-400" />
      </button>
      {isOpen && (
        <div className={"absolute top-full left-0 w-full mt-1 bg-white border border-gray-200 shadow-xl rounded-lg z-50 flex flex-col" + (wide ? " !max-w-2xl" : "") }>
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
                {debouncedSearch ? `Select ${filteredOptions.length} Matches` : 'Select All'}
              </button>
              <button onClick={clearFiltered} className="text-xs text-red-500 hover:text-red-700 font-medium">
                {debouncedSearch ? 'Clear Matches' : 'Clear'}
              </button>
            </div>
          </div>
          {filteredOptions.length === 0 ? (
            <div className="p-3 text-sm text-gray-500 text-center">No matches found</div>
          ) : (
            <List
              height={listHeight}
              itemCount={filteredOptions.length}
              itemSize={36}
              width="100%"
              className="custom-scrollbar"
            >
              {Row}
            </List>
          )}
        </div>
      )}
    </div>
  )
}
