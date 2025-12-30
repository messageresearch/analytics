import React, { useState, useRef, useEffect, useMemo, useCallback, startTransition } from 'react'
import { FixedSizeList as List } from 'react-window'
import Icon from './Icon'

export default function MultiSelect({ label, options, selected, onChange, wide }){
  const [isOpen, setIsOpen] = useState(false)
  const [search, setSearch] = useState('')
  const [debouncedSearch, setDebouncedSearch] = useState('')
  const [isBatchUpdating, setIsBatchUpdating] = useState(false)
  const [lastSearchTerm, setLastSearchTerm] = useState('')
  const ref = useRef(null)
  const searchRef = useRef(null)

  // Debounce search input (150ms)
  useEffect(()=>{
    const timer = setTimeout(()=> setDebouncedSearch(search), 150)
    return ()=> clearTimeout(timer)
  },[search])

  // Just update search - no auto-clear anymore since we have Add option
  const handleSearchChange = useCallback((e)=>{
    setSearch(e.target.value)
  },[])

  useEffect(()=>{
    const handleClickOutside = (e)=>{ if(ref.current && !ref.current.contains(e.target)) setIsOpen(false) }
    document.addEventListener('mousedown', handleClickOutside)
    return ()=>document.removeEventListener('mousedown', handleClickOutside)
  },[])

  useEffect(()=>{
    if(isOpen && searchRef.current) searchRef.current.focus()
    if(!isOpen){ setSearch(''); setDebouncedSearch(''); setIsBatchUpdating(false) }
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
    // Capture current matches and search term before any state changes
    const matchesToAdd = [...filteredOptions]
    const searchUsed = debouncedSearch.trim()
    
    if(matchesToAdd.length > 500) setIsBatchUpdating(true)
    if(matchesToAdd.length > 100) setIsOpen(false)
    
    // Save the search term used for display
    if(searchUsed) setLastSearchTerm(searchUsed)
    
    // Replace: start fresh with only the matches
    startTransition(()=>{
      onChange([...matchesToAdd])
    })
  },[filteredOptions, debouncedSearch, onChange])

  // Add matches to existing selection
  const addToFiltered = useCallback(()=> {
    const matchesToAdd = [...filteredOptions]
    const currentSelected = [...selected]
    const searchUsed = debouncedSearch.trim()
    
    if(matchesToAdd.length > 500) setIsBatchUpdating(true)
    if(matchesToAdd.length > 100) setIsOpen(false)
    
    // Append search term to display
    if(searchUsed){
      setLastSearchTerm(prev => prev ? `${prev}, ${searchUsed}` : searchUsed)
    }
    
    startTransition(()=>{
      const newSet = new Set(currentSelected)
      for(let i=0; i<matchesToAdd.length; i++) newSet.add(matchesToAdd[i])
      onChange([...newSet])
    })
  },[selected, filteredOptions, debouncedSearch, onChange])

  const clearFiltered = useCallback(()=> {
    // Capture current matches before any state changes
    const matchesToRemove = new Set(filteredOptions)
    const currentSelected = [...selected]
    
    if(filteredOptions.length > 500) setIsBatchUpdating(true)
    if(filteredOptions.length > 100) setIsOpen(false)
    
    // Clear the last search term when clearing
    setLastSearchTerm('')
    
    startTransition(()=>{
      onChange(currentSelected.filter(s => !matchesToRemove.has(s)))
    })
  },[selected, filteredOptions, onChange])

  // Handle Enter key to select matches
  const handleKeyDown = useCallback((e)=>{
    if(e.key === 'Enter' && debouncedSearch.trim() && filteredOptions.length > 0){
      e.preventDefault()
      // Enter always selects (replaces), use "+ Add" button to append
      selectAllFiltered()
    }
  },[debouncedSearch, filteredOptions.length, selectAllFiltered])

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
      <button onClick={()=>setIsOpen(!isOpen)} disabled={isBatchUpdating} className={"w-full bg-white border border-gray-300 text-left text-sm text-gray-700 rounded-lg p-2.5 shadow-sm flex justify-between items-center hover:border-blue-400 transition" + (wide ? " !max-w-2xl" : "") + (isBatchUpdating ? " opacity-50 cursor-wait" : "")}>
        <span className="truncate">
          {isBatchUpdating ? 'Updating...' : 
           selected.length===0 ? 'Select...' : 
           selected.length===options.length ? 'All Selected' : 
           lastSearchTerm ? `${selected.length} matching "${lastSearchTerm}"` :
           `${selected.length} Selected`}
        </span>
        <Icon name="chevronDown" size={14} className="text-gray-400" />
      </button>
      {isOpen && (
        <div className={"absolute top-full left-0 w-full mt-1 bg-white border border-gray-200 shadow-xl rounded-lg z-50 flex flex-col" + (wide ? " !max-w-2xl" : "") }>
          <div className="p-2 border-b bg-gray-50 rounded-t-lg">
            <input
              ref={searchRef}
              type="text"
              value={search}
              onChange={handleSearchChange}
              onKeyDown={handleKeyDown}
              placeholder="Search and press Enter..."
              className="w-full px-2 py-1.5 text-sm border border-gray-300 rounded focus:outline-none focus:ring-1 focus:ring-blue-400 focus:border-blue-400"
            />
            <div className="flex justify-between mt-2 gap-2">
              {debouncedSearch ? (
                <>
                  <button onClick={selectAllFiltered} className="text-xs text-blue-600 hover:text-blue-800 font-medium">
                    Select {filteredOptions.length}
                  </button>
                  {selected.length > 0 && (
                    <button onClick={addToFiltered} className="text-xs text-green-600 hover:text-green-800 font-medium">
                      + Add {filteredOptions.length}
                    </button>
                  )}
                  <button onClick={clearFiltered} className="text-xs text-red-500 hover:text-red-700 font-medium ml-auto">
                    Clear Matches
                  </button>
                </>
              ) : (
                <>
                  <button onClick={selectAllFiltered} className="text-xs text-blue-600 hover:text-blue-800 font-medium">
                    Select All
                  </button>
                  <button onClick={clearFiltered} className="text-xs text-red-500 hover:text-red-700 font-medium">
                    Clear
                  </button>
                </>
              )}
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
