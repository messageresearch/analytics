import { useMemo, useCallback, useRef, useState, useEffect, useTransition } from 'react'

/**
 * High-performance filter hook for large datasets (25K+ items)
 * Uses Set-based lookups and deferred updates for responsive UI
 */
export default function useOptimizedFilters(rawData, initialFilters = {}) {
  const [isPending, startTransition] = useTransition()
  
  // Store filter state
  const [filterState, setFilterState] = useState({
    churches: initialFilters.churches || [],
    speakers: initialFilters.speakers || [],
    titles: initialFilters.titles || [],
    years: initialFilters.years || [],
    types: initialFilters.types || [],
    langs: initialFilters.langs || []
  })
  
  // Debounce timer ref
  const debounceRef = useRef(null)
  
  // Pre-compute Sets for O(1) lookups
  const filterSets = useMemo(() => ({
    churches: new Set(filterState.churches),
    speakers: new Set(filterState.speakers),
    titles: new Set(filterState.titles),
    years: new Set(filterState.years),
    types: new Set(filterState.types),
    langs: new Set(filterState.langs)
  }), [filterState])
  
  // Optimized filter function using Set.has() instead of Array.includes()
  const filteredData = useMemo(() => {
    if (!rawData || !rawData.length) return []
    
    const { churches, speakers, titles, years, types, langs } = filterSets
    
    // Early exit if all filters are empty (show all)
    const hasChurchFilter = churches.size > 0
    const hasSpeakerFilter = speakers.size > 0
    const hasTitleFilter = titles.size > 0
    const hasYearFilter = years.size > 0
    const hasTypeFilter = types.size > 0
    const hasLangFilter = langs.size > 0
    
    // If no filters, return all data
    if (!hasChurchFilter && !hasSpeakerFilter && !hasTitleFilter && 
        !hasYearFilter && !hasTypeFilter && !hasLangFilter) {
      return rawData
    }
    
    // Use a single pass filter with Set.has() for O(1) lookups
    return rawData.filter(s => 
      (!hasChurchFilter || churches.has(s.church)) &&
      (!hasSpeakerFilter || speakers.has(s.speaker)) &&
      (!hasTitleFilter || titles.has(s.title)) &&
      (!hasYearFilter || years.has(s.year)) &&
      (!hasTypeFilter || types.has(s.type)) &&
      (!hasLangFilter || langs.has(s.language))
    )
  }, [rawData, filterSets])
  
  // Debounced filter update to prevent UI jank
  const updateFilter = useCallback((key, values, immediate = false) => {
    if (debounceRef.current) {
      clearTimeout(debounceRef.current)
    }
    
    const doUpdate = () => {
      startTransition(() => {
        setFilterState(prev => ({
          ...prev,
          [key]: values
        }))
      })
    }
    
    if (immediate) {
      doUpdate()
    } else {
      debounceRef.current = setTimeout(doUpdate, 100)
    }
  }, [])
  
  // Batch update multiple filters at once
  const updateFilters = useCallback((updates, immediate = false) => {
    if (debounceRef.current) {
      clearTimeout(debounceRef.current)
    }
    
    const doUpdate = () => {
      startTransition(() => {
        setFilterState(prev => ({
          ...prev,
          ...updates
        }))
      })
    }
    
    if (immediate) {
      doUpdate()
    } else {
      debounceRef.current = setTimeout(doUpdate, 100)
    }
  }, [])
  
  // Reset all filters
  const resetFilters = useCallback((defaults) => {
    startTransition(() => {
      setFilterState(defaults)
    })
  }, [])
  
  // Cleanup debounce on unmount
  useEffect(() => {
    return () => {
      if (debounceRef.current) {
        clearTimeout(debounceRef.current)
      }
    }
  }, [])
  
  return {
    filteredData,
    filterState,
    filterSets,
    updateFilter,
    updateFilters,
    resetFilters,
    isPending
  }
}

/**
 * Create indexed lookup maps for ultra-fast data retrieval
 */
export function createDataIndex(rawData) {
  const byChurch = new Map()
  const bySpeaker = new Map()
  const byYear = new Map()
  const byType = new Map()
  const byLang = new Map()
  const byId = new Map()
  
  for (const item of rawData) {
    // Index by ID
    byId.set(item.id, item)
    
    // Index by church
    if (!byChurch.has(item.church)) byChurch.set(item.church, [])
    byChurch.get(item.church).push(item)
    
    // Index by speaker
    if (item.speaker) {
      if (!bySpeaker.has(item.speaker)) bySpeaker.set(item.speaker, [])
      bySpeaker.get(item.speaker).push(item)
    }
    
    // Index by year
    if (!byYear.has(item.year)) byYear.set(item.year, [])
    byYear.get(item.year).push(item)
    
    // Index by type
    if (!byType.has(item.type)) byType.set(item.type, [])
    byType.get(item.type).push(item)
    
    // Index by language
    if (!byLang.has(item.language)) byLang.set(item.language, [])
    byLang.get(item.language).push(item)
  }
  
  return { byChurch, bySpeaker, byYear, byType, byLang, byId }
}
