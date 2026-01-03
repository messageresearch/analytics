import React, { useState, useEffect, useMemo, useRef, useCallback, Component, useTransition, startTransition } from 'react'
import {
  ComposedChart, Line, Bar, BarChart, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, Area, Cell
} from 'recharts'
import JSZip from 'jszip'
import resampleData from './utils/resample'
import { getCachedChunks, cacheChunks, getCacheStats, isCacheValid, setCacheVersion, clearCache } from './utils/chunkCache'
import Icon from './components/Icon'
import staticChannels from '../channels.json'
import MultiSelect from './components/MultiSelect'
import VirtualizedTable from './components/VirtualizedTable'
import TopicAnalyzer from './components/TopicAnalyzerDefault'
import StatCard from './components/StatCard'
import SermonModal from './components/SermonModal'
import ChartModal from './components/ChartModal'
import ChannelChart from './components/ChannelChart'
// Channel preview modal removed — channel links open directly in a new tab
import useDebouncedCallback from './hooks/useDebouncedCallback'
import { DEFAULT_TERM, DEFAULT_REGEX_STR, DEFAULT_VARIATIONS, WORDS_PER_MINUTE, CHART_POINT_THRESHOLD, getColor } from './constants_local'

// --- ERROR BOUNDARY ---
class ErrorBoundary extends Component {
  constructor(props) { super(props); this.state = { hasError: false, error: null }; }
  static getDerivedStateFromError(error) { return { hasError: true, error }; }
  componentDidCatch(error, errorInfo) { console.error('Uncaught error:', error, errorInfo); }
  render() {
    if (this.state.hasError) {
      return (
        <div className="min-h-screen flex items-center justify-center bg-red-50 p-4">
          <div className="bg-white p-8 rounded-xl shadow-xl border border-red-200 max-w-lg w-full text-center">
            <h2 className="text-2xl font-bold text-red-600 mb-4">Application Error</h2>
            <p className="text-gray-600 mb-4">A critical error stopped the dashboard.</p>
            <div className="text-left bg-gray-100 p-2 rounded text-xs text-red-800 font-mono mb-4 overflow-auto max-h-32">
              {this.state.error && this.state.error.toString()}
            </div>
            <button onClick={() => window.location.reload()} className="bg-red-600 text-white font-bold py-2 px-6 rounded hover:bg-red-700 transition">Reload Page</button>
          </div>
        </div>
      )
    }
    return this.props.children
  }
}

// (Icon, MultiSelect, TopicAnalyzer, StatCard, SermonModal, ChartModal, HeatmapDetails and constants
// are now imported from `src/components/*` and `src/constants.js`)

// resampleData is provided by src/utils/resample

// The corresponding UI components (MultiSelect, StatCard, TopicAnalyzer, HeatmapDetails,
// ChartModal, SnippetRow, and SermonModal) were moved to `src/components/*` and are
// imported at the top of this file. Inline duplicates removed to avoid redeclaration.

export default function App(){
  const [rawData, setRawData] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [apiPrefix, setApiPrefix] = useState('site_api/')
  const [totalChunks, setTotalChunks] = useState(0)
  const [totalSermons, setTotalSermons] = useState(0)
  const [dataDate, setDataDate] = useState(null)
  const [channels, setChannels] = useState([])

  // GLOBAL
  const [activeTerm, setActiveTerm] = useState(DEFAULT_TERM)
  const [activeRegex, setActiveRegex] = useState(DEFAULT_REGEX_STR)
  const [customCounts, setCustomCounts] = useState(null)
  const [matchedTerms, setMatchedTerms] = useState([]) // Track which terms were matched and their counts
  const [isAnalyzing, setIsAnalyzing] = useState(false)
  const [analysisProgress, setAnalysisProgress] = useState({ status: '', percent: 0 })
  const lastAnalysisRef = useRef({ term: null, regex: null })

  // FILTERS - use useTransition for non-blocking filter updates
  const [isPending, startFilterTransition] = useTransition()
  const [selChurches, setSelChurchesRaw] = useState([])
  const [selSpeakers, setSelSpeakersRaw] = useState([])
  const [selTitles, setSelTitlesRaw] = useState([])
  const [selYears, setSelYearsRaw] = useState([])
  const [selTypes, setSelTypesRaw] = useState([])
  const [selLangs, setSelLangsRaw] = useState([])
  
  // Deferred filter setters - keeps UI responsive during 26K item filter operations
  const setSelChurches = useCallback((v) => startFilterTransition(() => setSelChurchesRaw(v)), [])
  const setSelSpeakers = useCallback((v) => startFilterTransition(() => setSelSpeakersRaw(v)), [])
  const setSelTitles = useCallback((v) => startFilterTransition(() => setSelTitlesRaw(v)), [])
  const setSelYears = useCallback((v) => startFilterTransition(() => setSelYearsRaw(v)), [])
  const setSelTypes = useCallback((v) => startFilterTransition(() => setSelTypesRaw(v)), [])
  const setSelLangs = useCallback((v) => startFilterTransition(() => setSelLangsRaw(v)), [])
  
  const [rollingWeeks, setRollingWeeks] = useState(26)
  const [aggregateWindow, setAggregateWindow] = useState(26)
  const [view, setView] = useState('dashboard')
  const [expandedChart, setExpandedChart] = useState(null)
  const [selectedSermon, setSelectedSermon] = useState(null)
  const [selectedSermonFocus, setSelectedSermonFocus] = useState(0)
  const [channelSearch, setChannelSearch] = useState('')
  const [channelSort, setChannelSort] = useState('name_asc')
  // Pinned popup for main chart - click to freeze the tooltip
  const [mainChartPinnedBucket, setMainChartPinnedBucket] = useState(null)
  const [mainChartPinnedPosition, setMainChartPinnedPosition] = useState({ x: 0, y: 0 })
  const mainChartPinnedRef = useRef(null)
  // preview modal removed: clicking a channel now opens its URL directly

  // TABLE
  const [tableFilters, setTableFilters] = useState({ date:'', church:'', speaker:'', title:'', category:'', mentions:'', rate:'' })
  const [page, setPage] = useState(1)
  const [sortConfig, setSortConfig] = useState({ key: 'mentionCount', direction: 'desc' })
  const PAGE_SIZE = 50

  const [isZipping, setIsZipping] = useState(false)
  const [zipProgress, setZipProgress] = useState('')
  const [isExportingMaster, setIsExportingMaster] = useState(false)

  // Close main chart pinned popup when clicking outside
  useEffect(() => {
    if (!mainChartPinnedBucket) return
    const handleClickOutside = (e) => {
      if (mainChartPinnedRef.current && !mainChartPinnedRef.current.contains(e.target)) {
        setMainChartPinnedBucket(null)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [mainChartPinnedBucket])

  // When a custom analysis runs and `customCounts` is set, default the table
  // to sort by mentions (highest first) so the most relevant sermons surface.
  useEffect(() => {
    if(customCounts && typeof customCounts === 'object'){
      setSortConfig({ key: 'mentionCount', direction: 'desc' })
      setPage(1)
    }
  }, [customCounts])

  // NOTE: Auto-run removed - pre-computed defaultSearchTerms from metadata.json now used instead
  // This eliminates the sluggish initial load that occurred when scanning all chunks on startup

  useEffect(()=>{
    const init = async ()=>{
      try{
        // Try multiple paths for metadata.json with fallback to raw.githubusercontent
        const metaPaths = [
          'site_api/metadata.json',
          '/wmbmentions.github.io/site_api/metadata.json',
          'https://raw.githubusercontent.com/messageanalytics/wmbmentions.github.io/main/docs/site_api/metadata.json',
          'metadata.json'
        ]
        let res = null
        let prefix = 'site_api/'
        for (const path of metaPaths) {
          try {
            res = await fetch(path)
            if (res.ok) {
              prefix = path.includes('raw.githubusercontent') ? 'https://raw.githubusercontent.com/messageanalytics/wmbmentions.github.io/main/docs/site_api/' : (path.includes('/wmbmentions.github.io/') ? '/wmbmentions.github.io/site_api/' : 'site_api/')
              break
            }
          } catch (e) {}
        }
        if (!res || !res.ok) throw new Error('Metadata not found.')
        const json = await res.json()
        const dataVersion = json.generated || 'Unknown'
        setDataDate(dataVersion)
        
        // Set total sermon count early for loading screen
        if (json.totalSermons) setTotalSermons(json.totalSermons)
        
        // Auto-invalidate cache if data version changed
        const cacheValid = await isCacheValid(dataVersion)
        if (!cacheValid) {
          console.log('Data version changed, clearing chunk cache...')
          await clearCache()
          await setCacheVersion(dataVersion)
        }
        try{
          // If build-time static import exists, populate channels immediately so Data tab isn't empty during dev
          if(staticChannels && (!channels || channels.length === 0)){
            try{
              let cList = []
              if(Array.isArray(staticChannels)) cList = staticChannels
              else if(staticChannels && typeof staticChannels === 'object'){
                cList = Object.entries(staticChannels).map(([name, meta]) => ({ name, url: meta && (meta.url || meta.link || meta.href) || '', filename: meta && meta.filename }))
              }
              if(cList.length>0) setChannels(cList)
            }catch(e){ console.warn('Static channels import failed early', e) }
          }
          // Try a few paths so channels.json is found whether it's in site_api/ or at repo root
          const tries = [
            prefix + 'channels.json',
            '/wmbmentions.github.io/site_api/channels.json',
            '/wmbmentions.github.io/channels.json',
            'https://raw.githubusercontent.com/messageanalytics/wmbmentions.github.io/main/docs/site_api/channels.json',
            'https://raw.githubusercontent.com/messageanalytics/wmbmentions.github.io/main/channels.json',
            '/channels.json',
            'channels.json'
          ]
          let cRes = null
          let cData = null
          for(const p of tries){
            try{
              // console.debug('Trying channels path', p)
              const r = await fetch(p)
              if(r && r.ok){ cRes = r; break }
            }catch(e){}
          }
          if(cRes){
            cData = await cRes.json()
            let cList = []
            if(Array.isArray(cData)) cList = cData
            else if(cData && typeof cData === 'object'){
              cList = Object.entries(cData).map(([name, meta]) => ({ name, url: meta && (meta.url || meta.link || meta.href) || '', filename: meta && meta.filename }))
            }
            if(cList.length > 0) setChannels(cList)
          } else {
            console.warn('channels.json not found at tried paths')
          }
          // If no channels loaded via fetch, fall back to static import (build-time)
          if((!cData || (Array.isArray(cData) && cData.length===0) || (cData && typeof cData === 'object' && Object.keys(cData).length===0)) && staticChannels){
            try{
              let cList = []
              if(Array.isArray(staticChannels)) cList = staticChannels
              else if(staticChannels && typeof staticChannels === 'object'){
                cList = Object.entries(staticChannels).map(([name, meta]) => ({ name, url: meta && (meta.url || meta.link || meta.href) || '', filename: meta && meta.filename }))
              }
              if(cList.length>0) setChannels(cList)
            }catch(e){ console.warn('Static channels import failed', e) }
          }
        }catch(e){ console.warn('Using default channels', e) }
        const currentTimestamp = new Date().getTime()
        const list = (json.sermons || []).filter(s=>s.timestamp <= currentTimestamp).map(s=>{ const durationHrs = (s.wordCount / WORDS_PER_MINUTE) / 60; return { ...s, path: s.path, durationHrs: durationHrs>0?durationHrs:0.5, mentionsPerHour: durationHrs>0?parseFloat((s.mentionCount / durationHrs).toFixed(1)):0 } })
        setRawData(list); setTotalChunks(json.totalChunks || 0); setApiPrefix(prefix)
        // Use raw setters for initial load (no transition delay needed)
        setSelChurchesRaw([...new Set(list.map(s=>s.church))]);
        setSelSpeakersRaw([...new Set(list.map(s=>s.speaker))].filter(Boolean));
        setSelTitlesRaw([...new Set(list.map(s=>s.title))].filter(Boolean));
        const currentYear = new Date().getFullYear(); const years = [...new Set(list.map(s=>s.year))].filter(y=>parseInt(y) <= currentYear).sort().reverse(); const defaultYears = years.filter(y=>parseInt(y) >= 2020); setSelYearsRaw(defaultYears.length>0?defaultYears:years)
        const types = [...new Set(list.map(s=>s.type))]; setSelTypesRaw(types)
        const langs = [...new Set(list.map(s=>s.language))]; setSelLangsRaw(langs)  // Default: ALL languages selected
        
        // Load pre-computed default search terms if available (eliminates sluggish auto-run)
        if (json.defaultSearchTerms && Array.isArray(json.defaultSearchTerms)) {
          setMatchedTerms(json.defaultSearchTerms)
        }
        
        setLoading(false)
      }catch(e){ setError(e.message); setLoading(false) }
    }
    init()
  },[])

  const options = useMemo(()=>{ const getUnique = (k) => [...new Set(rawData.map(s=>s[k]))].filter(Boolean).sort(); return { churches: getUnique('church'), speakers: getUnique('speaker'), years: getUnique('year').reverse(), types: getUnique('type'), langs: getUnique('language'), titles: getUnique('title') } }, [rawData])

  // Build church aliases map from staticChannels (for legacy name search)
  const churchAliases = useMemo(() => {
    const aliases = {}
    if (staticChannels && typeof staticChannels === 'object') {
      for (const [name, meta] of Object.entries(staticChannels)) {
        if (meta && meta.legacyNames && Array.isArray(meta.legacyNames)) {
          aliases[name] = meta.legacyNames
        }
      }
    }
    return aliases
  }, [])

  // Optimize: only create new objects for items that have custom counts (reduces memory pressure on mobile)
  const enrichedData = useMemo(()=>{ 
    if(!customCounts || customCounts.size === 0) return rawData
    return rawData.map(s => { 
      const newCount = customCounts.get(s.id) || 0
      return { ...s, mentionCount: newCount, mentionsPerHour: s.durationHrs > 0 ? parseFloat((newCount / s.durationHrs).toFixed(1)) : 0, searchTerm: activeRegex } 
    }) 
  }, [rawData, customCounts, activeRegex])
  
  // PERFORMANCE: Pre-compute Sets for O(1) lookups instead of O(n) Array.includes()
  // This is critical for 26K+ items - reduces filter time from ~150ms to ~5ms
  const filterSets = useMemo(() => ({
    churches: new Set(selChurches),
    speakers: new Set(selSpeakers),
    titles: new Set(selTitles),
    years: new Set(selYears),
    types: new Set(selTypes),
    langs: new Set(selLangs)
  }), [selChurches, selSpeakers, selTitles, selYears, selTypes, selLangs])
  
  const filteredData = useMemo(()=> {
    const { churches, speakers, titles, years, types, langs } = filterSets
    return enrichedData.filter(s => 
      churches.has(s.church) && 
      speakers.has(s.speaker) && 
      titles.has(s.title) && 
      years.has(s.year) && 
      types.has(s.type) && 
      langs.has(s.language)
    )
  }, [enrichedData, filterSets])

  // Church coverage stats for horizontal bar chart (independent of filters)
  const [coverageShowPercent, setCoverageShowPercent] = useState(false)
  const [coverageExpanded, setCoverageExpanded] = useState(false)
  const [coverageTab, setCoverageTab] = useState('bars') // 'bars' or 'heatmap'
  const churchCoverageData = useMemo(() => {
    if (!rawData.length) return []
    const churchMap = new Map()
    for (const s of rawData) {
      if (!churchMap.has(s.church)) {
        churchMap.set(s.church, { church: s.church, count: 0, minDate: s.timestamp, maxDate: s.timestamp })
      }
      const entry = churchMap.get(s.church)
      entry.count++
      if (s.timestamp < entry.minDate) entry.minDate = s.timestamp
      if (s.timestamp > entry.maxDate) entry.maxDate = s.timestamp
    }
    const total = rawData.length
    const result = Array.from(churchMap.values()).map(c => ({
      ...c,
      percent: ((c.count / total) * 100).toFixed(1),
      dateRange: `${new Date(c.minDate).getFullYear()} - ${new Date(c.maxDate).getFullYear()}`
    }))
    result.sort((a, b) => b.count - a.count)
    return result
  }, [rawData])

  // Church × Year heatmap data
  const churchYearHeatmap = useMemo(() => {
    if (!rawData.length) return { churches: [], years: [], data: new Map(), maxCount: 0 }
    
    // Build church × year counts
    const heatmapMap = new Map() // "church|year" -> count
    const yearsSet = new Set()
    const churchCounts = new Map() // track total per church for sorting
    
    for (const s of rawData) {
      const year = new Date(s.timestamp).getFullYear()
      yearsSet.add(year)
      const key = `${s.church}|${year}`
      heatmapMap.set(key, (heatmapMap.get(key) || 0) + 1)
      churchCounts.set(s.church, (churchCounts.get(s.church) || 0) + 1)
    }
    
    // Sort churches by total count (descending), years ascending
    const churches = Array.from(churchCounts.entries())
      .sort((a, b) => b[1] - a[1])
      .map(([church]) => church)
    const years = Array.from(yearsSet).sort((a, b) => a - b)
    
    // Find max for color scaling
    let maxCount = 0
    for (const count of heatmapMap.values()) {
      if (count > maxCount) maxCount = count
    }
    
    return { churches, years, data: heatmapMap, maxCount }
  }, [rawData])

  // Speaker × Year heatmap data
  const speakerYearHeatmap = useMemo(() => {
    if (!rawData.length) return { speakers: [], years: [], data: new Map(), maxCount: 0, speakerTotals: new Map() }
    
    const heatmapMap = new Map() // "speaker|year" -> count
    const yearsSet = new Set()
    const speakerCounts = new Map()
    
    for (const s of rawData) {
      const speaker = s.speaker || 'Unknown'
      const year = new Date(s.timestamp).getFullYear()
      yearsSet.add(year)
      const key = `${speaker}|${year}`
      heatmapMap.set(key, (heatmapMap.get(key) || 0) + 1)
      speakerCounts.set(speaker, (speakerCounts.get(speaker) || 0) + 1)
    }
    
    // Sort speakers by total count (descending), years ascending
    const speakers = Array.from(speakerCounts.entries())
      .sort((a, b) => b[1] - a[1])
      .map(([speaker]) => speaker)
    const years = Array.from(yearsSet).sort((a, b) => a - b)
    
    let maxCount = 0
    for (const count of heatmapMap.values()) {
      if (count > maxCount) maxCount = count
    }
    
    return { speakers, years, data: heatmapMap, maxCount, speakerTotals: speakerCounts }
  }, [rawData])

  const dateDomain = useMemo(()=>{ if(selYears.length===0) return ['auto','auto']; const validYears = selYears.map(y=>parseInt(y)).filter(y=>!isNaN(y)); if(validYears.length===0) return ['auto','auto']; const minYear = Math.min(...validYears); const maxYear = Math.max(...validYears); return [new Date(minYear,0,1).getTime(), new Date(maxYear,11,31).getTime()] }, [selYears])

  // Detect mobile for memory-conscious processing
  const isMobile = useMemo(() => {
    if (typeof window === 'undefined') return false
    return /iPhone|iPad|iPod|Android/i.test(navigator.userAgent) || window.innerWidth < 768
  }, [])

  const handleAnalysis = async (term, variations, rawRegex = null, options = {}) => {
    // Allow analysis when either a term is provided or a raw regex is provided
    if((!term || !term.trim()) && (!rawRegex || !rawRegex.trim())) return
    lastAnalysisRef.current = { term, regex: rawRegex || null }
    setIsAnalyzing(true)
    setAnalysisProgress({ status: 'Preparing search...', percent: 0 })
    setMatchedTerms([]) // Clear previous matched terms
    // Run the main-thread scanner directly
    await scanOnMainThread(term, variations, rawRegex, { ...options, isMobile })
  }

  // Scan chunks on the main thread with IndexedDB caching
  const scanOnMainThread = async (term, variations, rawRegex = null, options = {}) => {
    const { wholeWords = true, isMobile = false } = options
    try{
      const BATCH = 50
      let regex
      // Skip detailed term counting on mobile to save memory
      const termCounts = isMobile ? null : Object.create(null)
      if(rawRegex && (''+rawRegex).trim()){
        try{ regex = new RegExp(rawRegex, 'gi') }catch(e){ setAnalysisProgress({ status: 'Invalid regex pattern', percent: 0, error: true }); setIsAnalyzing(false); return }
      } else {
        const vars = Array.isArray(variations) ? variations.map(v=>(''+v).trim()).filter(Boolean) : (''+(variations||'')).split(',').map(v=>v.trim()).filter(Boolean)
        const isRegexLike = (s) => /[\\\(\)\[\]\|\^\$\.\*\+\?]/.test(s)
        const escapeRe = (s) => (''+s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
        const patterns = [term, ...vars].map(t => isRegexLike(t) ? t : escapeRe(t))
        if(wholeWords){
          regex = new RegExp(`\\b(${patterns.join('|')})\\b`, 'gi')
        } else {
          regex = new RegExp(`(${patterns.join('|')})`, 'gi')
        }
      }
      const counts = Object.create(null)
      const total = totalChunks || 0
      if(!total || total === 0){ setAnalysisProgress({ status: 'No data available', percent: 0, error: true }); setIsAnalyzing(false); return }
      
      // Check IndexedDB cache first (skip on mobile for speed)
      let cachedData = new Map()
      let uncachedIndices = Array.from({ length: total }, (_, i) => i)
      
      if (!isMobile) {
        setAnalysisProgress({ status: 'Checking saved data...', percent: 5 })
        const allIndices = Array.from({ length: total }, (_, i) => i)
        cachedData = await getCachedChunks(allIndices)
        uncachedIndices = allIndices.filter(i => !cachedData.has(i))
        setAnalysisProgress({ status: cachedData.size === total ? 'All data cached! ⚡' : `${cachedData.size} of ${total} chunks ready`, percent: 10 })
        await new Promise(r => setTimeout(r, 50))
      }
      
      const cachedCount = cachedData.size
      let processedChunks = 0
      
      // Process cached chunks first (desktop only)
      if (cachedData.size > 0) {
        for (const [idx, chunk] of cachedData.entries()) {
          for(const item of chunk){
            try{
              const matchList = item.text ? item.text.match(regex) : null
              if(matchList && matchList.length > 0){
                counts[item.id] = (counts[item.id]||0) + matchList.length
                if (termCounts) {
                  for(const m of matchList){
                    const key = m.toLowerCase()
                    termCounts[key] = (termCounts[key] || 0) + 1
                  }
                }
              }
            }catch(e){}
          }
          processedChunks++
          if (processedChunks % 20 === 0) {
            const pct = Math.round((processedChunks/total)*100)
            setAnalysisProgress({ status: `Searching cached transcripts...`, percent: 10 + Math.round(pct * 0.4), detail: `${processedChunks.toLocaleString()} of ${total.toLocaleString()}` })
          }
        }
      }
      
      // Fetch and process uncached chunks
      if (uncachedIndices.length > 0) {
        setAnalysisProgress({ status: `Downloading ${uncachedIndices.length.toLocaleString()} transcript chunks...`, percent: 50, detail: 'First search takes longer' })
        // Skip caching on mobile to reduce memory pressure
        const newlyCached = isMobile ? null : new Map()
        
        for(let i=0; i<uncachedIndices.length; i+=BATCH){
          const batchIndices = uncachedIndices.slice(i, i+BATCH)
          const promises = batchIndices.map(j =>
            fetch(`${apiPrefix}text_chunk_${j}.json`)
              .then(r => r.ok ? r.json() : [])
              .then(data => ({ idx: j, data }))
              .catch(() => ({ idx: j, data: [] }))
          )
          const results = await Promise.all(promises)
          
          for(const { idx, data } of results){
            // Cache this chunk for next time (desktop only)
            if (newlyCached && data.length > 0) {
              newlyCached.set(idx, data)
            }
            for(const item of data){
              try{
                const matchList = item.text ? item.text.match(regex) : null
                if(matchList && matchList.length > 0){
                  counts[item.id] = (counts[item.id]||0) + matchList.length
                  if (termCounts) {
                    for(const m of matchList){
                      const key = m.toLowerCase()
                      termCounts[key] = (termCounts[key] || 0) + 1
                    }
                  }
                }
              }catch(e){}
            }
            processedChunks++
          }
          const pct = Math.round((processedChunks/total)*100)
          setAnalysisProgress({ status: 'Searching transcripts...', percent: 50 + Math.round(pct * 0.45), detail: `${processedChunks.toLocaleString()} of ${total.toLocaleString()}` })
          await new Promise(r => setTimeout(r, 5))
        }
        
        // Cache newly fetched chunks in background (desktop only)
        if (newlyCached && newlyCached.size > 0) {
          setTimeout(() => {
            cacheChunks(newlyCached).then(() => {
              console.log(`Cached ${newlyCached.size} chunks for faster future searches`)
            }).catch(e => console.warn('Cache failed:', e))
          }, 500)
        }
      }
      
      // Finalize results
      setAnalysisProgress({ status: 'Counting matches...', percent: 95 })
      
      // Build Map directly from counts object
      const map = new Map()
      for (const id in counts) {
        if (Object.hasOwn(counts, id)) {
          map.set(id, counts[id])
        }
      }
      
      setCustomCounts(map)
      setActiveTerm(term)
      setActiveRegex(rawRegex && rawRegex.trim() ? rawRegex : regex.source)
      
      // Process term counts (desktop only)
      let sortedTerms = []
      if (termCounts) {
        const termEntries = []
        for (const t in termCounts) {
          if (Object.hasOwn(termCounts, t)) {
            termEntries.push({ term: t, count: termCounts[t] })
          }
        }
        termEntries.sort((a, b) => b.count - a.count)
        sortedTerms = termEntries.slice(0, 50)
      }
      
      setMatchedTerms(sortedTerms)
      setIsAnalyzing(false)
      setAnalysisProgress({ status: cachedCount === total ? 'Done! (from cache ⚡)' : 'Done!', percent: 100 })
    }catch(e){ console.error('Main-thread scan failed', e); setIsAnalyzing(false); setAnalysisProgress({ status: 'Search failed', percent: 0, error: true }) }
  }

  const stats = useMemo(()=>{ if(!filteredData.length) return null; const total = filteredData.length; const mentions = filteredData.reduce((acc,s)=>acc+s.mentionCount,0); const max = Math.max(...filteredData.map(s=>s.mentionCount)); return { totalSermons: total, totalMentions: mentions, maxMentions: max, avg: (mentions / total).toFixed(1) } }, [filteredData])

  const aggregatedChartData = useMemo(()=>{
    if(!filteredData.length) return []
    const sorted = [...filteredData].sort((a,b)=>a.timestamp - b.timestamp)
    
    // Build buckets with sermons included for pinned popup
    const useMonth = sorted.length > 3000
    const buckets = {}
    sorted.forEach(item => {
      if(!item.timestamp || isNaN(item.timestamp)) return
      const date = new Date(item.timestamp)
      let key, ts
      if (useMonth) {
        key = `${date.getFullYear()}-${date.getMonth()}`
        ts = new Date(date.getFullYear(), date.getMonth(), 1).getTime()
      } else {
        const d = new Date(date)
        const day = d.getDay(), diff = d.getDate() - day + (day == 0 ? -6 : 1)
        const monday = new Date(d.setDate(diff))
        monday.setHours(0,0,0,0)
        key = monday.getTime()
        ts = key
      }
      if (!buckets[key]) buckets[key] = { timestamp: ts, mentionCount: 0, count: 0, sermons: [] }
      buckets[key].mentionCount += item.mentionCount || 0
      buckets[key].count++
      buckets[key].sermons.push(item)
    })
    
    const monthly = Object.values(buckets).sort((a,b)=>a.timestamp-b.timestamp)
    
    // Convert weeks to approximate months for rolling average window
    // 4 weeks ≈ 1 month, 12 weeks ≈ 3 months, 26 weeks ≈ 6 months
    const windowMonths = Math.max(1, Math.round(aggregateWindow / 4))
    return monthly.map((item, idx, arr)=>{ 
      let sum=0, count=0
      for(let i=idx; i>=Math.max(0, idx-(windowMonths-1)); i--){ 
        sum += arr[i].mentionCount
        count++ 
      } 
      return { ...item, rollingAvg: count>0?parseFloat((sum/count).toFixed(1)):0 } 
    })
  }, [filteredData, aggregateWindow, activeTerm])

  const channelTrends = useMemo(()=>{
    if(!filteredData.length) return []
    const charts = []
    const windowMs = rollingWeeks * 7 * 24 * 60 * 60 * 1000
    selChurches.forEach((church, index)=>{
      const churchSermons = filteredData.filter(s=>s.church===church).sort((a,b)=>a.timestamp - b.timestamp)
      if(churchSermons.length===0) return
      let left=0, sum=0, count=0
      const raw = churchSermons.map((sermon, right)=>{
        sum += sermon.mentionCount; count++
        while(sermon.timestamp - churchSermons[left].timestamp > windowMs){ sum -= churchSermons[left].mentionCount; count--; left++ }
        return { ...sermon, rollingAvg: count>0?parseFloat((sum/count).toFixed(1)):0 }
      })
      // Find channel URL from channels list
      const channelData = channels.find(c => c.name === church)
      const url = channelData && (channelData.url || channelData.link || channelData.href) || null
      // transcriptCounts computed by ChannelChart from raw data (no CSV dependency)
      charts.push({ church, raw, data: resampleData(raw, undefined, activeTerm), color: getColor(index), transcriptCounts: null, url })
    })
    // Sort charts alphabetically by church name
    charts.sort((a, b) => a.church.localeCompare(b.church))
    return charts
  }, [filteredData, selChurches, rollingWeeks, activeTerm, channels])

  const handleCustomDownload = async (dataToDownload) => {
    setIsZipping(true); setZipProgress('Initializing...'); const zip = new JSZip(); const folder = zip.folder('Archive')
    try{
      const neededIds = new Set(dataToDownload.map(s=>s.id)); const BATCH = 5
      for(let i=0;i<totalChunks;i+=BATCH){ setZipProgress(`Processing... ${(i/totalChunks*100).toFixed(0)}%`); const promises=[]; for(let j=i;j<Math.min(i+BATCH, totalChunks); j++){ promises.push(fetch(`${apiPrefix}text_chunk_${j}.json`).then(r=>r.ok?r.json():[])) } const results = await Promise.all(promises); results.forEach(chunk=>{ chunk.forEach(item=>{ if(neededIds.has(item.id)){ const meta = dataToDownload.find(f=>f.id===item.id); if(meta) folder.file(`${meta.date} - ${meta.church} - ${meta.title}`.replace(/[^a-z0-9 \-\.]/gi, '_') + '.txt', item.text) } }) }) }
      setZipProgress('Compressing...'); const content = await zip.generateAsync({ type: 'blob' }); const a = document.createElement('a'); a.href = window.URL.createObjectURL(content); a.download = `Sermon_Archive.zip`; a.click();
    }catch(e){ alert('Download failed') } finally { setIsZipping(false) }
  }

  const formatDate = (ts) => new Date(ts).toLocaleDateString(undefined, { month: 'short', year: 'numeric' })

  const processedTableData = useMemo(()=>{
    let data = [...filteredData]
    if(tableFilters.date) data = data.filter(s=>s.date.includes(tableFilters.date))
    if(tableFilters.church) data = data.filter(s=>s.church.toLowerCase().includes(tableFilters.church.toLowerCase()))
    if(tableFilters.speaker) data = data.filter(s=>s.speaker && s.speaker.toLowerCase().includes(tableFilters.speaker.toLowerCase()))
    if(tableFilters.title) data = data.filter(s=>s.title.toLowerCase().includes(tableFilters.title.toLowerCase()))
    if(tableFilters.category) data = data.filter(s=>s.type.toLowerCase().includes(tableFilters.category.toLowerCase()))
    if(tableFilters.mentions) data = data.filter(s=>s.mentionCount >= parseInt(tableFilters.mentions))
    if(tableFilters.rate) data = data.filter(s=>s.mentionsPerHour >= parseFloat(tableFilters.rate))
    if(sortConfig.key){ data.sort((a,b)=>{ let aVal=a[sortConfig.key], bVal=b[sortConfig.key]; if(typeof aVal === 'string') aVal = aVal.toLowerCase(); if(typeof bVal === 'string') bVal = bVal.toLowerCase(); if(aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1; if(aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1; return 0 }) }
    return data
  }, [filteredData, sortConfig, tableFilters])

  const displayedSermons = useMemo(()=> processedTableData.slice(0, page * PAGE_SIZE), [processedTableData, page])
  const handleSort = (key) => { let direction = 'asc'; if(sortConfig.key === key && sortConfig.direction === 'asc') direction = 'desc'; setSortConfig({ key, direction }) }
  const updateFilter = (key, val) => { setTableFilters(prev=> ({ ...prev, [key]: val })); setPage(1) }
  const debouncedUpdateFilter = useDebouncedCallback((k, v) => updateFilter(k, v), 250)

  useEffect(()=>{ return ()=>{ try{ if(workerRef.current){ workerRef.current.terminate(); workerRef.current = null } }catch(e){} } }, [])

  // Enhanced loading skeleton for faster perceived load
  if(loading) return (
    <div className="min-h-screen bg-gray-50">
      <div className="bg-white border-b shadow-sm">
        <div className="max-w-7xl mx-auto px-4 py-4 flex items-center gap-3">
          <div className="bg-blue-600 text-white p-2 rounded-lg animate-pulse w-10 h-10"></div>
          <div>
            <div className="h-5 w-48 bg-gray-200 rounded animate-pulse"></div>
            <div className="h-3 w-32 bg-gray-100 rounded animate-pulse mt-1"></div>
          </div>
        </div>
      </div>
      <div className="max-w-7xl mx-auto px-4 mt-8">
        <div className="bg-white p-6 rounded-xl border shadow-sm mb-8">
          <div className="h-6 w-32 bg-gray-200 rounded animate-pulse mb-4"></div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {[1,2,3,4].map(i => <div key={i} className="h-10 bg-gray-100 rounded animate-pulse"></div>)}
          </div>
        </div>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
          {[1,2,3,4].map(i => <div key={i} className="bg-white p-4 rounded-xl border h-20 animate-pulse"></div>)}
        </div>
        <div className="text-center text-gray-500 mt-12">
          <div className="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600 mb-3"></div>
          <p className="font-medium">Loading {totalSermons > 0 ? `${Math.floor(totalSermons / 1000).toLocaleString()},000+` : '25,000+'} transcripts...</p>
          <p className="text-xs text-gray-400 mt-1">Preparing search database</p>
        </div>
      </div>
    </div>
  )
  if(error) return <div className="h-screen flex items-center justify-center text-red-600 font-bold">{error}</div>

  return (
    <ErrorBoundary>
      <div className="min-h-screen pb-20 relative">
        <div className="bg-white border-b sticky top-0 z-30 shadow-sm">
          <div className="max-w-7xl mx-auto px-4 py-3 md:py-4 flex flex-col md:flex-row md:justify-between md:items-center gap-4 md:gap-3">
            <div className="flex items-center gap-3 min-w-0">
              <div className="bg-blue-600 text-white p-2 rounded-lg shadow-lg shadow-blue-200 flex-shrink-0"><Icon name="barChart" /></div>
              <div className="min-w-0 flex-1">
                <h1 className="font-bold text-base md:text-lg text-gray-900 leading-tight truncate">Message Analytics v3.0</h1>
                <p className="text-xs text-gray-500 truncate">Tracking: <span className="font-bold text-blue-700">{activeTerm}</span></p>
              </div>
            </div>
            <div className="flex flex-col md:flex-row items-stretch md:items-center gap-3 md:gap-4 w-full md:w-auto">
              {dataDate && <div className="text-xs text-gray-400 text-center md:text-right">Updated: {dataDate}</div>}
              <div className="flex bg-gray-100 p-1 rounded-lg w-full md:w-auto">{['dashboard','data'].map(tab => (<button key={tab} onClick={()=>setView(tab)} className={`px-3 md:px-4 py-1.5 rounded-md text-xs md:text-sm font-medium transition capitalize flex-1 md:flex-none ${view===tab ? 'bg-white text-blue-600 shadow-sm' : 'text-gray-500 hover:text-gray-900'}`}>{tab}</button>))}</div>
            </div>
          </div>
        </div>
        <div className="bg-yellow-50 border-b border-yellow-100 text-yellow-800 text-xs text-center py-2 px-4"><span className="font-bold">Disclaimer:</span> This data is based solely on available YouTube automated transcripts and may not represent all sermons preached during this timeframe.</div>

        {/* How It Works Overview */}
        <div className="bg-blue-50 border-b border-blue-100">
          <div className="max-w-7xl mx-auto px-4 py-3">
            <details className="text-sm">
              <summary className="cursor-pointer font-semibold text-blue-800 hover:text-blue-600 flex items-center gap-2">
                <Icon name="info" size={16} />
                <span>How does this site work? (Click to expand)</span>
              </summary>
              <div className="mt-3 text-blue-900 space-y-3 text-xs leading-relaxed">
                <p><strong>📺 Data Source:</strong> This site aggregates sermon transcripts from <strong>church YouTube channels</strong> — not individual speakers. We monitor specific church channels and download their video transcripts when available.</p>
                
                <div className="bg-blue-100 rounded-lg p-3 space-y-2">
                  <p><strong>🔍 Search Engine:</strong> By default, we search for "William Branham" (Brother Branham) mentions, but <strong>you can search for anything</strong>. The search is highly flexible to accommodate YouTube's AI-generated transcripts, which often contain spelling errors due to speaker accents, audio quality, or AI interpretation.</p>
                  
                  <p className="font-semibold mt-2">Search Fields:</p>
                  <ul className="list-disc list-inside space-y-1 ml-2">
                    <li><strong>Search Term:</strong> The main word or phrase you're looking for.</li>
                    <li><strong>Variations:</strong> Add alternate spellings or related terms (comma-separated). For example: "branham, branam, branum" — no regex knowledge needed!</li>
                    <li><strong>Regex Pattern:</strong> For advanced users — a regular expression that matches multiple variations at once. This is powerful for handling transcript errors. <a href="https://regex101.com/" target="_blank" rel="noopener noreferrer" className="text-blue-600 underline hover:text-blue-800">Learn regex at regex101.com ↗</a></li>
                    <li><strong>Whole Word Only:</strong> When ON (default), only matches complete words. Turn OFF for partial matches — but be careful: searching "art" will also find "heart", "start", "party", etc.</li>
                  </ul>
                  
                  <p className="mt-2"><strong>Why Regex?</strong> YouTube transcripts have many spelling variations. For "Brother Branham", we use:<br/>
                  <code className="bg-blue-200 px-1 rounded text-[10px] break-all">{'\\b(?:(?:brother\\s+william)|william|brother)\\s+br[aeiou]n[dh]*[aeiou]m\\b'}</code><br/>
                  This single pattern matches 250+ variations like "brother branham", "william branam", "brother branum", "william brandham", etc. You can test and visualize patterns at <a href="https://regex101.com/" target="_blank" rel="noopener noreferrer" className="text-blue-600 underline hover:text-blue-800">regex101.com ↗</a></p>
                </div>

                <p><strong>⚠️ Speaker Data Limitations:</strong> Speaker names are extracted from video titles/descriptions using automated detection. This data may be <strong>incomplete or inaccurate</strong> — many videos don't include speaker information, and our algorithm can't always detect it reliably.</p>
                
                <div className="bg-blue-100 rounded-lg p-3 space-y-2">
                  <p className="font-semibold">📈 Understanding the Charts:</p>
                  <ul className="list-disc list-inside space-y-1 ml-2">
                    <li><strong>Main Dashboard Chart:</strong> Shows aggregated data across all selected churches. Displays total mentions over time with sermon counts.</li>
                    <li><strong>Rolling Averages:</strong> The rolling average lines smooth out daily fluctuations to reveal trends. A rising rolling average indicates increasing mention frequency over time.</li>
                    <li><strong>Individual Church Charts:</strong> Each church has its own chart showing mentions and sermon counts. Click on a chart to expand it for more detail. Hover over data points to see exact values.</li>
                  </ul>
                </div>
                
                <div className="bg-blue-100 rounded-lg p-3 space-y-2">
                  <p className="font-semibold">📋 Data Views:</p>
                  <ul className="list-disc list-inside space-y-1 ml-2">
                    <li><strong>Dashboard Tab:</strong> Visual charts and statistics. Best for seeing trends and patterns at a glance.</li>
                    <li><strong>Data Tab:</strong> A searchable, sortable table of all sermons. You can filter, sort by any column, and click rows to view sermon details. Great for finding specific sermons or doing detailed analysis.</li>
                  </ul>
                </div>
                
                <div className="bg-blue-100 rounded-lg p-3 space-y-2">
                  <p className="font-semibold">📜 Transcript List:</p>
                  <ul className="list-disc list-inside space-y-1 ml-2">
                    <li><strong>Default View:</strong> The transcript list shows all sermons sorted by highest mention count first, with columns for date, title, church, speaker, mention count, and transcript availability.</li>
                    <li><strong>Sorting:</strong> Click any column header to sort by that column. Click again to reverse the sort order. An arrow (▲/▼) indicates the current sort direction.</li>
                    <li><strong>Column Resizing:</strong> Drag the border between column headers to resize columns to your preference.</li>
                    <li><strong>Row Selection:</strong> Click on any row to open a detailed modal showing the full sermon information, a direct link to the YouTube video, and (if available) the full transcript text with your search terms highlighted in yellow.</li>
                  </ul>
                </div>
                
                <p><strong>📊 Charts by Church:</strong> Each chart below represents a <strong>church channel</strong>, not a speaker. The data shows sermon activity and mention frequency over time for that church.</p>
              </div>
            </details>
          </div>
        </div>

        <main className="max-w-7xl mx-auto px-4 mt-8">
          <div className="bg-white p-6 rounded-xl border shadow-sm mb-8">
            <div className="flex justify-between items-center mb-4">
              <div>
                <h3 className="font-bold text-gray-800 flex items-center gap-2"><Icon name="filter" /> Filter Database</h3>
                <div className="text-xs text-gray-500 mt-1">Selected Churches: <span className="font-medium text-gray-700">{selChurches.length.toLocaleString()}</span> • Speakers: <span className="font-medium text-gray-700">{selSpeakers.length.toLocaleString()}</span> • Titles: <span className="font-medium text-gray-700">{selTitles.length.toLocaleString()}</span>{isPending && <span className="ml-2 text-blue-500">⟳</span>}</div>
              </div>
              <button onClick={()=>{ setSelChurchesRaw(options.churches); setSelSpeakersRaw(options.speakers); setSelTitlesRaw(options.titles); setSelYearsRaw(options.years); setSelTypesRaw(options.types); setSelLangsRaw(options.langs) }} className="text-xs text-blue-600 font-medium hover:underline">Reset All</button>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-4">
              <MultiSelect label="Churches" options={options.churches} selected={selChurches} onChange={setSelChurches} medium aliases={churchAliases} />
              <div>
                <MultiSelect label="Speakers" options={options.speakers} selected={selSpeakers} onChange={setSelSpeakers} wide />
                <div className="text-xs text-amber-600 mt-1 flex items-center gap-1" title="Speaker names are extracted from video titles/descriptions and may be incomplete or inaccurate">
                  <Icon name="warning" size={12} />
                  <span>Data may be incomplete</span>
                </div>
              </div>
              <div className="md:col-span-2">
                <MultiSelect label="Titles" options={options.titles} selected={selTitles} onChange={setSelTitles} wide />
              </div>
            </div>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <MultiSelect label="Years" options={options.years} selected={selYears} onChange={setSelYears} />
              <MultiSelect label="Categories" options={options.types} selected={selTypes} onChange={setSelTypes} />
              <MultiSelect label="Languages" options={options.langs} selected={selLangs} onChange={setSelLangs} />
            </div>
          </div>

          {/* Transcript Coverage by Church - shows ENTIRE database, ignores filters */}
          <div className={`bg-white p-6 rounded-xl border shadow-sm mb-8 ${!coverageExpanded ? 'hover:border-blue-300 transition-colors' : ''}`}>
            <div className="flex justify-between items-center">
              <div 
                className={`flex items-center gap-2 cursor-pointer select-none ${!coverageExpanded ? 'hover:text-blue-600' : ''}`} 
                onClick={() => setCoverageExpanded(!coverageExpanded)}
              >
                <div className={`p-1 rounded ${!coverageExpanded ? 'bg-blue-100 text-blue-600' : ''}`}>
                  <Icon name={coverageExpanded ? 'chevronDown' : 'chevronRight'} size={16} />
                </div>
                <h3 className="font-bold text-gray-800 flex items-center gap-2">
                  <Icon name="barChart" /> Transcript Coverage by Church
                </h3>
                <span className="text-xs text-gray-500 ml-2">({churchCoverageData.length} churches • {rawData.length.toLocaleString()} total transcripts)</span>
                {!coverageExpanded && <span className="text-xs text-blue-500 ml-2 font-medium">Click to expand</span>}
              </div>
              {coverageExpanded && (
                <div className="flex items-center gap-4">
                  {/* Tab selector */}
                  <div className="flex bg-gray-100 rounded-lg p-1">
                    <button onClick={() => setCoverageTab('bars')} className={`px-3 py-1 text-xs font-medium rounded ${coverageTab === 'bars' ? 'bg-white shadow text-blue-600' : 'text-gray-500 hover:text-gray-700'}`}>
                      Bar Chart
                    </button>
                    <button onClick={() => setCoverageTab('heatmap')} className={`px-3 py-1 text-xs font-medium rounded ${coverageTab === 'heatmap' ? 'bg-white shadow text-blue-600' : 'text-gray-500 hover:text-gray-700'}`}>
                      Church × Year
                    </button>
                    <button onClick={() => setCoverageTab('speakers')} className={`px-3 py-1 text-xs font-medium rounded ${coverageTab === 'speakers' ? 'bg-white shadow text-blue-600' : 'text-gray-500 hover:text-gray-700'}`}>
                      Speaker × Year
                    </button>
                  </div>
                  {/* Count/% toggle for bar chart only */}
                  {coverageTab === 'bars' && (
                    <>
                      <span className="text-xs text-gray-500">Show:</span>
                      <div className="flex bg-gray-100 rounded-lg p-1">
                        <button onClick={() => setCoverageShowPercent(false)} className={`px-2 py-1 text-xs font-medium rounded ${!coverageShowPercent ? 'bg-white shadow text-blue-600' : 'text-gray-500'}`}>Count</button>
                        <button onClick={() => setCoverageShowPercent(true)} className={`px-2 py-1 text-xs font-medium rounded ${coverageShowPercent ? 'bg-white shadow text-blue-600' : 'text-gray-500'}`}>% of Total</button>
                      </div>
                    </>
                  )}
                </div>
              )}
            </div>
            {coverageExpanded && (
              <p className="text-xs text-gray-500 mt-4 mb-3 bg-blue-50 p-2 rounded">
                📊 This {coverageTab === 'bars' ? 'chart' : 'heatmap'} shows the <strong>entire database</strong> — it is not affected by the filters above. 
                {coverageTab === 'bars' && ' Use it to understand overall data coverage before filtering.'}
                {coverageTab === 'heatmap' && ' Darker cells indicate more transcripts for that church/year combination.'}
                {coverageTab === 'speakers' && ' Darker cells indicate more transcripts for that speaker/year combination.'}
              </p>
            )}
            
            {/* Bar Chart Tab */}
            {coverageExpanded && coverageTab === 'bars' && churchCoverageData.length > 0 && (
              <div style={{ height: Math.max(400, churchCoverageData.length * 22) }}>
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={churchCoverageData} layout="vertical" margin={{ top: 5, right: 80, left: 10, bottom: 5 }} barSize={12}>
                    <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                    <XAxis type="number" tickFormatter={v => coverageShowPercent ? `${v}%` : v.toLocaleString()} />
                    <YAxis 
                      type="category" 
                      dataKey="church" 
                      width={280} 
                      tick={{ fontSize: 11 }} 
                      interval={0}
                    />
                    <Tooltip 
                      content={({ active, payload }) => {
                        if (active && payload && payload.length) {
                          const d = payload[0].payload
                          const isSelected = selChurches.includes(d.church)
                          return (
                            <div className="bg-white border rounded-lg shadow-lg p-3 text-sm">
                              <p className="font-bold text-gray-800">{d.church}</p>
                              <p className="text-blue-600">Transcripts: <span className="font-semibold">{d.count.toLocaleString()}</span></p>
                              <p className="text-gray-600">Coverage: {d.dateRange}</p>
                              <p className="text-gray-600">{d.percent}% of total transcripts</p>
                              {isSelected && <p className="text-green-600 text-xs mt-1">✓ Currently selected in filters</p>}
                            </div>
                          )
                        }
                        return null
                      }}
                    />
                    <Bar dataKey={coverageShowPercent ? 'percent' : 'count'} radius={[0, 4, 4, 0]}>
                      {churchCoverageData.map((entry, index) => {
                        // Gradient: darker blue (#1e40af) for more transcripts, lighter blue (#bfdbfe) for fewer
                        const ratio = 1 - (index / (churchCoverageData.length - 1 || 1))
                        const r = Math.round(191 + (30 - 191) * ratio)
                        const g = Math.round(219 + (64 - 219) * ratio)
                        const b = Math.round(254 + (175 - 254) * ratio)
                        const gradientColor = `rgb(${r},${g},${b})`
                        return (
                          <Cell 
                            key={entry.church} 
                            fill={gradientColor}
                            style={{ cursor: 'pointer' }}
                            onClick={() => {
                              if (selChurches.length === 1 && selChurches[0] === entry.church) {
                                setSelChurches(options.churches)
                              } else {
                                setSelChurches([entry.church])
                              }
                            }}
                          />
                        )
                      })}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}
            
            {/* Year Heatmap Tab */}
            {coverageExpanded && coverageTab === 'heatmap' && churchYearHeatmap.churches.length > 0 && (
              <div className="w-full">
                {/* CSS Grid heatmap - responsive columns */}
                <div 
                  className="grid gap-px bg-gray-200 border rounded overflow-hidden"
                  style={{ 
                    gridTemplateColumns: `minmax(140px, 200px) repeat(${churchYearHeatmap.years.length}, minmax(28px, 1fr)) 50px`
                  }}
                >
                  {/* Header row */}
                  <div className="bg-gray-100 p-2 text-xs font-semibold text-gray-700 sticky top-0">Church</div>
                  {churchYearHeatmap.years.map(year => (
                    <div key={year} className="bg-gray-100 p-1 text-xs font-semibold text-gray-600 text-center sticky top-0">
                      '{String(year).slice(-2)}
                    </div>
                  ))}
                  <div className="bg-gray-100 p-1 text-xs font-semibold text-gray-600 text-center sticky top-0">Total</div>
                  
                  {/* Church rows */}
                  {churchYearHeatmap.churches.map((church, churchIdx) => {
                    const churchTotal = churchCoverageData.find(c => c.church === church)?.count || 0
                    const isSelected = selChurches.length === 1 && selChurches[0] === church
                    return (
                      <React.Fragment key={church}>
                        {/* Church name cell */}
                        <div 
                          className={`p-1.5 text-xs text-gray-700 truncate cursor-pointer hover:bg-blue-50 transition-colors ${isSelected ? 'bg-blue-100 font-semibold' : 'bg-white'}`}
                          title={`${church} - Click to filter`}
                          onClick={() => {
                            if (isSelected) {
                              setSelChurches(options.churches)
                            } else {
                              setSelChurches([church])
                            }
                          }}
                        >
                          {church}
                        </div>
                        {/* Year cells */}
                        {churchYearHeatmap.years.map(year => {
                          const count = churchYearHeatmap.data.get(`${church}|${year}`) || 0
                          const intensity = count > 0 
                            ? Math.min(1, Math.log(count + 1) / Math.log(churchYearHeatmap.maxCount + 1))
                            : 0
                          const bgColor = count === 0 
                            ? '#ffffff' 
                            : `rgba(37, 99, 235, ${0.15 + intensity * 0.85})`
                          const textColor = intensity > 0.5 ? 'white' : (count > 0 ? '#1e40af' : '#d1d5db')
                          
                          return (
                            <div 
                              key={`${church}-${year}`}
                              className="p-0.5 text-xs text-center relative group cursor-pointer"
                              style={{ backgroundColor: bgColor, color: textColor }}
                              title={`${church} (${year}): ${count} transcript${count !== 1 ? 's' : ''}`}
                              onClick={() => {
                                if (isSelected) {
                                  setSelChurches(options.churches)
                                } else {
                                  setSelChurches([church])
                                }
                              }}
                            >
                              {count > 0 ? count : '·'}
                            </div>
                          )
                        })}
                        {/* Total cell */}
                        <div 
                          className={`p-0.5 text-xs text-center font-medium cursor-pointer hover:bg-blue-50 ${isSelected ? 'bg-blue-100' : 'bg-gray-50'}`}
                          onClick={() => {
                            if (isSelected) {
                              setSelChurches(options.churches)
                            } else {
                              setSelChurches([church])
                            }
                          }}
                        >
                          {churchTotal}
                        </div>
                      </React.Fragment>
                    )
                  })}
                  
                  {/* Year totals row */}
                  <div className="bg-gray-100 p-1.5 text-xs font-semibold text-gray-700">Totals</div>
                  {churchYearHeatmap.years.map(year => {
                    let yearTotal = 0
                    for (const church of churchYearHeatmap.churches) {
                      yearTotal += churchYearHeatmap.data.get(`${church}|${year}`) || 0
                    }
                    return (
                      <div key={`total-${year}`} className="bg-gray-100 p-0.5 text-xs text-center font-medium text-gray-700">
                        {yearTotal || ''}
                      </div>
                    )
                  })}
                  <div className="bg-gray-100 p-0.5 text-xs text-center font-bold text-gray-700">
                    {rawData.length.toLocaleString()}
                  </div>
                </div>
                
                {/* Scrollable container for many churches */}
                <div className="max-h-[600px] overflow-y-auto" style={{ display: 'none' }}></div>
              </div>
            )}
            
            {/* Speaker × Year Heatmap Tab */}
            {coverageExpanded && coverageTab === 'speakers' && speakerYearHeatmap.speakers.length > 0 && (
              <div className="w-full">
                <div 
                  className="grid gap-px bg-gray-200 border rounded overflow-hidden"
                  style={{ 
                    gridTemplateColumns: `minmax(120px, 180px) repeat(${speakerYearHeatmap.years.length}, minmax(28px, 1fr)) 50px`
                  }}
                >
                  {/* Header row */}
                  <div className="bg-gray-100 p-2 text-xs font-semibold text-gray-700 sticky top-0">Speaker</div>
                  {speakerYearHeatmap.years.map(year => (
                    <div key={year} className="bg-gray-100 p-1 text-xs font-semibold text-gray-600 text-center sticky top-0">
                      '{String(year).slice(-2)}
                    </div>
                  ))}
                  <div className="bg-gray-100 p-1 text-xs font-semibold text-gray-600 text-center sticky top-0">Total</div>
                  
                  {/* Speaker rows */}
                  {speakerYearHeatmap.speakers.map((speaker) => {
                    const speakerTotal = speakerYearHeatmap.speakerTotals.get(speaker) || 0
                    const isSelected = selSpeakers.length === 1 && selSpeakers[0] === speaker
                    return (
                      <React.Fragment key={speaker}>
                        {/* Speaker name cell */}
                        <div 
                          className={`p-1.5 text-xs text-gray-700 truncate cursor-pointer hover:bg-green-50 transition-colors ${isSelected ? 'bg-green-100 font-semibold' : 'bg-white'}`}
                          title={`${speaker} - Click to filter`}
                          onClick={() => {
                            if (isSelected) {
                              setSelSpeakers(options.speakers)
                            } else {
                              setSelSpeakers([speaker])
                            }
                          }}
                        >
                          {speaker}
                        </div>
                        {/* Year cells */}
                        {speakerYearHeatmap.years.map(year => {
                          const count = speakerYearHeatmap.data.get(`${speaker}|${year}`) || 0
                          const intensity = count > 0 
                            ? Math.min(1, Math.log(count + 1) / Math.log(speakerYearHeatmap.maxCount + 1))
                            : 0
                          // Green gradient for speakers
                          const bgColor = count === 0 
                            ? '#ffffff' 
                            : `rgba(22, 163, 74, ${0.15 + intensity * 0.85})`
                          const textColor = intensity > 0.5 ? 'white' : (count > 0 ? '#166534' : '#d1d5db')
                          
                          return (
                            <div 
                              key={`${speaker}-${year}`}
                              className="p-0.5 text-xs text-center relative group cursor-pointer"
                              style={{ backgroundColor: bgColor, color: textColor }}
                              title={`${speaker} (${year}): ${count} transcript${count !== 1 ? 's' : ''}`}
                              onClick={() => {
                                if (isSelected) {
                                  setSelSpeakers(options.speakers)
                                } else {
                                  setSelSpeakers([speaker])
                                }
                              }}
                            >
                              {count > 0 ? count : '·'}
                            </div>
                          )
                        })}
                        {/* Total cell */}
                        <div 
                          className={`p-0.5 text-xs text-center font-medium cursor-pointer hover:bg-green-50 ${isSelected ? 'bg-green-100' : 'bg-gray-50'}`}
                          onClick={() => {
                            if (isSelected) {
                              setSelSpeakers(options.speakers)
                            } else {
                              setSelSpeakers([speaker])
                            }
                          }}
                        >
                          {speakerTotal}
                        </div>
                      </React.Fragment>
                    )
                  })}
                  
                  {/* Year totals row */}
                  <div className="bg-gray-100 p-1.5 text-xs font-semibold text-gray-700">Totals</div>
                  {speakerYearHeatmap.years.map(year => {
                    let yearTotal = 0
                    for (const speaker of speakerYearHeatmap.speakers) {
                      yearTotal += speakerYearHeatmap.data.get(`${speaker}|${year}`) || 0
                    }
                    return (
                      <div key={`total-${year}`} className="bg-gray-100 p-0.5 text-xs text-center font-medium text-gray-700">
                        {yearTotal || ''}
                      </div>
                    )
                  })}
                  <div className="bg-gray-100 p-0.5 text-xs text-center font-bold text-gray-700">
                    {rawData.length.toLocaleString()}
                  </div>
                </div>
              </div>
            )}
            
            {coverageExpanded && coverageTab === 'bars' && (
              <p className="text-xs text-gray-500 mt-2">Click a bar to filter by that church. Darker bars = more transcripts.</p>
            )}
            {coverageExpanded && coverageTab === 'heatmap' && (
              <p className="text-xs text-gray-500 mt-2">Click any cell or church name to filter. Darker blue = more transcripts. '·' = no data for that year.</p>
            )}
            {coverageExpanded && coverageTab === 'speakers' && (
              <p className="text-xs text-gray-500 mt-2">Click any cell or speaker name to filter. Darker green = more transcripts. '·' = no data for that year.</p>
            )}
          </div>

          {view === 'dashboard' && stats && (
            <>
              <TopicAnalyzer onAnalyze={handleAnalysis} isAnalyzing={isAnalyzing} progress={analysisProgress} initialTerm={DEFAULT_TERM} initialVariations={DEFAULT_VARIATIONS} matchedTerms={matchedTerms} totalTranscripts={rawData.length} />
              <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-8">
                <StatCard title="Filtered Sermons" value={stats.totalSermons.toLocaleString()} icon="fileText" color="blue" sub={`of ${rawData.length.toLocaleString()} total`} />
                <StatCard title={`${activeTerm} Mentions`} value={stats.totalMentions.toLocaleString()} icon="users" color="green" sub="in filtered results" />
                <StatCard title="Avg Mentions" value={stats.avg} icon="barChart" color="indigo" sub="per sermon (filtered)" />
                <StatCard title="Peak Count" value={stats.maxMentions} icon="activity" color="purple" sub="single sermon max" />
                <StatCard title="Total Transcripts" value={rawData.length.toLocaleString()} icon="download" color="gray" sub="entire database" />
              </div>

              <div className="bg-white p-6 rounded-xl border shadow-sm h-[500px] mb-8 relative">
                <div className="flex justify-between items-center mb-6">
                  <div>
                    <h3 className="font-bold text-gray-800 flex items-center gap-2"><Icon name="activity" /> Detailed Activity & Trend ({activeTerm})</h3>
                    <p className="text-xs text-gray-500 mt-1">Area = Volume. Line = {aggregateWindow < 5 ? '1 Mo' : aggregateWindow < 20 ? '3 Mo' : '6 Mo'} Rolling Avg. Click a bar to browse sermons.</p>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-gray-500" title="Rolling average window for trend line smoothing">Trend Window:</span>
                    <div className="flex bg-gray-100 rounded-lg p-1" title="Shorter = more responsive, Longer = smoother trend">
                      {[{w:4,l:'1 Mo',t:'1-month rolling average'},{w:12,l:'3 Mo',t:'3-month rolling average'},{w:26,l:'6 Mo',t:'6-month rolling average'}].map(({w,l,t})=> 
                        <button key={w} onClick={()=>setAggregateWindow(w)} title={t} className={`px-2 py-1 text-xs font-medium rounded ${aggregateWindow===w ? 'bg-white shadow text-blue-600' : 'text-gray-500'}`}>{l}</button>
                      )}
                    </div>
                  </div>
                </div>
                <ResponsiveContainer width="100%" height="85%">
                  <ComposedChart 
                    data={aggregatedChartData}
                    onClick={(e) => {
                      if (e && e.activePayload && e.activePayload[0]) {
                        const bucket = aggregatedChartData.find(d => d.timestamp === e.activePayload[0].payload.timestamp)
                        if (bucket) {
                          setMainChartPinnedBucket(bucket)
                          setMainChartPinnedPosition({ x: e.chartX || 200, y: e.chartY || 100 })
                        }
                      }
                    }}
                  >
                    <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e5e7eb" />
                    <XAxis dataKey="timestamp" type="number" scale="time" domain={dateDomain} tickFormatter={formatDate} tick={{fontSize:10}} minTickGap={120} />
                    <YAxis tick={{fontSize:10}} />
                    <Tooltip 
                      content={({ active, payload }) => {
                        if (mainChartPinnedBucket) return null
                        if (!active || !payload || !payload.length) return null
                        const p = payload[0].payload
                        const useMonth = aggregatedChartData.length > 150
                        const dateLabel = useMonth 
                          ? new Date(p.timestamp).toLocaleDateString(undefined, {month:'long', year:'numeric'})
                          : `Week of ${new Date(p.timestamp).toLocaleDateString(undefined, {month:'short', day:'numeric'})} - ${new Date(p.timestamp + 6*24*60*60*1000).toLocaleDateString(undefined, {month:'short', day:'numeric', year:'numeric'})}`
                        return (
                          <div className="bg-white p-3 rounded-lg shadow-lg text-sm border border-gray-200">
                            <div className="font-bold text-gray-900">{dateLabel}</div>
                            <div className="text-xs text-gray-500 mt-1">{p.mentionCount} mentions • {p.sermons?.length || 0} sermon{(p.sermons?.length || 0) !== 1 ? 's' : ''}</div>
                            <div className="mt-2 text-blue-600 text-xs font-medium">Click to browse sermons →</div>
                          </div>
                        )
                      }}
                    />
                    <Legend wrapperStyle={{fontSize:'11px', paddingTop:'10px', marginBottom: '20px'}} />
                    <Area type="monotone" dataKey="mentionCount" name="Volume" stroke="#93c5fd" fill="#bfdbfe" fillOpacity={1} isAnimationActive={false} cursor="pointer" />
                    <Line type="monotone" dataKey="rollingAvg" name="Aggregate Rolling Avg" stroke="#1d4ed8" strokeWidth={3} dot={false} connectNulls={true} isAnimationActive={false} />
                  </ComposedChart>
                </ResponsiveContainer>

                {/* Pinned popup for main chart */}
                {mainChartPinnedBucket && (
                  <div 
                    ref={mainChartPinnedRef}
                    className="fixed sm:absolute inset-x-2 sm:inset-x-auto bottom-2 sm:bottom-auto bg-white rounded-xl shadow-2xl border border-gray-200 sm:w-96 max-h-[60vh] sm:max-h-[400px] flex flex-col z-50"
                    style={{ 
                      left: typeof window !== 'undefined' && window.innerWidth >= 640 ? Math.min(mainChartPinnedPosition.x + 60, window.innerWidth - 450) : undefined, 
                      top: typeof window !== 'undefined' && window.innerWidth >= 640 ? Math.max(20, Math.min(mainChartPinnedPosition.y, 300)) : undefined
                    }}
                  >
                    <div className="p-4 border-b border-gray-100">
                      <div className="flex justify-between items-start">
                        <div>
                          <div className="font-bold text-gray-900">
                            {aggregatedChartData.length > 150 
                              ? new Date(mainChartPinnedBucket.timestamp).toLocaleDateString(undefined, {month:'long', year:'numeric'})
                              : `Week of ${new Date(mainChartPinnedBucket.timestamp).toLocaleDateString(undefined, {month:'short', day:'numeric'})} - ${new Date(mainChartPinnedBucket.timestamp + 6*24*60*60*1000).toLocaleDateString(undefined, {month:'short', day:'numeric', year:'numeric'})}`
                            }
                          </div>
                          <div className="text-sm text-gray-500 mt-1">{mainChartPinnedBucket.sermons?.length || 0} sermon{(mainChartPinnedBucket.sermons?.length || 0) !== 1 ? 's' : ''}</div>
                        </div>
                        <button onClick={()=>setMainChartPinnedBucket(null)} className="p-1 hover:bg-gray-100 rounded-full"><Icon name="x" /></button>
                      </div>
                      <div className="flex gap-4 mt-3 text-sm">
                        <div className="text-center">
                          <div className="text-xl font-bold text-blue-600">{mainChartPinnedBucket.mentionCount}</div>
                          <div className="text-xs text-gray-500">mentions</div>
                        </div>
                        <div className="text-center">
                          <div className="text-xl font-bold text-gray-700">{mainChartPinnedBucket.sermons?.length ? (mainChartPinnedBucket.mentionCount / mainChartPinnedBucket.sermons.length).toFixed(1) : 0}</div>
                          <div className="text-xs text-gray-500">avg/sermon</div>
                        </div>
                        <div className="text-center">
                          <div className="text-xl font-bold text-emerald-600">{mainChartPinnedBucket.rollingAvg || 'n/a'}</div>
                          <div className="text-xs text-gray-500">trend</div>
                        </div>
                      </div>
                    </div>
                    <div className="text-xs font-medium text-gray-600 px-4 pt-3 pb-2">Click a sermon to view transcript:</div>
                    <div className="flex-1 overflow-auto px-3 pb-3">
                      <div className="space-y-2">
                        {[...(mainChartPinnedBucket.sermons || [])].sort((a,b) => (b.mentionCount || 0) - (a.mentionCount || 0)).map((s,i)=> (
                          <button 
                            key={s.id || i}
                            onClick={()=>{ setSelectedSermon(s); setSelectedSermonFocus(0); setMainChartPinnedBucket(null) }} 
                            className="w-full text-left p-3 bg-gray-50 rounded-lg border border-gray-200 hover:bg-blue-50 hover:border-blue-300 transition-all group"
                          >
                            <div className="font-medium text-gray-900 group-hover:text-blue-600 line-clamp-2">{s.title || 'Untitled'}</div>
                            <div className="flex justify-between items-center mt-2 text-xs">
                              <span className="text-gray-500">{s.church || 'Unknown'}</span>
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
              </div>

              {/* Channel breakdown */}
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-8">
                {channelTrends.map((c, idx) => (
                  <div key={c.church || idx} className="bg-white p-4 rounded-xl border shadow-sm">
                    <ChannelChart church={c.church} data={c.data} raw={c.raw} color={c.color} domain={dateDomain} transcriptCounts={c.transcriptCounts} onExpand={(payload)=>setExpandedChart(payload)} />
                  </div>
                ))}
              </div>
              <div className="bg-white rounded-xl border shadow-sm overflow-hidden mb-8 p-6">
                <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-2 mb-4">
                  <h3 className="font-bold text-gray-800">Transcript List ({processedTableData.length.toLocaleString()})</h3>
                  {(customCounts && (activeTerm || (lastAnalysisRef.current && lastAnalysisRef.current.term))) ? (
                    <div className="text-sm bg-blue-50 text-blue-700 px-3 py-1 rounded font-medium">Sorted by mentions for: <span className="font-semibold">{activeTerm || (lastAnalysisRef.current && lastAnalysisRef.current.term)}</span></div>
                  ) : (sortConfig && sortConfig.key === 'mentionCount' && sortConfig.direction === 'desc') ? (
                    <div className="text-sm bg-blue-50 text-blue-700 px-3 py-1 rounded font-medium">Sorted by mentions (high → low)</div>
                  ) : null}
                </div>
                  <VirtualizedTable
                  columns={[
                    { key: 'date', label: 'Date', width: '110px', filterKey: 'date', filterPlaceholder: 'YYYY-MM', render: (r) => r.date },
                    { key: 'church', label: 'Church', width: '160px', filterKey: 'church', render: (r) => (<span className="bg-gray-100 px-2 py-1 rounded text-xs font-semibold text-gray-600">{r.church}</span>) },
                    { key: 'speaker', label: 'Speaker', width: '140px', filterKey: 'speaker', render: (r) => (<span className="text-xs text-gray-600 truncate">{r.speaker || '—'}</span>) },
                    { key: 'title', label: 'Title', width: '2fr', filterKey: 'title', render: (r) => (<div className="font-medium text-gray-900 truncate">{r.title}</div>) },
                    { key: 'type', label: 'Type', width: '100px', filterKey: 'category', render: (r) => (<span className="bg-gray-50 px-2 py-1 rounded text-xs border">{r.type}</span>) },
                    { key: 'mentionCount', label: 'Mentions', width: '90px', filterKey: 'mentions', filterType: 'number', centered: true, render: (r) => (<div className={`text-center font-bold ${r.mentionCount===0 ? 'text-red-500' : 'text-blue-600'}`}>{r.mentionCount}</div>) },
                    { key: 'mentionsPerHour', label: 'Rate/Hr', width: '70px', filterKey: 'rate', filterType: 'number', centered: true, render: (r) => (<div className="text-center text-xs">{r.mentionsPerHour}</div>) },
                    { key: 'action', label: 'Download', width: '80px', centered: true, noTruncate: true, render: (r) => (<button onClick={(e)=>{ e.stopPropagation(); const a = document.createElement('a'); a.href = r.path; a.download = `${r.date} - ${r.title}.txt`; a.click(); }} className="text-gray-400 hover:text-blue-600 flex items-center justify-center w-full"><Icon name="download" size={18} /></button>) }
                  ]}
                  data={processedTableData}
                  rowHeight={64}
                  height={480}
                  sortConfig={sortConfig}
                  onSort={(k)=>handleSort(k)}
                  filters={tableFilters}
                  onFilterChange={(k,v)=>updateFilter(k,v)}
                  onRowClick={(row)=>{ setSelectedSermon({ ...row, searchTerm: activeRegex || activeTerm || (lastAnalysisRef.current && lastAnalysisRef.current.term) || null }); setSelectedSermonFocus(0); }}
                />
              </div>
            </>
          )}

          {view === 'data' && (
            <div className="grid grid-cols-1 gap-8">
              <div className="bg-white p-8 rounded-xl border shadow-sm">
                <h3 className="text-xl font-bold text-gray-900 mb-4 flex items-center gap-2"><Icon name="download" /> Download Data</h3>
                <div className="space-y-3">
                  <button onClick={()=>handleCustomDownload(rawData)} disabled={isZipping} className="w-full bg-blue-600 hover:bg-blue-700 text-white font-bold py-3 px-4 rounded-lg flex items-center justify-center gap-2 transition disabled:opacity-50">{isZipping ? <span>{zipProgress}</span> : <span>Download Full Archive (All Transcripts — {rawData.length.toLocaleString()} files)</span>}</button>
                  <button onClick={()=>handleCustomDownload(filteredData)} disabled={isZipping} className="w-full bg-gray-100 hover:bg-gray-200 text-gray-800 font-bold py-3 px-4 rounded-lg flex items-center justify-center gap-2 transition disabled:opacity-50">{isZipping ? <span>{zipProgress}</span> : <span>Download Transcripts For Filtered View ({filteredData.length.toLocaleString()} files)</span>}</button>
                </div>
              </div>
              <div className="bg-white p-8 rounded-xl border shadow-sm">
                <div className="flex justify-between items-start mb-4">
                  <div>
                    <h3 className="text-xl font-bold text-gray-900 whitespace-nowrap">Data Sources</h3>
                    <div className="text-sm text-gray-500 mt-1">{channels.length.toLocaleString()} Channels</div>
                  </div>
                  <div className="flex items-center gap-2">
                    <input value={channelSearch} onChange={(e)=>setChannelSearch(e.target.value)} placeholder="Search channels..." className="text-sm border rounded px-3 py-2" />
                    <select value={channelSort} onChange={(e)=>setChannelSort(e.target.value)} className="text-sm border rounded px-2 py-2">
                      <option value="name_asc">Name ↑</option>
                      <option value="name_desc">Name ↓</option>
                    </select>
                    <button onClick={()=>{
                      const rows = channels.map(c=>([c.name || '', c.url || '', c.filename || '']))
                      const header = ['name','url','filename']
                      const lines = [header].concat(rows).map(cols => cols.map(v => `"${(''+ (v||'')).replace(/"/g,'""')}"`).join(','))
                      // Use CRLF and BOM for better Excel compatibility
                      const csvText = '\uFEFF' + lines.join('\r\n')
                      const blob = new Blob([csvText], { type: 'text/csv;charset=utf-8;' })
                      const url = URL.createObjectURL(blob)
                      const a = document.createElement('a'); a.href = url; a.download = 'channels.csv'; a.click(); URL.revokeObjectURL(url)
                    }} className="text-sm bg-gray-100 hover:bg-gray-200 px-3 py-1.5 rounded">Export Channel List to CSV</button>
                    <button disabled={isExportingMaster} title="Export Master List with Transcript Metadata (Titles, Dates, Speakers, Youtube Links) to CSV" onClick={async ()=>{
                      setIsExportingMaster(true)
                      try{
                        const extractYoutubeFromText = (txt) => {
                          if(!txt) return ''
                          const ytMatch = txt.match(/https?:\/\/(?:www\.)?(?:youtube\.com\/watch\?v=[A-Za-z0-9_\-]+|youtu\.be\/[A-Za-z0-9_\-]+)/i)
                          if(ytMatch) return ytMatch[0]
                          const urlLine = txt.split(/\r?\n/).find(l => /youtube\.com\/watch|youtu\.be\//i.test(l))
                          if(urlLine){ const m = urlLine.match(/https?:\/\/(?:www\.)?\S+/i); return m?m[0]:'' }
                          const explicit = txt.split(/\r?\n/).map(l=>l.trim()).find(l=>/^URL:\s*/i.test(l))
                          if(explicit){ const m = explicit.match(/https?:\/\/(?:www\.)?\S+/i); return m?m[0]:'' }
                          return ''
                        }

                        const fetchVideoUrl = async (r) => {
                          if(r.url) return r.url
                          if(r.videoUrl) return r.videoUrl
                          if(!r.path) return ''
                          const candidates = [apiPrefix + r.path, '/' + r.path, r.path]
                          for(const p of candidates){
                            try{
                              const res = await fetch(p)
                              if(res && res.ok){
                                const text = await res.text()
                                const found = extractYoutubeFromText(text)
                                if(found) return found
                              }
                            }catch(e){}
                          }
                          return ''
                        }

                        const BATCH = 6
                        const rows = []
                        for(let i=0;i<rawData.length;i+=BATCH){
                          const batch = rawData.slice(i, i+BATCH)
                          const results = await Promise.all(batch.map(r=>fetchVideoUrl(r).then(url=>({ r, url })) ))
                          results.forEach(({ r, url })=>{
                            const videoUrl = url || ''
                            rows.push([r.id||'', r.date||'', r.church||'', r.title||'', r.speaker||'', r.type||'', r.language||'', videoUrl])
                          })
                        }

                        const header = ['id','date','church','title','speaker','type','language','videoUrl']
                        const lines = [header].concat(rows).map(cols => cols.map(v => `"${(''+(v||'')).replace(/"/g,'""')}"`).join(','))
                        const csvText = '\uFEFF' + lines.join('\r\n')
                        const blob = new Blob([csvText], { type: 'text/csv;charset=utf-8;' })
                        const url = URL.createObjectURL(blob)
                        const a = document.createElement('a'); a.href = url; a.download = 'master_sermons.csv'; a.click(); URL.revokeObjectURL(url)
                      }catch(e){ alert('Failed to build master CSV: ' + (e && e.message || e)) }finally{ setIsExportingMaster(false) }
                    }} className="text-sm bg-blue-50 hover:bg-blue-100 px-3 py-1.5 rounded">{isExportingMaster? 'Building CSV…' : 'Export Master List to CSV'}</button>
                  </div>
                </div>
                <ul className="space-y-2 pr-2">
                  {channels.length === 0 && <li className="text-sm text-gray-500">No channels loaded.</li>}
                  {(() => {
                    const q = channelSearch.trim().toLowerCase()
                    let list = channels.slice()
                    if(q) list = list.filter(c => (c.name || '').toLowerCase().includes(q) || (c.url || '').toLowerCase().includes(q) || (c.filename || '').toLowerCase().includes(q))
                    if(channelSort === 'name_asc') list.sort((a,b)=>( (a.name||'').localeCompare(b.name||'') ))
                    else list.sort((a,b)=>( (b.name||'').localeCompare(a.name||'') ))
                    return list.map((c,i)=>{
                      const name = c.name || c.channel || c.title || c.church || 'Unknown'
                      const href = c.url || c.link || c.href || '#'
                      return (
                        <li key={i}>
                          <div onClick={()=>{ const target = c.url || c.link || c.href || '#'; if(target && target !== '#'){ window.open(target, '_blank', 'noopener'); } else { alert('No channel URL available'); } }} className="channel-link-row block text-sm bg-gray-50 p-3 rounded border transition hover:bg-gray-100 cursor-pointer">
                            <div className="flex items-center justify-between gap-3">
                              <div className="min-w-0">
                                <div className="font-medium text-gray-700 truncate">{name}</div>
                                <div className="text-xs text-blue-600 truncate"><a href={href} target="_blank" rel="noopener noreferrer">{href}</a></div>
                              </div>
                              <Icon name="link" size={18} className="text-blue-400" />
                            </div>
                          </div>
                        </li>
                      )
                    })
                  })()}
                </ul>
                {/* channel preview modal removed — clicking opens channel URL in a new tab */}
              </div>
            </div>
          )}
        </main>
        {selectedSermon && <SermonModal sermon={selectedSermon} focusMatchIndex={selectedSermonFocus} onClose={()=>{ setSelectedSermon(null); setSelectedSermonFocus(0); }} />}
        {expandedChart && (()=>{
          // Re-resolve the chart from latest channelTrends so mention counts reflect current `customCounts`.
          // Normalize church names to avoid mismatch due to punctuation/casing/whitespace.
          const normalize = (v) => ('' + (v || '')).replace(/[^0-9A-Za-z]+/g, ' ').trim().toLowerCase()
          const target = normalize(expandedChart.church || expandedChart.name)
          const fresh = channelTrends && channelTrends.find(c => normalize(c.church) === target)
          const chartToShow = fresh ? { ...fresh, showRaw: expandedChart.showRaw || false } : expandedChart
          return (<ChartModal chart={chartToShow} domain={dateDomain} searchTerm={activeTerm || (lastAnalysisRef.current && lastAnalysisRef.current.term) || ''} onClose={()=>setExpandedChart(null)} onSelectSermon={(s, focusIndex)=>{ setExpandedChart(null); setSelectedSermon({ ...s, searchTerm: activeRegex || activeTerm || (lastAnalysisRef.current && lastAnalysisRef.current.term) || null }); setSelectedSermonFocus(focusIndex || 0); }} />)
        })()}
      </div>
    </ErrorBoundary>
  )
}
