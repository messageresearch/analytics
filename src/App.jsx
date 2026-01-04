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
import LazyChannelChart from './components/LazyChannelChart'
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
  const [chartMode, setChartMode] = useState('mentions') // 'mentions' or 'volume'
  const [view, setView] = useState('dashboard')
  const [expandedChart, setExpandedChart] = useState(null)
  const [selectedSermon, setSelectedSermon] = useState(null)
  const [selectedSermonFocus, setSelectedSermonFocus] = useState(0)
  const [channelSearch, setChannelSearch] = useState('')
  const [channelSort, setChannelSort] = useState('name_asc')
  const [expandedChannels, setExpandedChannels] = useState(new Set()) // Track which channels are expanded
  const [channelVideoSearch, setChannelVideoSearch] = useState({}) // Track search term per channel
  const [channelVideoSort, setChannelVideoSort] = useState({}) // Track sort config per channel { key, direction }
  const [channelTranscriptPreview, setChannelTranscriptPreview] = useState(null) // { video, content } for popup
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
  const [chartsCollapsed, setChartsCollapsed] = useState(true)

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
                cList = Object.entries(staticChannels).map(([name, meta]) => ({ name, url: meta && (meta.url || meta.link || meta.href || (meta.playlists && meta.playlists[0] && `https://www.youtube.com/playlist?list=${meta.playlists[0].id}`)) || '', filename: meta && meta.filename }))
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
              cList = Object.entries(cData).map(([name, meta]) => ({ name, url: meta && (meta.url || meta.link || meta.href || (meta.playlists && meta.playlists[0] && `https://www.youtube.com/playlist?list=${meta.playlists[0].id}`)) || '', filename: meta && meta.filename }))
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
                cList = Object.entries(staticChannels).map(([name, meta]) => ({ name, url: meta && (meta.url || meta.link || meta.href || (meta.playlists && meta.playlists[0] && `https://www.youtube.com/playlist?list=${meta.playlists[0].id}`)) || '', filename: meta && meta.filename }))
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
        const currentYear = new Date().getFullYear(); const years = [...new Set(list.map(s=>s.year))].filter(y=>parseInt(y) <= currentYear).sort().reverse(); const defaultYears = years.filter(y=>parseInt(y) >= 2010); setSelYearsRaw(defaultYears.length>0?defaultYears:years)
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

  // All unique options from raw data (used for reset and initial state)
  const options = useMemo(()=>{ const getUnique = (k) => [...new Set(rawData.map(s=>s[k]))].filter(Boolean).sort(); return { churches: getUnique('church'), speakers: getUnique('speaker'), years: getUnique('year').reverse(), types: getUnique('type'), langs: getUnique('language'), titles: getUnique('title') } }, [rawData])
  
  // SMART CASCADING FILTERS: Options adjust based on selected churches
  // Simpler cascade: Church → Speakers/Titles/Years/Types/Langs (all based only on church)
  // This prevents circular dependencies that empty out all filters
  const filteredOptions = useMemo(() => {
    const churchSet = new Set(selChurches)
    const speakerSet = new Set(selSpeakers)
    
    // All sermons from selected churches (the base filter for everything else)
    const churchFilteredSermons = rawData.filter(s => churchSet.has(s.church))
    
    // Speakers: only speakers who appear in selected churches
    const speakersFiltered = [...new Set(
      churchFilteredSermons.map(s => s.speaker)
    )].filter(Boolean).sort()
    
    // Titles: filtered by selected churches, and optionally by speakers if narrowed
    const titlesFiltered = [...new Set(
      churchFilteredSermons.filter(s => {
        // If speakers have been narrowed (not all selected), also filter titles by speaker
        if (selSpeakers.length > 0 && selSpeakers.length < speakersFiltered.length) {
          return speakerSet.has(s.speaker)
        }
        return true
      }).map(s => s.title)
    )].filter(Boolean).sort()
    
    // Years: only years that have data for selected churches
    const yearsFiltered = [...new Set(
      churchFilteredSermons.map(s => s.year)
    )].filter(Boolean).sort().reverse()
    
    // Categories: only categories that exist for selected churches
    const typesFiltered = [...new Set(
      churchFilteredSermons.map(s => s.type)
    )].filter(Boolean).sort()
    
    // Languages: only languages that exist for selected churches  
    const langsFiltered = [...new Set(
      churchFilteredSermons.map(s => s.language)
    )].filter(Boolean).sort()
    
    return {
      churches: options.churches, // Churches always show all (top-level filter)
      speakers: speakersFiltered,
      titles: titlesFiltered,
      years: yearsFiltered,
      types: typesFiltered,
      langs: langsFiltered
    }
  }, [rawData, options, selChurches, selSpeakers])

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

  // Compute transcript stats for the main dashboard
  const transcriptStats = useMemo(() => {
    const total = rawData.length
    const withTranscript = rawData.filter(s => s.hasTranscript === true || (s.hasTranscript === undefined && s.path)).length
    const withoutTranscript = total - withTranscript
    return { total, withTranscript, withoutTranscript }
  }, [rawData])

  // Filter to only items with transcripts (for display counts)
  const transcriptsOnly = useMemo(() => 
    rawData.filter(s => s.hasTranscript === true || (s.hasTranscript === undefined && s.path))
  , [rawData])

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
  const [speakerDisplayLimit, setSpeakerDisplayLimit] = useState(100)
  const churchCoverageData = useMemo(() => {
    if (!rawData.length) return []
    const churchMap = new Map()
    for (const s of rawData) {
      if (!churchMap.has(s.church)) {
        churchMap.set(s.church, { 
          church: s.church, 
          count: 0, 
          minDate: s.timestamp, 
          maxDate: s.timestamp,
          speakers: new Set(),
          unknownSpeakerCount: 0,
          categories: new Map()
        })
      }
      const entry = churchMap.get(s.church)
      entry.count++
      if (s.timestamp < entry.minDate) entry.minDate = s.timestamp
      if (s.timestamp > entry.maxDate) entry.maxDate = s.timestamp
      // Track speakers - "Unknown Speaker" is used for unidentified speakers
      const speaker = s.speaker || 'Unknown Speaker'
      if (speaker === 'Unknown Speaker') {
        entry.unknownSpeakerCount++
      } else {
        entry.speakers.add(speaker)
      }
      // Track categories
      const category = s.type || 'Unknown'
      entry.categories.set(category, (entry.categories.get(category) || 0) + 1)
    }
    const total = rawData.length
    const result = Array.from(churchMap.values()).map(c => ({
      church: c.church,
      count: c.count,
      minDate: c.minDate,
      maxDate: c.maxDate,
      percent: ((c.count / total) * 100).toFixed(1),
      dateRange: `${new Date(c.minDate).getFullYear()} - ${new Date(c.maxDate).getFullYear()}`,
      speakerCount: c.speakers.size,
      unknownSpeakerCount: c.unknownSpeakerCount,
      identifiedCount: c.count - c.unknownSpeakerCount,
      categories: Object.fromEntries(c.categories)
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
    lastAnalysisRef.current = { term, regex: rawRegex || null, wholeWords: options.wholeWords !== false }
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
      let requiredRegexes = null
      let excludedRegexes = null
      let searchType = null
      let proximityParams = null // For NEAR, SENTENCE, PARAGRAPH
      const termCounts = isMobile ? null : Object.create(null)
      
      // Helper: Parse boolean/proximity search syntax
      const parseBooleanSearch = (input) => {
        if (!input || typeof input !== 'string') return null
        const trimmed = input.trim()
        
        // ONEAR/n pattern (ordered): "term1 ONEAR/5 term2"
        const onearMatch = trimmed.match(/^(.+?)\s+ONEAR\/(\d+)\s+(.+)$/i)
        if (onearMatch) {
          return { type: 'onear', terms: [onearMatch[1].trim(), onearMatch[3].trim()], distance: parseInt(onearMatch[2], 10), ordered: true, excluded: [] }
        }
        
        // NEAR/n or AROUND(n) pattern: "term1 NEAR/5 term2" or "term1 ~5 term2" or "term1 AROUND(5) term2"
        const nearMatch = trimmed.match(/^(.+?)\s+(?:NEAR\/(\d+)|~(\d+)|AROUND\((\d+)\))\s+(.+)$/i)
        if (nearMatch) {
          return { type: 'near', terms: [nearMatch[1].trim(), nearMatch[5].trim()], distance: parseInt(nearMatch[2] || nearMatch[3] || nearMatch[4], 10), excluded: [] }
        }
        
        // SENTENCE pattern: "term1 /s term2"
        const sentenceMatch = trimmed.match(/^(.+?)\s+(?:\/s|SENTENCE)\s+(.+)$/i)
        if (sentenceMatch) {
          return { type: 'sentence', terms: [sentenceMatch[1].trim(), sentenceMatch[2].trim()], excluded: [] }
        }
        
        // PARAGRAPH pattern: "term1 /p term2"
        const paragraphMatch = trimmed.match(/^(.+?)\s+(?:\/p|PARAGRAPH)\s+(.+)$/i)
        if (paragraphMatch) {
          return { type: 'paragraph', terms: [paragraphMatch[1].trim(), paragraphMatch[2].trim()], excluded: [] }
        }
        
        // Exact phrase in quotes
        const phraseMatch = trimmed.match(/^"([^"]+)"$/)
        if (phraseMatch) {
          return { type: 'phrase', phrase: phraseMatch[1], excluded: [] }
        }
        
        // Lookahead patterns like (?=.*\bterm1\b)(?=.*\bterm2\b).*
        if (trimmed.includes('(?=') && trimmed.includes('.*')) {
          const termMatches = [...trimmed.matchAll(/\(\?=\.\*(?:\\b)?(\w+)(?:\\b)?\)/g)]
          const terms = termMatches.map(m => m[1]).filter(Boolean)
          if (terms.length >= 2) return { type: 'and', required: terms, excluded: [] }
        }
        
        // Extract NOT terms first
        let excluded = []
        let remaining = trimmed
        
        if (/\sNOT\s/i.test(remaining)) {
          const parts = remaining.split(/\s+NOT\s+/i)
          remaining = parts[0].trim()
          excluded = parts.slice(1).map(t => t.trim()).filter(Boolean)
        }
        
        // Extract -term or !term
        const negativeTerms = [...remaining.matchAll(/(?:^|\s)[-!](\w+)/g)]
        if (negativeTerms.length > 0) {
          excluded = [...excluded, ...negativeTerms.map(m => m[1])]
          remaining = remaining.replace(/(?:^|\s)[-!]\w+/g, ' ').trim()
        }
        
        // Check for OR patterns
        if (/\sOR\s/i.test(remaining)) {
          const terms = remaining.split(/\s+OR\s+/i).map(t => t.trim()).filter(Boolean)
          if (terms.length >= 2 || excluded.length > 0) return { type: 'or', required: terms, excluded }
        }
        if (/\s*\|\s*/.test(remaining)) {
          const terms = remaining.split(/\s*\|\s*/).map(t => t.trim()).filter(Boolean)
          if (terms.length >= 2 || excluded.length > 0) return { type: 'or', required: terms, excluded }
        }
        
        // Check for AND patterns
        if (/\sAND\s/i.test(remaining)) {
          const terms = remaining.split(/\s+AND\s+/i).map(t => t.trim()).filter(Boolean)
          if (terms.length >= 2 || excluded.length > 0) return { type: 'and', required: terms, excluded }
        }
        if (/^\+\S/.test(remaining) && remaining.includes(' +')) {
          const terms = remaining.split(/\s+/).filter(t => t.startsWith('+')).map(t => t.slice(1).trim()).filter(Boolean)
          if (terms.length >= 2 || excluded.length > 0) return { type: 'and', required: terms, excluded }
        }
        if (/\s*&\s*/.test(remaining) && !remaining.includes('|')) {
          const terms = remaining.split(/\s*&\s*/).map(t => t.trim()).filter(Boolean)
          if (terms.length >= 2 || excluded.length > 0) return { type: 'and', required: terms, excluded }
        }
        
        // Single term with exclusions
        if (excluded.length > 0 && remaining.trim()) {
          return { type: 'and', required: [remaining.trim()], excluded }
        }
        
        return null
      }
      
      // Helper: Build regex for a single term (with wildcard support)
      const hasWildcard = (t) => /[*?]/.test(t)
      const wildcardToRegex = (pattern) => {
        let escaped = pattern.replace(/[.+^${}()|[\]\\]/g, '\\$&')
        escaped = escaped.replace(/\*/g, '\\S*').replace(/\?/g, '.')
        return escaped
      }
      const buildTermRegex = (t, whole = true) => {
        if (hasWildcard(t)) {
          const pattern = wildcardToRegex(t)
          return whole ? new RegExp(`\\b(${pattern})\\b`, 'gi') : new RegExp(`(${pattern})`, 'gi')
        }
        const escapeRe = (s) => (''+s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
        const isRegexLike = (s) => /[\\\(\)\[\]\|\^\$\.\*\+\?]/.test(s)
        const pattern = isRegexLike(t) ? t : escapeRe(t)
        return whole ? new RegExp(`\\b(${pattern})\\b`, 'gi') : new RegExp(`(${pattern})`, 'gi')
      }
      
      // Helper: Build pattern for a term (with wildcard support)
      const buildPattern = (t, whole = true) => {
        if (hasWildcard(t)) {
          const pattern = wildcardToRegex(t)
          return whole ? `\\b${pattern}\\b` : pattern
        }
        const escapeRe = (s) => (''+s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
        return whole ? `\\b${escapeRe(t)}\\b` : escapeRe(t)
      }
      
      // Helper: Find proximity matches (supports ordered for ONEAR)
      const findProximityMatches = (text, terms, type, distance = 5, ordered = false) => {
        const pattern1 = buildPattern(terms[0], wholeWords)
        const pattern2 = buildPattern(terms[1], wholeWords)
        const regex1 = new RegExp(pattern1, 'gi')
        const regex2 = new RegExp(pattern2, 'gi')
        
        if (type === 'near' || type === 'onear') {
          // Find positions of both terms
          const pos1 = [], pos2 = []
          let m
          while ((m = regex1.exec(text)) !== null) pos1.push({ index: m.index, term: m[0] })
          while ((m = regex2.exec(text)) !== null) pos2.push({ index: m.index, term: m[0] })
          if (pos1.length === 0 || pos2.length === 0) return []
          
          // Count words between positions
          const wordPattern = /\S+/g
          const words = []
          while ((m = wordPattern.exec(text)) !== null) words.push({ start: m.index, end: m.index + m[0].length })
          
          const matches = []
          for (const p1 of pos1) {
            for (const p2 of pos2) {
              // For ordered search (ONEAR), term1 must come before term2
              if (ordered && p1.index >= p2.index) continue
              
              const start = Math.min(p1.index, p2.index)
              const end = Math.max(p1.index, p2.index)
              const wordsBetween = words.filter(w => w.start > start && w.end < end).length
              if (wordsBetween <= distance) {
                matches.push({ index: start, term: p1.term })
              }
            }
          }
          return matches
        } else if (type === 'sentence') {
          const sentences = text.split(/[.!?]+\s+/)
          const matches = []
          let offset = 0
          for (const sentence of sentences) {
            regex1.lastIndex = 0; regex2.lastIndex = 0
            if (regex1.test(sentence) && (regex2.lastIndex = 0, regex2.test(sentence))) {
              regex1.lastIndex = 0
              const m = regex1.exec(sentence)
              if (m) matches.push({ index: offset + m.index, term: m[0] })
            }
            offset += sentence.length + 2
          }
          return matches
        } else if (type === 'paragraph') {
          const paragraphs = text.split(/\n\s*\n|\r\n\s*\r\n/)
          const matches = []
          let offset = 0
          for (const para of paragraphs) {
            regex1.lastIndex = 0; regex2.lastIndex = 0
            if (regex1.test(para) && (regex2.lastIndex = 0, regex2.test(para))) {
              regex1.lastIndex = 0
              const m = regex1.exec(para)
              if (m) matches.push({ index: offset + m.index, term: m[0] })
            }
            offset += para.length + 2
          }
          return matches
        }
        return []
      }
      
      // Check for boolean search - check term first, then rawRegex
      const searchInput = (term && parseBooleanSearch(term)) ? term : (rawRegex || '')
      const booleanSearch = parseBooleanSearch(searchInput)
      
      if (booleanSearch) {
        searchType = booleanSearch.type
        try {
          // Handle proximity searches
          if (searchType === 'near' || searchType === 'onear' || searchType === 'sentence' || searchType === 'paragraph') {
            proximityParams = booleanSearch
            const terms = booleanSearch.terms || []
            const patterns = terms.map(t => {
              if (hasWildcard(t)) return wildcardToRegex(t)
              return (''+t).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
            })
            const combinedPattern = patterns.join('|')
            regex = wholeWords 
              ? new RegExp(`\\b(${combinedPattern})\\b`, 'gi')
              : new RegExp(`(${combinedPattern})`, 'gi')
            
            let msg = ''
            if (searchType === 'near') msg = `NEAR/${booleanSearch.distance} search: "${terms[0]}" within ${booleanSearch.distance} words of "${terms[1]}"`
            else if (searchType === 'onear') msg = `ONEAR/${booleanSearch.distance} search: "${terms[0]}" within ${booleanSearch.distance} words BEFORE "${terms[1]}" (ordered)`
            else if (searchType === 'sentence') msg = `SENTENCE search: "${terms[0]}" and "${terms[1]}" in same sentence`
            else if (searchType === 'paragraph') msg = `PARAGRAPH search: "${terms[0]}" and "${terms[1]}" in same paragraph`
            setAnalysisProgress({ status: msg + '...', percent: 5 })
          }
          // Handle phrase search
          else if (searchType === 'phrase') {
            const phrase = booleanSearch.phrase
            const escapeRe = (s) => (''+s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
            regex = new RegExp(`(${escapeRe(phrase)})`, 'gi')
            setAnalysisProgress({ status: `Exact phrase: "${phrase}"...`, percent: 5 })
          }
          // Handle AND/OR/NOT
          else {
            const requiredTerms = booleanSearch.required || []
            if (requiredTerms.length > 0) {
              requiredRegexes = requiredTerms.map(t => buildTermRegex(t, wholeWords))
            }
            if (booleanSearch.excluded && booleanSearch.excluded.length > 0) {
              excludedRegexes = booleanSearch.excluded.map(t => buildTermRegex(t, wholeWords))
            }
            
            if (requiredTerms.length > 0) {
              const combinedPattern = requiredTerms.map(t => {
                const isRegexLike = (s) => /[\\\(\)\[\]\|\^\$\.\*\+\?]/.test(s)
                const escapeRe = (s) => (''+s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
                return isRegexLike(t) ? t : escapeRe(t)
              }).join('|')
              
              regex = wholeWords 
                ? new RegExp(`\\b(${combinedPattern})\\b`, 'gi')
                : new RegExp(`(${combinedPattern})`, 'gi')
            }
            
            let msg = searchType === 'and' 
              ? `AND search: finding texts with ALL ${requiredTerms.length} terms`
              : `OR search: finding texts with ANY of ${requiredTerms.length} terms`
            if (booleanSearch.excluded && booleanSearch.excluded.length > 0) msg += ` (excluding ${booleanSearch.excluded.length})`
            setAnalysisProgress({ status: msg + '...', percent: 5 })
          }
        } catch(e) {
          setAnalysisProgress({ status: 'Invalid boolean search terms', percent: 0, error: true })
          setIsAnalyzing(false)
          return
        }
      } else if(rawRegex && (''+rawRegex).trim()){
        try{ regex = new RegExp(rawRegex, 'gi') }catch(e){ setAnalysisProgress({ status: 'Invalid regex pattern', percent: 0, error: true }); setIsAnalyzing(false); return }
      } else {
        const vars = Array.isArray(variations) ? variations.map(v=>(''+v).trim()).filter(Boolean) : (''+(variations||'')).split(',').map(v=>v.trim()).filter(Boolean)
        const isRegexLike = (s) => /[\\\(\)\[\]\|\^\$\.\*\+\?]/.test(s)
        const escapeRe = (s) => (''+s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
        const patterns = [term, ...vars].filter(Boolean).map(t => isRegexLike(t) ? t : escapeRe(t))
        if (patterns.length > 0) {
          if(wholeWords){
            regex = new RegExp(`\\b(${patterns.join('|')})\\b`, 'gi')
          } else {
            regex = new RegExp(`(${patterns.join('|')})`, 'gi')
          }
        }
      }
      
      // Helper to process each item with boolean search support
      const processItem = (item) => {
        if (!item.text) return
        
        // Check excluded terms first (NOT)
        if (excludedRegexes) {
          const hasExcluded = excludedRegexes.some(r => { r.lastIndex = 0; return r.test(item.text) })
          if (hasExcluded) return
        }
        
        // Handle proximity searches
        if (proximityParams) {
          const terms = proximityParams.terms || []
          const ordered = proximityParams.ordered || false
          const matches = findProximityMatches(item.text, terms, searchType, proximityParams.distance, ordered)
          if (matches.length > 0) {
            counts[item.id] = (counts[item.id]||0) + matches.length
            if (termCounts) {
              for (const m of matches) {
                const key = m.term.toLowerCase()
                termCounts[key] = (termCounts[key] || 0) + 1
              }
            }
          }
          return
        }
        
        // Check required terms based on search type
        if (requiredRegexes && requiredRegexes.length > 0) {
          if (searchType === 'and') {
            const allPresent = requiredRegexes.every(r => { r.lastIndex = 0; return r.test(item.text) })
            if (!allPresent) return
          } else if (searchType === 'or') {
            const anyPresent = requiredRegexes.some(r => { r.lastIndex = 0; return r.test(item.text) })
            if (!anyPresent) return
          }
        }
        
        if (!regex) return
        try {
          const matchList = item.text.match(regex)
          if (matchList && matchList.length > 0) {
            counts[item.id] = (counts[item.id]||0) + matchList.length
            if (termCounts) {
              for(const m of matchList){
                const key = m.toLowerCase()
                termCounts[key] = (termCounts[key] || 0) + 1
              }
            }
          }
        } catch(e){}
      }
      
      const counts = Object.create(null)
      const total = totalChunks || 0
      if(!total || total === 0){ setAnalysisProgress({ status: 'No data available', percent: 0, error: true }); setIsAnalyzing(false); return }
      
      // Check IndexedDB cache first (skip on mobile for speed)
      let cachedData = new Map()
      let uncachedIndices = Array.from({ length: total }, (_, i) => i)
      
      if (!isMobile) {
        setAnalysisProgress({ status: 'Checking saved data...', percent: 5 })
        await new Promise(r => setTimeout(r, 100)) // Show checking message
        const allIndices = Array.from({ length: total }, (_, i) => i)
        cachedData = await getCachedChunks(allIndices)
        uncachedIndices = allIndices.filter(i => !cachedData.has(i))
        if (cachedData.size === total) {
          setAnalysisProgress({ status: 'All data cached! ⚡', percent: 10, detail: 'Fast search mode' })
        } else if (cachedData.size > 0) {
          setAnalysisProgress({ status: `Found ${cachedData.size.toLocaleString()} cached chunks`, percent: 10, detail: `${uncachedIndices.length.toLocaleString()} need downloading` })
        } else {
          setAnalysisProgress({ status: 'No cached data found', percent: 10, detail: 'Downloading chunks...' })
        }
        await new Promise(r => setTimeout(r, 150)) // Show cache status message
      }
      
      const cachedCount = cachedData.size
      let processedChunks = 0
      
      // Process cached chunks first (desktop only)
      if (cachedData.size > 0) {
        setAnalysisProgress({ status: 'Searching cached transcripts...', percent: 15, detail: `0 of ${cachedData.size.toLocaleString()} chunks` })
        await new Promise(r => setTimeout(r, 30)) // Let UI update
        
        const cachedEntries = Array.from(cachedData.entries())
        const CACHE_BATCH = 25 // Process in batches to show progress
        
        for (let i = 0; i < cachedEntries.length; i += CACHE_BATCH) {
          const batch = cachedEntries.slice(i, i + CACHE_BATCH)
          for (const [idx, chunk] of batch) {
            for(const item of chunk){
              processItem(item)
            }
            processedChunks++
          }
          
          // Update progress after each batch
          const basePercent = uncachedIndices.length > 0 ? 15 : 15
          const maxPercent = uncachedIndices.length > 0 ? 50 : 95
          const progressPercent = basePercent + Math.round((processedChunks/cachedData.size) * (maxPercent - basePercent))
          setAnalysisProgress({ 
            status: 'Searching cached transcripts...', 
            percent: progressPercent, 
            detail: `${processedChunks.toLocaleString()} of ${cachedData.size.toLocaleString()} chunks` 
          })
          
          // Yield to let React render the progress update
          await new Promise(r => setTimeout(r, 10))
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
              processItem(item)
            }
            processedChunks++
          }
          const pct = Math.round((processedChunks/total)*100)
          setAnalysisProgress({ status: 'Searching chunks...', percent: 50 + Math.round(pct * 0.45), detail: `${processedChunks.toLocaleString()} of ${total.toLocaleString()} chunks` })
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

  const stats = useMemo(()=>{ if(!filteredData.length) return null; const withTranscripts = filteredData.filter(s => s.hasTranscript !== false); const total = withTranscripts.length; const mentions = withTranscripts.reduce((acc,s)=>acc+s.mentionCount,0); let max = 0; let peakSermon = null; withTranscripts.forEach(s => { if(s.mentionCount > max) { max = s.mentionCount; peakSermon = s; } }); return { totalSermons: total, totalMentions: mentions, maxMentions: max, peakSermon, avg: total > 0 ? (mentions / total).toFixed(1) : '0' } }, [filteredData])
  
  // Church scrape stats for Data tab - count transcripts and find latest date per church
  const churchScrapeStats = useMemo(() => {
    if (!rawData.length) return {}
    const stats = {}
    rawData.forEach(s => {
      const church = s.church
      if (!stats[church]) {
        stats[church] = { count: 0, latestDate: null, latestTitle: '' }
      }
      // Only count items with transcripts for the count
      if (s.hasTranscript !== false) {
        stats[church].count++
      }
      // Skip 'Unknown Date' and empty dates when finding latest
      const isValidDate = s.date && s.date !== 'Unknown Date' && s.date !== 'Unknown' && /^\d{4}-\d{2}-\d{2}$/.test(s.date)
      if (isValidDate && (!stats[church].latestDate || s.date > stats[church].latestDate)) {
        stats[church].latestDate = s.date
        stats[church].latestTitle = s.title
      }
    })
    return stats
  }, [rawData])

  // Get all videos for a specific church (used in expandable channel rows)
  const getChannelVideos = useCallback((churchName) => {
    const normalizedName = churchName.replace(/\s+/g, '_')
    return rawData
      .filter(s => s.church === normalizedName || s.church === churchName)
      .sort((a, b) => (b.date || '').localeCompare(a.date || ''))
  }, [rawData])
  
  // Unfiltered stats for comparison - uses enrichedData to get current search results across ALL sermons
  const unfilteredStats = useMemo(()=>{ if(!enrichedData.length) return null; const mentions = enrichedData.reduce((acc,s)=>acc+s.mentionCount,0); return { totalMentions: mentions } }, [enrichedData])
  
  // Check if any filters are active
  const hasActiveFilters = useMemo(() => {
    return selChurches.length < options.churches.length ||
           selSpeakers.length < options.speakers.length ||
           selYears.length < options.years.length ||
           selTypes.length < options.types.length ||
           selLangs.length < options.langs.length
  }, [selChurches, selSpeakers, selYears, selTypes, selLangs, options])
  
  // Clear all filters function
  const clearAllFilters = useCallback(() => {
    setSelChurchesRaw(options.churches)
    setSelSpeakersRaw(options.speakers)
    setSelTitlesRaw(options.titles)
    setSelYearsRaw(options.years)
    setSelTypesRaw(options.types)
    setSelLangsRaw(options.langs)
  }, [options])

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
      // Calculate rolling averages for both mentions and volume
      let mentionSum=0, volumeSum=0, windowCount=0
      for(let i=idx; i>=Math.max(0, idx-(windowMonths-1)); i--){ 
        mentionSum += arr[i].mentionCount
        volumeSum += arr[i].count
        windowCount++ 
      } 
      return { 
        ...item, 
        rollingAvg: windowCount>0 ? parseFloat((mentionSum/windowCount).toFixed(1)) : 0,
        volumeRollingAvg: windowCount>0 ? parseFloat((volumeSum/windowCount).toFixed(1)) : 0
      } 
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
    let data = filteredData.filter(s => s.hasTranscript !== false)
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
          <p className="font-medium">Loading 30,000+ transcripts...</p>
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
              <div className="flex bg-gray-100 p-1 rounded-lg w-full md:w-auto">{['dashboard','data','about'].map(tab => (<button key={tab} onClick={()=>setView(tab)} className={`px-3 md:px-4 py-1.5 rounded-md text-xs md:text-sm font-medium transition capitalize flex-1 md:flex-none ${view===tab ? 'bg-white text-blue-600 shadow-sm' : 'text-gray-500 hover:text-gray-900'}`}>{tab}</button>))}</div>
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
                  
                  <p className="font-semibold mt-3">🔎 Boolean Search Operators:</p>
                  <ul className="list-disc list-inside space-y-1 ml-2">
                    <li><strong>AND</strong> — Both terms required: <code className="bg-blue-200 px-1 rounded text-[10px]">faith AND grace</code></li>
                    <li><strong>OR</strong> — Either term matches: <code className="bg-blue-200 px-1 rounded text-[10px]">prophet OR messenger</code></li>
                    <li><strong>NOT</strong> — Exclude a term: <code className="bg-blue-200 px-1 rounded text-[10px]">sex NOT adam</code></li>
                    <li><strong>"quotes"</strong> — Exact phrase: <code className="bg-blue-200 px-1 rounded text-[10px]">"seven seals"</code></li>
                    <li><strong>Wildcards</strong> — <code className="bg-blue-200 px-1 rounded text-[10px]">proph*</code> matches prophet, prophecy, prophetic</li>
                    <li><strong>NEAR/n</strong> — Words within n words: <code className="bg-blue-200 px-1 rounded text-[10px]">seven NEAR/5 seals</code></li>
                    <li><strong>SENTENCE</strong> — Both in same sentence: <code className="bg-blue-200 px-1 rounded text-[10px]">bride SENTENCE rapture</code></li>
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
          {view !== 'about' && (
          <div className="bg-white p-6 rounded-xl border shadow-sm mb-8">
            <div className="flex justify-between items-center mb-4">
              <div>
                <h3 className="font-bold text-gray-800 flex items-center gap-2"><Icon name="filter" /> Filter Database</h3>
                <div className="text-xs text-gray-500 mt-1">
                  Selected Churches: <span className="font-medium text-gray-700">{selChurches.length.toLocaleString()}</span> • 
                  Speakers: <span className="font-medium text-gray-700">{selSpeakers.filter(s => filteredOptions.speakers.includes(s)).length.toLocaleString()} of {filteredOptions.speakers.length.toLocaleString()} available</span> • 
                  Transcripts: <span className="font-medium text-gray-700">{filteredData.filter(s => s.hasTranscript !== false).length.toLocaleString()} of {transcriptStats.withTranscript.toLocaleString()}</span>
                  {isPending && <span className="ml-2 text-blue-500">⟳</span>}
                </div>
              </div>
              <button onClick={()=>{ setSelChurchesRaw(options.churches); setSelSpeakersRaw(options.speakers); setSelTitlesRaw(options.titles); setSelYearsRaw(options.years); setSelTypesRaw(options.types); setSelLangsRaw(options.langs) }} className="text-xs text-blue-600 font-medium hover:underline">Reset All</button>
            </div>
            {/* Smart filter hint - always visible */}
            <div className="text-xs text-blue-600 bg-blue-50 rounded px-2 py-1 mb-3 flex items-center gap-1">
              <Icon name="info" size={12} />
              <span>Smart filters: Options adjust based on selected churches</span>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-4">
              <MultiSelect label="Churches" options={options.churches} selected={selChurches} onChange={setSelChurches} medium aliases={churchAliases} />
              <div>
                <MultiSelect label="Speakers" options={filteredOptions.speakers} selected={selSpeakers} onChange={setSelSpeakers} wide allOptions={options.speakers} />
                <div className="text-xs text-amber-600 mt-1 flex items-center gap-1" title="Speaker names are extracted from video titles/descriptions and may be incomplete or inaccurate">
                  <Icon name="warning" size={12} />
                  <span>Data may be incomplete</span>
                </div>
              </div>
              <div className="md:col-span-2">
                <MultiSelect label="Video Titles" options={filteredOptions.titles} selected={selTitles} onChange={setSelTitles} wide allOptions={options.titles} />
              </div>
            </div>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <MultiSelect label={`Years${selYears.length > 0 ? ` (${Math.min(...selYears.map(y => parseInt(y)))}–${Math.max(...selYears.map(y => parseInt(y)))})` : ''}`} options={filteredOptions.years} selected={selYears} onChange={setSelYears} allOptions={options.years} />
              <MultiSelect label="Categories" options={filteredOptions.types} selected={selTypes} onChange={setSelTypes} allOptions={options.types} />
              <MultiSelect label="Languages" options={filteredOptions.langs} selected={selLangs} onChange={setSelLangs} allOptions={options.langs} />
            </div>
          </div>
          )}

          {view !== 'about' && (
          /* Transcript Coverage by Church - shows ENTIRE database, ignores filters */
          <div className={`bg-white p-4 sm:p-6 rounded-xl border shadow-sm mb-8 ${!coverageExpanded ? 'hover:border-blue-300 transition-colors' : ''}`}>
            <div className="flex flex-col gap-3">
              {/* Title row - always visible */}
              <div 
                className={`flex flex-wrap items-center gap-2 cursor-pointer select-none ${!coverageExpanded ? 'hover:text-blue-600' : ''}`} 
                onClick={() => setCoverageExpanded(!coverageExpanded)}
              >
                <div className={`p-1 rounded flex-shrink-0 ${!coverageExpanded ? 'bg-blue-100 text-blue-600' : ''}`}>
                  <Icon name={coverageExpanded ? 'chevronDown' : 'chevronRight'} size={16} />
                </div>
                <h3 className="font-bold text-gray-800 flex items-center gap-2 text-sm sm:text-base">
                  <Icon name="barChart" /> <span>Coverage by Church</span>
                </h3>
                <span className="text-xs text-gray-500 w-full sm:w-auto sm:ml-2">({churchCoverageData.length} churches • <span className="text-green-600">{transcriptStats.withTranscript.toLocaleString()} with transcripts</span> • <span className="text-red-500">{transcriptStats.withoutTranscript.toLocaleString()} without</span>)</span>
                {!coverageExpanded && <span className="text-xs text-blue-500 font-medium hidden sm:inline">Click to expand</span>}
              </div>
              
              {/* Controls row - only when expanded */}
              {coverageExpanded && (
                <div className="flex flex-wrap items-center gap-2 sm:gap-4">
                  {/* Tab selector */}
                  <div className="flex bg-gray-100 rounded-lg p-1">
                    <button onClick={() => setCoverageTab('bars')} className={`px-2 sm:px-3 py-1 text-xs font-medium rounded ${coverageTab === 'bars' ? 'bg-white shadow text-blue-600' : 'text-gray-500 hover:text-gray-700'}`}>
                      Bar Chart
                    </button>
                    <button onClick={() => setCoverageTab('heatmap')} className={`px-2 sm:px-3 py-1 text-xs font-medium rounded ${coverageTab === 'heatmap' ? 'bg-white shadow text-blue-600' : 'text-gray-500 hover:text-gray-700'}`}>
                      Church × Year
                    </button>
                    <button onClick={() => setCoverageTab('speakers')} className={`px-2 sm:px-3 py-1 text-xs font-medium rounded ${coverageTab === 'speakers' ? 'bg-white shadow text-blue-600' : 'text-gray-500 hover:text-gray-700'}`}>
                      Speaker × Year
                    </button>
                  </div>
                  {/* Count/% toggle for bar chart only */}
                  {coverageTab === 'bars' && (
                    <div className="flex items-center gap-2">
                      <span className="text-xs text-gray-500">Show:</span>
                      <div className="flex bg-gray-100 rounded-lg p-1">
                        <button onClick={() => setCoverageShowPercent(false)} className={`px-2 py-1 text-xs font-medium rounded ${!coverageShowPercent ? 'bg-white shadow text-blue-600' : 'text-gray-500'}`}>Count</button>
                        <button onClick={() => setCoverageShowPercent(true)} className={`px-2 py-1 text-xs font-medium rounded ${coverageShowPercent ? 'bg-white shadow text-blue-600' : 'text-gray-500'}`}>% of Total</button>
                      </div>
                    </div>
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
            {coverageExpanded && coverageTab === 'bars' && churchCoverageData.length > 0 && (() => {
              const maxCount = Math.max(...churchCoverageData.map(d => d.count))
              const maxTick = Math.ceil(maxCount / 250) * 250
              const ticks = []
              for (let i = 0; i <= maxTick; i += 250) ticks.push(i)
              return (
              <div className="-mx-4 sm:mx-0 px-2 sm:px-0">
                <p className="text-xs text-gray-500 mb-2 px-1">Click a bar to see more detailed data about that church's transcripts. Darker bars = more transcripts.</p>
                <div className="text-xs text-gray-600 font-medium text-center mb-1">{coverageShowPercent ? '% of Total Transcripts' : 'Number of Transcripts'}</div>
                <div style={{ height: Math.max(400, churchCoverageData.length * 22) }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={churchCoverageData} layout="vertical" margin={{ top: 20, right: 30, left: 5, bottom: 5 }} barSize={12}>
                      <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                      <XAxis 
                        type="number" 
                        tickFormatter={v => coverageShowPercent ? `${v}%` : v.toLocaleString()} 
                        orientation="top"
                        axisLine={false}
                        tickLine={false}
                        tick={{ fontSize: 9 }}
                        ticks={coverageShowPercent ? [0, 2, 4, 6, 8, 10] : ticks}
                        domain={coverageShowPercent ? [0, 10] : [0, maxTick]}
                      />
                      <XAxis 
                        type="number" 
                        tickFormatter={v => coverageShowPercent ? `${v}%` : v.toLocaleString()} 
                        xAxisId="bottom"
                        orientation="bottom"
                        tick={{ fontSize: 9 }}
                        ticks={coverageShowPercent ? [0, 2, 4, 6, 8, 10] : ticks}
                        domain={coverageShowPercent ? [0, 10] : [0, maxTick]}
                      />
                      <YAxis 
                        type="category" 
                        dataKey="church" 
                        width={typeof window !== 'undefined' && window.innerWidth < 640 ? 120 : 260} 
                        tick={{ fontSize: typeof window !== 'undefined' && window.innerWidth < 640 ? 8 : 11 }} 
                        interval={0}
                      />
                    <Tooltip 
                      content={({ active, payload }) => {
                        if (!active || !payload || !payload.length) return null
                        const d = payload[0].payload
                        const isSelected = selChurches.includes(d.church)
                        const categoryEntries = Object.entries(d.categories || {}).sort((a, b) => b[1] - a[1])
                        return (
                          <div className="bg-white border rounded-lg shadow-lg p-3 text-sm max-w-xs">
                            <p className="font-bold text-gray-800 mb-2">{d.church}</p>
                            <div className="space-y-1">
                              <p className="text-blue-600">📊 Transcripts: <span className="font-semibold">{d.count.toLocaleString()}</span> <span className="text-gray-400">({d.percent}%)</span></p>
                              <p className="text-gray-600">📅 Coverage: {d.dateRange}</p>
                              <p className="text-green-600">👤 Speakers: <span className="font-semibold">{d.speakerCount}</span> identified</p>
                              <p className="text-amber-600 text-xs ml-4">↳ {d.identifiedCount.toLocaleString()} transcripts with known speaker</p>
                              <p className="text-amber-600 text-xs ml-4">↳ {d.unknownSpeakerCount.toLocaleString()} transcripts with unknown speaker</p>
                              {categoryEntries.length > 0 && (
                                <div className="mt-2 pt-2 border-t">
                                  <p className="text-gray-700 font-medium text-xs">📁 Categories:</p>
                                  <div className="text-xs text-gray-600 ml-2">
                                    {categoryEntries.map(([cat, count]) => (
                                      <p key={cat}>{cat}: {count.toLocaleString()}</p>
                                    ))}
                                  </div>
                                </div>
                              )}
                            </div>
                          </div>
                        )
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
              </div>
              )
            })()}
            
            {/* Year Heatmap Tab */}
            {coverageExpanded && coverageTab === 'heatmap' && churchYearHeatmap.churches.length > 0 && (
              <div className="w-full -mx-4 sm:mx-0 px-1 sm:px-0">
                {/* CSS Grid heatmap - very compact on mobile */}
                <div 
                  className="grid border border-gray-300 rounded overflow-hidden"
                  style={{ 
                    gridTemplateColumns: `${typeof window !== 'undefined' && window.innerWidth < 640 ? 50 : 200}px repeat(${churchYearHeatmap.years.length}, 1fr) ${typeof window !== 'undefined' && window.innerWidth < 640 ? 32 : 50}px`,
                    fontSize: 'clamp(6px, 1.5vw, 12px)'
                  }}
                >
                  {/* Header row */}
                  <div className="bg-gray-100 p-0.5 sm:p-2 font-semibold text-gray-700 sticky left-0 z-10 border-b border-r border-gray-300">
                    <span className="hidden sm:inline">Church</span>
                    <span className="sm:hidden text-[5px]">Church / Year →</span>
                  </div>
                  {churchYearHeatmap.years.map(year => (
                    <div key={year} className="bg-gray-100 p-0 sm:p-0.5 font-semibold text-gray-600 text-center sticky top-0 border-b border-r border-gray-300">
                      <span className="hidden sm:inline">'{String(year).slice(-2)}</span>
                      <span className="sm:hidden text-[5px]">'{String(year).slice(-2)}</span>
                    </div>
                  ))}
                  <div className="bg-gray-100 p-0.5 font-semibold text-gray-600 text-center sticky top-0 border-b border-gray-300">Total</div>
                  
                  {/* Church rows */}
                  {churchYearHeatmap.churches.map((church, churchIdx) => {
                    const churchTotal = churchCoverageData.find(c => c.church === church)?.count || 0
                    const isSelected = selChurches.length === 1 && selChurches[0] === church
                    const isLastRow = churchIdx === churchYearHeatmap.churches.length - 1
                    return (
                      <React.Fragment key={church}>
                        {/* Church name cell */}
                        <div 
                          className={`p-0.5 sm:p-1.5 text-gray-700 cursor-pointer hover:bg-blue-50 transition-colors sticky left-0 z-10 text-[5px] sm:text-xs sm:truncate line-clamp-2 sm:line-clamp-none overflow-hidden border-r border-gray-300 ${!isLastRow ? 'border-b border-gray-200' : ''} ${isSelected ? 'bg-blue-100 font-semibold' : 'bg-white'}`}
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
                              className={`p-0 sm:p-0.5 text-center relative group cursor-pointer min-h-[14px] sm:min-h-0 border-r border-gray-200 ${!isLastRow ? 'border-b border-gray-200' : ''}`}
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
                              <span className="hidden sm:inline">{count > 0 ? count : '·'}</span>
                              <span className="sm:hidden text-[3px]">{count > 0 ? count : ''}</span>
                            </div>
                          )
                        })}
                        {/* Total cell */}
                        <div 
                          className={`px-1 py-0 sm:p-0.5 text-center font-medium cursor-pointer hover:bg-blue-50 whitespace-nowrap ${!isLastRow ? 'border-b border-gray-200' : ''} ${isSelected ? 'bg-blue-100' : 'bg-gray-50'}`}
                          onClick={() => {
                            if (isSelected) {
                              setSelChurches(options.churches)
                            } else {
                              setSelChurches([church])
                            }
                          }}
                        >
                          {churchTotal.toLocaleString()}
                        </div>
                      </React.Fragment>
                    )
                  })}
                  
                  {/* Year totals row */}
                  <div className="bg-gray-100 p-0.5 sm:p-1.5 font-semibold text-gray-700 border-t border-r border-gray-300">Totals</div>
                  {churchYearHeatmap.years.map(year => {
                    let yearTotal = 0
                    for (const church of churchYearHeatmap.churches) {
                      yearTotal += churchYearHeatmap.data.get(`${church}|${year}`) || 0
                    }
                    return (
                      <div key={`total-${year}`} className="bg-gray-100 p-0 sm:p-0.5 text-center font-medium text-gray-700 border-t border-r border-gray-300">
                        <span className="hidden sm:inline">{yearTotal || ''}</span>
                      </div>
                    )
                  })}
                  <div className="bg-gray-100 px-1 py-0 sm:p-0.5 text-center font-bold text-gray-700 whitespace-nowrap border-t border-gray-300">
                    {transcriptStats.withTranscript.toLocaleString()}
                  </div>
                </div>
                
                {/* Scrollable container for many churches */}
                <div className="max-h-[600px] overflow-y-auto" style={{ display: 'none' }}></div>
              </div>
            )}
            
            {/* Speaker × Year Heatmap Tab */}
            {coverageExpanded && coverageTab === 'speakers' && speakerYearHeatmap.speakers.length > 0 && (
              <div className="w-full -mx-4 sm:mx-0 px-1 sm:px-0">
                {/* Summary header */}
                <div className="flex flex-wrap items-center justify-between gap-2 mb-2 px-1">
                  <div className="text-xs sm:text-sm text-gray-600">
                    <span className="font-semibold text-green-600">{speakerYearHeatmap.speakers.length}</span> speakers · 
                    <span className="font-semibold">{transcriptStats.withTranscript.toLocaleString()}</span> total transcripts
                    {speakerYearHeatmap.speakers.length > speakerDisplayLimit && (
                      <span className="text-gray-400"> · Showing top {Math.min(speakerDisplayLimit, speakerYearHeatmap.speakers.length)}</span>
                    )}
                  </div>
                </div>
                <p className="text-[10px] sm:text-xs text-gray-500 italic mb-2 px-1">
                  Note: "Unknown" speakers appear when church channels don't include the speaker name in the video title or description, so our detection algorithm cannot identify them yet.
                </p>
                <div 
                  className="grid border border-gray-300 rounded overflow-hidden"
                  style={{ 
                    gridTemplateColumns: `${typeof window !== 'undefined' && window.innerWidth < 640 ? 50 : 180}px repeat(${speakerYearHeatmap.years.length}, 1fr) ${typeof window !== 'undefined' && window.innerWidth < 640 ? 32 : 50}px`,
                    fontSize: 'clamp(6px, 1.5vw, 12px)'
                  }}
                >
                  {/* Header row */}
                  <div className="bg-gray-100 p-0.5 sm:p-2 font-semibold text-gray-700 sticky left-0 z-10 border-b border-r border-gray-300">
                    <span className="hidden sm:inline">Speaker</span>
                    <span className="sm:hidden text-[5px]">Speaker / Year →</span>
                  </div>
                  {speakerYearHeatmap.years.map(year => (
                    <div key={year} className="bg-gray-100 p-0 sm:p-0.5 font-semibold text-gray-600 text-center sticky top-0 border-b border-r border-gray-300">
                      <span className="hidden sm:inline">'{String(year).slice(-2)}</span>
                      <span className="sm:hidden text-[5px]">'{String(year).slice(-2)}</span>
                    </div>
                  ))}
                  <div className="bg-gray-100 p-0.5 font-semibold text-gray-600 text-center sticky top-0 border-b border-gray-300">Total</div>
                  
                  {/* Speaker rows - limited display */}
                  {speakerYearHeatmap.speakers.slice(0, speakerDisplayLimit).map((speaker, speakerIdx) => {
                    const speakerTotal = speakerYearHeatmap.speakerTotals.get(speaker) || 0
                    const isSelected = selSpeakers.length === 1 && selSpeakers[0] === speaker
                    const isLastRow = speakerIdx === Math.min(speakerDisplayLimit, speakerYearHeatmap.speakers.length) - 1
                    return (
                      <React.Fragment key={speaker}>
                        {/* Speaker name cell */}
                        <div 
                          className={`p-0.5 sm:p-1.5 text-gray-700 cursor-pointer hover:bg-green-50 transition-colors sticky left-0 z-10 text-[5px] sm:text-xs sm:truncate line-clamp-2 sm:line-clamp-none overflow-hidden border-r border-gray-300 ${!isLastRow ? 'border-b border-gray-200' : ''} ${isSelected ? 'bg-green-100 font-semibold' : 'bg-white'}`}
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
                              className={`p-0 sm:p-0.5 text-center relative group cursor-pointer min-h-[14px] sm:min-h-0 border-r border-gray-200 ${!isLastRow ? 'border-b border-gray-200' : ''}`}
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
                              <span className="hidden sm:inline">{count > 0 ? count : '·'}</span>
                              <span className="sm:hidden text-[3px]">{count > 0 ? count : ''}</span>
                            </div>
                          )
                        })}
                        {/* Total cell */}
                        <div 
                          className={`px-1 py-0 sm:p-0.5 text-center font-medium cursor-pointer hover:bg-green-50 whitespace-nowrap ${!isLastRow ? 'border-b border-gray-200' : ''} ${isSelected ? 'bg-green-100' : 'bg-gray-50'}`}
                          onClick={() => {
                            if (isSelected) {
                              setSelSpeakers(options.speakers)
                            } else {
                              setSelSpeakers([speaker])
                            }
                          }}
                        >
                          {speakerTotal.toLocaleString()}
                        </div>
                      </React.Fragment>
                    )
                  })}
                  
                  {/* Year totals row */}
                  <div className="bg-gray-100 p-0.5 sm:p-1.5 font-semibold text-gray-700 border-t border-r border-gray-300">Totals</div>
                  {speakerYearHeatmap.years.map(year => {
                    let yearTotal = 0
                    for (const speaker of speakerYearHeatmap.speakers) {
                      yearTotal += speakerYearHeatmap.data.get(`${speaker}|${year}`) || 0
                    }
                    return (
                      <div key={`total-${year}`} className="bg-gray-100 p-0 sm:p-0.5 text-center font-medium text-gray-700 border-t border-r border-gray-300">
                        <span className="hidden sm:inline">{yearTotal || ''}</span>
                      </div>
                    )
                  })}
                  <div className="bg-gray-100 px-1 py-0 sm:p-0.5 text-center font-bold text-gray-700 whitespace-nowrap border-t border-gray-300">
                    {transcriptStats.withTranscript.toLocaleString()}
                  </div>
                </div>
                
                {/* Show more/less buttons */}
                {speakerYearHeatmap.speakers.length > 100 && (
                  <div className="flex flex-wrap items-center justify-center gap-2 mt-3">
                    {speakerDisplayLimit < speakerYearHeatmap.speakers.length && (
                      <button
                        onClick={() => setSpeakerDisplayLimit(prev => Math.min(prev + 100, speakerYearHeatmap.speakers.length))}
                        className="px-3 py-1.5 text-xs sm:text-sm bg-green-50 text-green-700 rounded-lg hover:bg-green-100 transition-colors font-medium"
                      >
                        Show {Math.min(100, speakerYearHeatmap.speakers.length - speakerDisplayLimit)} more speakers
                      </button>
                    )}
                    {speakerDisplayLimit > 100 && (
                      <button
                        onClick={() => setSpeakerDisplayLimit(100)}
                        className="px-3 py-1.5 text-xs sm:text-sm bg-gray-100 text-gray-600 rounded-lg hover:bg-gray-200 transition-colors font-medium"
                      >
                        Show top 100 only
                      </button>
                    )}
                    {speakerDisplayLimit < speakerYearHeatmap.speakers.length && (
                      <button
                        onClick={() => setSpeakerDisplayLimit(speakerYearHeatmap.speakers.length)}
                        className="px-3 py-1.5 text-xs sm:text-sm bg-gray-100 text-gray-600 rounded-lg hover:bg-gray-200 transition-colors font-medium"
                      >
                        Show all {speakerYearHeatmap.speakers.length}
                      </button>
                    )}
                  </div>
                )}
              </div>
            )}
            
            {coverageExpanded && coverageTab === 'heatmap' && (
              <p className="text-xs text-gray-500 mt-2">Click any cell or church name to filter. Darker blue = more transcripts. '·' = no data for that year.</p>
            )}
            {coverageExpanded && coverageTab === 'speakers' && (
              <p className="text-xs text-gray-500 mt-2">Click any cell or speaker name to filter. Darker green = more transcripts. '·' = no data for that year.</p>
            )}
          </div>
          )}

          {view === 'dashboard' && stats && (
            <>
              <TopicAnalyzer onAnalyze={handleAnalysis} isAnalyzing={isAnalyzing} progress={analysisProgress} initialTerm={DEFAULT_TERM} initialVariations={DEFAULT_VARIATIONS} matchedTerms={matchedTerms} totalTranscripts={transcriptStats.withTranscript} />
              <div className="grid grid-cols-3 md:grid-cols-4 xl:grid-cols-7 gap-1.5 sm:gap-2 mb-6">
                <StatCard title="Filtered Transcripts" value={stats.totalSermons.toLocaleString()} icon="fileText" color="blue" sub={`of ${transcriptStats.withTranscript.toLocaleString()} total`} />
                <StatCard title={`${activeTerm} Mentions`} value={stats.totalMentions.toLocaleString()} icon="users" color="green" sub="in filtered results" />
                {/* Only show hidden mentions card when filters are hiding results */}
                {hasActiveFilters && unfilteredStats && (unfilteredStats.totalMentions - stats.totalMentions) > 0 && (
                  <StatCard 
                    title="Hidden by Filters" 
                    value={`+${(unfilteredStats.totalMentions - stats.totalMentions).toLocaleString()}`} 
                    icon="eyeOff" 
                    color="amber" 
                    sub="Click to clear filters and reveal all results"
                    onClick={clearAllFilters}
                  />
                )}
                <StatCard title="Avg Mentions" value={stats.avg} icon="barChart" color="indigo" sub="per sermon (filtered)" />
                <StatCard 
                  title="Peak Count" 
                  value={stats.maxMentions} 
                  icon="activity" 
                  color="purple" 
                  sub={stats.peakSermon ? `Click to view transcript` : 'single sermon max'}
                  onClick={stats.peakSermon ? () => { setSelectedSermon({ ...stats.peakSermon, searchTerm: activeRegex || activeTerm || (lastAnalysisRef.current && lastAnalysisRef.current.term) || null, wholeWords: lastAnalysisRef.current?.wholeWords !== false }); setSelectedSermonFocus(0); } : undefined}
                />
                <StatCard title="Total Transcripts" value={transcriptStats.withTranscript.toLocaleString()} icon="download" color="gray" sub="searchable database" />
                <StatCard title="Videos w/o Transcripts" value={transcriptStats.withoutTranscript.toLocaleString()} icon="alertCircle" color="red" sub="transcripts not available" />
              </div>

              <div className="bg-white p-3 sm:p-6 rounded-xl border shadow-sm h-[350px] sm:h-[500px] mb-6 relative overflow-hidden">
                <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-2 sm:gap-3 mb-2 sm:mb-4">
                  <div>
                    <h3 className="font-bold text-gray-800 flex items-center gap-2 text-sm sm:text-base">
                      <Icon name="activity" /> 
                      {chartMode === 'mentions' ? `${activeTerm} Activity & Trend` : 'Transcript Volume Over Time'}
                    </h3>
                    <p className="text-[10px] sm:text-xs text-gray-500 mt-0.5 sm:mt-1 hidden sm:block">
                      {chartMode === 'mentions' 
                        ? `Based on filters + search. Line = ${aggregateWindow < 5 ? '1 Mo' : aggregateWindow < 20 ? '3 Mo' : '6 Mo'} rolling avg. Click a datapoint to browse.`
                        : `Based on filters only (ignores search). Line = ${aggregateWindow < 5 ? '1 Mo' : aggregateWindow < 20 ? '3 Mo' : '6 Mo'} rolling avg. Click a datapoint to browse.`
                      }
                    </p>
                  </div>
                  <div className="flex flex-wrap items-center gap-2 sm:gap-3">
                    <div className="flex bg-gray-100 rounded-lg p-0.5 sm:p-1" title="Toggle between search mentions and total transcript volume">
                      <button onClick={()=>setChartMode('mentions')} className={`px-1.5 sm:px-2 py-0.5 sm:py-1 text-[10px] sm:text-xs font-medium rounded transition-colors ${chartMode==='mentions' ? 'bg-white shadow text-blue-600' : 'text-gray-500 hover:text-gray-700'}`}>
                        Mentions
                      </button>
                      <button onClick={()=>setChartMode('volume')} className={`px-1.5 sm:px-2 py-0.5 sm:py-1 text-[10px] sm:text-xs font-medium rounded transition-colors ${chartMode==='volume' ? 'bg-white shadow text-emerald-600' : 'text-gray-500 hover:text-gray-700'}`}>
                        All Transcripts
                      </button>
                    </div>
                    <div className="flex items-center gap-1.5">
                      <span className="text-[10px] sm:text-xs text-gray-500 hidden sm:inline" title="Rolling average window for trend line smoothing">Avg Window:</span>
                      <div className="flex bg-gray-100 rounded-lg p-0.5 sm:p-1" title="Rolling average window: Shorter = more responsive trend line, Longer = smoother trend line">
                        {[{w:4,l:'1 Mo',t:'1-month rolling average'},{w:12,l:'3 Mo',t:'3-month rolling average'},{w:26,l:'6 Mo',t:'6-month rolling average'}].map(({w,l,t})=> 
                          <button key={w} onClick={()=>setAggregateWindow(w)} title={t} className={`px-1.5 sm:px-2 py-0.5 sm:py-1 text-[10px] sm:text-xs font-medium rounded ${aggregateWindow===w ? 'bg-white shadow text-blue-600' : 'text-gray-500'}`}>{l}</button>
                        )}
                      </div>
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
                    <YAxis 
                      tick={{fontSize:10}} 
                      label={{ 
                        value: chartMode === 'mentions' ? 'Mention Count' : 'Transcript Count', 
                        angle: -90, 
                        position: 'insideLeft', 
                        style: { textAnchor: 'middle', fontSize: 11, fill: '#6b7280' } 
                      }} 
                    />
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
                            <div className="text-xs text-gray-500 mt-1">
                              {chartMode === 'mentions' 
                                ? <>{p.mentionCount} mentions • {p.count} transcript{p.count !== 1 ? 's' : ''}</>
                                : <>{p.count} transcript{p.count !== 1 ? 's' : ''} • {p.mentionCount} mentions</>
                              }
                            </div>
                            <div className="mt-2 text-blue-600 text-xs font-medium">Click to browse sermons →</div>
                          </div>
                        )
                      }}
                    />
                    <Legend wrapperStyle={{fontSize:'11px', paddingTop:'8px'}} />
                    {chartMode === 'mentions' ? (
                      <>
                        <Area type="monotone" dataKey="mentionCount" name={`${activeTerm} Mentions`} stroke="#93c5fd" fill="#bfdbfe" fillOpacity={1} isAnimationActive={false} cursor="pointer" />
                        <Line type="monotone" dataKey="rollingAvg" name="Rolling Avg" stroke="#1d4ed8" strokeWidth={3} dot={false} connectNulls={true} isAnimationActive={false} />
                      </>
                    ) : (
                      <>
                        <Area type="monotone" dataKey="count" name="Transcripts" stroke="#6ee7b7" fill="#a7f3d0" fillOpacity={1} isAnimationActive={false} cursor="pointer" />
                        <Line type="monotone" dataKey="volumeRollingAvg" name="Rolling Avg" stroke="#059669" strokeWidth={3} dot={false} connectNulls={true} isAnimationActive={false} />
                      </>
                    )}
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
                        {chartMode === 'mentions' ? (
                          <>
                            <div className="text-center">
                              <div className="text-xl font-bold text-blue-600">{mainChartPinnedBucket.mentionCount}</div>
                              <div className="text-xs text-gray-500">mentions</div>
                            </div>
                            <div className="text-center">
                              <div className="text-xl font-bold text-gray-700">{mainChartPinnedBucket.count ? (mainChartPinnedBucket.mentionCount / mainChartPinnedBucket.count).toFixed(1) : 0}</div>
                              <div className="text-xs text-gray-500">avg/sermon</div>
                            </div>
                            <div className="text-center">
                              <div className="text-xl font-bold text-emerald-600">{mainChartPinnedBucket.rollingAvg || 'n/a'}</div>
                              <div className="text-xs text-gray-500">trend</div>
                            </div>
                          </>
                        ) : (
                          <>
                            <div className="text-center">
                              <div className="text-xl font-bold text-emerald-600">{mainChartPinnedBucket.count}</div>
                              <div className="text-xs text-gray-500">transcripts</div>
                            </div>
                            <div className="text-center">
                              <div className="text-xl font-bold text-blue-600">{mainChartPinnedBucket.mentionCount}</div>
                              <div className="text-xs text-gray-500">mentions</div>
                            </div>
                            <div className="text-center">
                              <div className="text-xl font-bold text-gray-700">{mainChartPinnedBucket.volumeRollingAvg || 'n/a'}</div>
                              <div className="text-xs text-gray-500">trend</div>
                            </div>
                          </>
                        )}
                      </div>
                    </div>
                    <div className="text-xs font-medium text-gray-600 px-4 pt-3 pb-2">Click a sermon to view transcript:</div>
                    <div className="flex-1 overflow-auto px-3 pb-3">
                      <div className="space-y-2">
                        {[...(mainChartPinnedBucket.sermons || [])].sort((a,b) => (b.mentionCount || 0) - (a.mentionCount || 0)).map((s,i)=> (
                          <button 
                            key={s.id || i}
                            onClick={()=>{ setSelectedSermon({ ...s, searchTerm: activeRegex || activeTerm || (lastAnalysisRef.current && lastAnalysisRef.current.term) || null, wholeWords: lastAnalysisRef.current?.wholeWords !== false }); setSelectedSermonFocus(0); setMainChartPinnedBucket(null) }} 
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
              <div className="bg-white rounded-xl border shadow-sm mb-8">
                <button 
                  onClick={() => setChartsCollapsed(!chartsCollapsed)}
                  className={`w-full flex items-center justify-between p-4 transition-colors ${chartsCollapsed ? 'bg-gradient-to-r from-blue-50 to-indigo-50 hover:from-blue-100 hover:to-indigo-100' : 'hover:bg-gray-50'}`}
                >
                  <div className="flex items-center gap-2">
                    <Icon name={chartsCollapsed ? 'chevronRight' : 'chevronDown'} size={18} className={chartsCollapsed ? 'text-blue-600' : 'text-gray-500'} />
                    <h3 className={`font-bold ${chartsCollapsed ? 'text-blue-800' : 'text-gray-800'}`}>Individual Church Charts</h3>
                    <span className="text-sm text-gray-500">({channelTrends.length} churches)</span>
                  </div>
                  <span className={`text-sm font-medium ${chartsCollapsed ? 'text-blue-600 bg-blue-100 px-3 py-1 rounded-full' : 'text-xs text-gray-400'}`}>
                    {chartsCollapsed ? '▶ Click to view charts' : 'Click to collapse'}
                  </span>
                </button>
                {!chartsCollapsed && (
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 p-4 pt-0">
                    {channelTrends.map((c, idx) => (
                      <div key={c.church || idx} className="bg-gray-50 p-4 rounded-xl border">
                        <LazyChannelChart church={c.church} data={c.data} raw={c.raw} color={c.color} domain={dateDomain} transcriptCounts={c.transcriptCounts} onExpand={(payload)=>setExpandedChart(payload)} />
                      </div>
                    ))}
                  </div>
                )}
              </div>
              <div className="bg-white rounded-xl border shadow-sm mb-8 p-4 sm:p-6 overflow-x-auto">
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
                    { key: 'church', label: 'Church', width: '160px', filterKey: 'church', hideOnMobile: true, render: (r) => (<span className="bg-gray-100 px-2 py-1 rounded text-xs font-semibold text-gray-600">{r.church}</span>) },
                    { key: 'speaker', label: 'Speaker', width: '140px', filterKey: 'speaker', hideOnMobile: true, render: (r) => (<span className="text-xs text-gray-600 truncate">{r.speaker || '—'}</span>) },
                    { key: 'title', label: 'Title', width: '2fr', filterKey: 'title', render: (r) => (<div className="font-medium text-gray-900 truncate">{r.title}</div>) },
                    { key: 'type', label: 'Type', width: '100px', filterKey: 'category', hideOnMobile: true, render: (r) => (<span className="bg-gray-50 px-2 py-1 rounded text-xs border">{r.type}</span>) },
                    { key: 'mentionCount', label: 'Mentions', width: '90px', filterKey: 'mentions', filterType: 'number', centered: true, render: (r) => (<div className={`text-center font-bold ${r.mentionCount===0 ? 'text-red-500' : 'text-blue-600'}`}>{r.mentionCount}</div>) },
                    { key: 'mentionsPerHour', label: 'Rate/Hr', width: '70px', filterKey: 'rate', filterType: 'number', centered: true, hideOnMobile: true, render: (r) => (<div className="text-center text-xs">{r.mentionsPerHour}</div>) },
                    { key: 'action', label: 'Download', width: '80px', centered: true, noTruncate: true, render: (r) => (<button onClick={(e)=>{ e.stopPropagation(); const a = document.createElement('a'); a.href = r.path; a.download = `${r.date} - ${r.title}.txt`; a.click(); }} className="text-gray-400 hover:text-blue-600 flex items-center justify-center w-full"><Icon name="download" size={18} /></button>) }
                  ]}
                  data={processedTableData}
                  rowHeight={64}
                  height={480}
                  sortConfig={sortConfig}
                  onSort={(k)=>handleSort(k)}
                  filters={tableFilters}
                  onFilterChange={(k,v)=>updateFilter(k,v)}
                  onRowClick={(row)=>{ setSelectedSermon({ ...row, searchTerm: activeRegex || activeTerm || (lastAnalysisRef.current && lastAnalysisRef.current.term) || null, wholeWords: lastAnalysisRef.current?.wholeWords !== false }); setSelectedSermonFocus(0); }}
                />
              </div>
            </>
          )}

          {view === 'data' && (
            <div className="grid grid-cols-1 gap-8">
              <div className="bg-white p-8 rounded-xl border shadow-sm">
                <h3 className="text-xl font-bold text-gray-900 mb-4 flex items-center gap-2"><Icon name="download" /> Download Data</h3>
                <div className="space-y-3">
                  <button onClick={()=>handleCustomDownload(rawData.filter(s => s.hasTranscript !== false))} disabled={isZipping} className="w-full bg-blue-600 hover:bg-blue-700 text-white font-bold py-3 px-4 rounded-lg flex items-center justify-center gap-2 transition disabled:opacity-50">{isZipping ? <span>{zipProgress}</span> : <span>Download Full Archive (All Transcripts — {transcriptStats.withTranscript.toLocaleString()} files)</span>}</button>
                  <button onClick={()=>handleCustomDownload(filteredData.filter(s => s.hasTranscript !== false))} disabled={isZipping} className="w-full bg-gray-100 hover:bg-gray-200 text-gray-800 font-bold py-3 px-4 rounded-lg flex items-center justify-center gap-2 transition disabled:opacity-50">{isZipping ? <span>{zipProgress}</span> : <span>Download Transcripts For Filtered View ({filteredData.filter(s => s.hasTranscript !== false).length.toLocaleString()} files)</span>}</button>
                </div>
              </div>
              <div className="bg-white p-4 sm:p-8 rounded-xl border shadow-sm">
                <div className="flex flex-col sm:flex-row sm:justify-between sm:items-start gap-4 mb-4">
                  <div>
                    <h3 className="text-xl font-bold text-gray-900 whitespace-nowrap">Data Sources</h3>
                    <div className="text-sm text-gray-500 mt-1">{channels.length.toLocaleString()} Channels</div>
                  </div>
                  <div className="flex flex-wrap items-center gap-2">
                    <input value={channelSearch} onChange={(e)=>setChannelSearch(e.target.value)} placeholder="Search channels..." className="text-sm border rounded px-3 py-2 w-full sm:w-auto" />
                    <select value={channelSort} onChange={(e)=>setChannelSort(e.target.value)} className="text-sm border rounded px-2 py-2 flex-1 sm:flex-none">
                      <option value="name_asc">Name ↑</option>
                      <option value="name_desc">Name ↓</option>
                      <option value="count_desc">Transcripts ↓</option>
                      <option value="count_asc">Transcripts ↑</option>
                      <option value="date_desc">Latest Date ↓</option>
                      <option value="date_asc">Latest Date ↑</option>
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
                    }} className="hidden sm:inline-block text-sm bg-gray-100 hover:bg-gray-200 px-3 py-1.5 rounded">Export CSV</button>
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
                    }} className="hidden sm:inline-block text-sm bg-blue-50 hover:bg-blue-100 px-3 py-1.5 rounded">{isExportingMaster? 'Building…' : 'Export Master'}</button>
                  </div>
                </div>
                <ul className="space-y-2 pr-2">
                  {channels.length === 0 && <li className="text-sm text-gray-500">No channels loaded.</li>}
                  {(() => {
                    const q = channelSearch.trim().toLowerCase()
                    let list = channels.slice()
                    if(q) list = list.filter(c => (c.name || '').toLowerCase().includes(q) || (c.url || '').toLowerCase().includes(q) || (c.filename || '').toLowerCase().includes(q))
                    if(channelSort === 'name_asc') list.sort((a,b)=>( (a.name||'').localeCompare(b.name||'') ))
                    else if(channelSort === 'name_desc') list.sort((a,b)=>( (b.name||'').localeCompare(a.name||'') ))
                    else if(channelSort === 'count_desc') list.sort((a,b)=>{ 
                      const nameA = (a.name || '').replace(/\s+/g, '_')
                      const nameB = (b.name || '').replace(/\s+/g, '_')
                      const countA = churchScrapeStats[nameA]?.count || churchScrapeStats[a.name]?.count || 0
                      const countB = churchScrapeStats[nameB]?.count || churchScrapeStats[b.name]?.count || 0
                      return countB - countA
                    })
                    else if(channelSort === 'count_asc') list.sort((a,b)=>{ 
                      const nameA = (a.name || '').replace(/\s+/g, '_')
                      const nameB = (b.name || '').replace(/\s+/g, '_')
                      const countA = churchScrapeStats[nameA]?.count || churchScrapeStats[a.name]?.count || 0
                      const countB = churchScrapeStats[nameB]?.count || churchScrapeStats[b.name]?.count || 0
                      return countA - countB
                    })
                    else if(channelSort === 'date_desc') list.sort((a,b)=>{ 
                      const nameA = (a.name || '').replace(/\s+/g, '_')
                      const nameB = (b.name || '').replace(/\s+/g, '_')
                      const dateA = churchScrapeStats[nameA]?.latestDate || churchScrapeStats[a.name]?.latestDate || ''
                      const dateB = churchScrapeStats[nameB]?.latestDate || churchScrapeStats[b.name]?.latestDate || ''
                      return dateB.localeCompare(dateA)
                    })
                    else if(channelSort === 'date_asc') list.sort((a,b)=>{ 
                      const nameA = (a.name || '').replace(/\s+/g, '_')
                      const nameB = (b.name || '').replace(/\s+/g, '_')
                      const dateA = churchScrapeStats[nameA]?.latestDate || churchScrapeStats[a.name]?.latestDate || ''
                      const dateB = churchScrapeStats[nameB]?.latestDate || churchScrapeStats[b.name]?.latestDate || ''
                      return dateA.localeCompare(dateB)
                    })
                    return list.map((c,i)=>{
                      const name = c.name || c.channel || c.title || c.church || 'Unknown'
                      const href = c.url || c.link || c.href || '#'
                      // Match church name to rawData (try with underscores and spaces)
                      const normalizedName = name.replace(/\s+/g, '_')
                      const scrapeInfo = churchScrapeStats[normalizedName] || churchScrapeStats[name] || null
                      const transcriptCount = scrapeInfo?.count || 0
                      const latestDate = scrapeInfo?.latestDate || null
                      const isStale = latestDate && new Date(latestDate) < new Date(Date.now() - 90 * 24 * 60 * 60 * 1000) // >90 days old
                      const isRecent = latestDate && new Date(latestDate) > new Date(Date.now() - 30 * 24 * 60 * 60 * 1000) // <30 days old
                      const isExpanded = expandedChannels.has(normalizedName)
                      const channelVideos = isExpanded ? getChannelVideos(name) : []
                      const searchTerm = channelVideoSearch[normalizedName] || ''
                      const sortCfg = channelVideoSort[normalizedName] || { key: 'date', direction: 'desc' }
                      
                      // Filter and sort videos
                      const filteredVideos = channelVideos.filter(v => {
                        if (!searchTerm) return true
                        const term = searchTerm.toLowerCase()
                        return (v.title || '').toLowerCase().includes(term) ||
                               (v.speaker || '').toLowerCase().includes(term) ||
                               (v.date || '').toLowerCase().includes(term)
                      }).sort((a, b) => {
                        const dir = sortCfg.direction === 'asc' ? 1 : -1
                        if (sortCfg.key === 'date') return dir * ((a.date || '').localeCompare(b.date || ''))
                        if (sortCfg.key === 'title') return dir * ((a.title || '').localeCompare(b.title || ''))
                        if (sortCfg.key === 'speaker') return dir * ((a.speaker || '').localeCompare(b.speaker || ''))
                        if (sortCfg.key === 'status') {
                          const aHas = a.hasTranscript !== false ? 1 : 0
                          const bHas = b.hasTranscript !== false ? 1 : 0
                          return dir * (aHas - bHas)
                        }
                        return 0
                      })
                      
                      const videosWithTranscripts = channelVideos.filter(v => v.hasTranscript !== false)
                      
                      return (
                        <li key={i}>
                          <div className="block text-sm bg-gray-50 rounded border transition">
                            {/* Channel header row */}
                            <div className="p-3 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2 sm:gap-3">
                              {/* Expand button with clear expand icon */}
                              <button
                                onClick={(e) => {
                                  e.stopPropagation()
                                  setExpandedChannels(prev => {
                                    const next = new Set(prev)
                                    if (next.has(normalizedName)) next.delete(normalizedName)
                                    else next.add(normalizedName)
                                    return next
                                  })
                                }}
                                className={`flex-shrink-0 flex items-center gap-1.5 px-2 py-1 rounded transition text-xs font-medium ${isExpanded ? 'bg-blue-100 text-blue-700' : 'bg-gray-200 text-gray-600 hover:bg-gray-300'}`}
                                title={isExpanded ? 'Click to collapse' : 'Click to expand and see all videos'}
                              >
                                <Icon name={isExpanded ? 'chevron-down' : 'plus'} size={14} />
                                <span className="hidden sm:inline">{isExpanded ? 'Collapse' : 'Expand'}</span>
                              </button>
                              <div className="min-w-0 flex-1 cursor-pointer hover:bg-gray-100 rounded px-2 -mx-2" onClick={()=>{ const target = c.url || c.link || c.href || '#'; if(target && target !== '#'){ window.open(target, '_blank', 'noopener'); } else { alert('No channel URL available'); } }}>
                                <div className="font-medium text-gray-700">{name}</div>
                                <div className="text-xs text-blue-600 truncate"><a href={href} target="_blank" rel="noopener noreferrer" onClick={e => e.stopPropagation()}>{href}</a></div>
                              </div>
                              <div className="flex items-center justify-between sm:justify-end gap-3 flex-shrink-0">
                                <div className="sm:text-right">
                                  <div className={`text-sm font-semibold ${transcriptCount > 0 ? 'text-gray-700' : 'text-red-500'}`}>
                                    {transcriptCount > 0 ? `${transcriptCount.toLocaleString()} transcripts` : 'No data'}
                                  </div>
                                  {latestDate ? (
                                    <div className={`text-xs ${isRecent ? 'text-green-600' : isStale ? 'text-orange-500' : 'text-blue-500'}`}>
                                      Latest: {latestDate}
                                      {isRecent && <span className="ml-1">✓</span>}
                                      {isStale && <span className="ml-1">⚠</span>}
                                    </div>
                                  ) : (
                                    <div className="text-xs text-red-400">Not scraped</div>
                                  )}
                                </div>
                                <a 
                                  href={c.url || c.link || c.href || '#'} 
                                  target="_blank" 
                                  rel="noopener noreferrer"
                                  onClick={(e) => { e.stopPropagation(); if(!(c.url || c.link || c.href)) { e.preventDefault(); alert('No channel URL available'); } }}
                                  className="p-1 text-red-500 hover:text-red-700 hover:bg-red-50 rounded transition"
                                  title="Open YouTube channel"
                                >
                                  <Icon name="externalLink" size={18} />
                                </a>
                              </div>
                            </div>
                            {/* Expanded video list */}
                            {isExpanded && (
                              <div className="border-t bg-white px-3 py-3">
                                {channelVideos.length === 0 ? (
                                  <div className="text-sm text-gray-500 py-2">No videos found for this channel.</div>
                                ) : (
                                  <div>
                                    {/* Search bar and download button */}
                                    <div className="flex flex-col sm:flex-row gap-2 mb-3">
                                      <div className="relative flex-1">
                                        <Icon name="search" size={14} className="absolute left-2 top-1/2 -translate-y-1/2 text-gray-400" />
                                        <input
                                          type="text"
                                          placeholder="Search videos by title, speaker, or date..."
                                          value={searchTerm}
                                          onChange={(e) => setChannelVideoSearch(prev => ({ ...prev, [normalizedName]: e.target.value }))}
                                          className="w-full pl-7 pr-3 py-1.5 text-xs border rounded focus:outline-none focus:ring-1 focus:ring-blue-500"
                                        />
                                        {searchTerm && (
                                          <button 
                                            onClick={() => setChannelVideoSearch(prev => ({ ...prev, [normalizedName]: '' }))}
                                            className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
                                          >
                                            <Icon name="x" size={12} />
                                          </button>
                                        )}
                                      </div>
                                      <button
                                        onClick={(e) => {
                                          e.stopPropagation()
                                          handleCustomDownload(videosWithTranscripts)
                                        }}
                                        disabled={isZipping || videosWithTranscripts.length === 0}
                                        className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition whitespace-nowrap"
                                        title={`Download all ${videosWithTranscripts.length} transcripts for ${name}`}
                                      >
                                        <Icon name="download" size={14} />
                                        <span>{isZipping ? 'Downloading...' : `Download All (${videosWithTranscripts.length})`}</span>
                                      </button>
                                    </div>
                                    
                                    {/* Video table */}
                                    <div className="max-h-72 overflow-y-auto border rounded">
                                      <table className="w-full text-xs">
                                        <thead className="sticky top-0 bg-gray-50 border-b">
                                          <tr className="text-gray-600 text-left">
                                            <th 
                                              className="py-2 px-2 font-medium w-24 cursor-pointer hover:bg-gray-100 select-none"
                                              onClick={() => setChannelVideoSort(prev => ({
                                                ...prev,
                                                [normalizedName]: { 
                                                  key: 'date', 
                                                  direction: sortCfg.key === 'date' && sortCfg.direction === 'desc' ? 'asc' : 'desc' 
                                                }
                                              }))}
                                            >
                                              Date {sortCfg.key === 'date' && <span className="ml-1">{sortCfg.direction === 'desc' ? '↓' : '↑'}</span>}
                                            </th>
                                            <th 
                                              className="py-2 px-2 font-medium cursor-pointer hover:bg-gray-100 select-none"
                                              onClick={() => setChannelVideoSort(prev => ({
                                                ...prev,
                                                [normalizedName]: { 
                                                  key: 'title', 
                                                  direction: sortCfg.key === 'title' && sortCfg.direction === 'asc' ? 'desc' : 'asc' 
                                                }
                                              }))}
                                            >
                                              Title {sortCfg.key === 'title' && <span className="ml-1">{sortCfg.direction === 'asc' ? '↑' : '↓'}</span>}
                                            </th>
                                            <th 
                                              className="py-2 px-2 font-medium w-28 hidden sm:table-cell cursor-pointer hover:bg-gray-100 select-none"
                                              onClick={() => setChannelVideoSort(prev => ({
                                                ...prev,
                                                [normalizedName]: { 
                                                  key: 'speaker', 
                                                  direction: sortCfg.key === 'speaker' && sortCfg.direction === 'asc' ? 'desc' : 'asc' 
                                                }
                                              }))}
                                            >
                                              Speaker {sortCfg.key === 'speaker' && <span className="ml-1">{sortCfg.direction === 'asc' ? '↑' : '↓'}</span>}
                                            </th>
                                            <th 
                                              className="py-2 px-2 font-medium w-20 text-center cursor-pointer hover:bg-gray-100 select-none"
                                              onClick={() => setChannelVideoSort(prev => ({
                                                ...prev,
                                                [normalizedName]: { 
                                                  key: 'status', 
                                                  direction: sortCfg.key === 'status' && sortCfg.direction === 'desc' ? 'asc' : 'desc' 
                                                }
                                              }))}
                                              title="Transcript availability status"
                                            >
                                              Transcript {sortCfg.key === 'status' && <span className="ml-1">{sortCfg.direction === 'desc' ? '↓' : '↑'}</span>}
                                            </th>
                                            <th className="py-2 px-2 font-medium w-24 text-center">Actions</th>
                                          </tr>
                                        </thead>
                                        <tbody className="divide-y divide-gray-100">
                                          {filteredVideos.length === 0 ? (
                                            <tr>
                                              <td colSpan="5" className="py-4 text-center text-gray-500">
                                                No videos match your search.
                                              </td>
                                            </tr>
                                          ) : filteredVideos.map((video, vi) => (
                                            <tr key={vi} className="hover:bg-gray-50">
                                              <td className="py-2 px-2 text-gray-600 whitespace-nowrap">{video.date || 'Unknown'}</td>
                                              <td className="py-2 px-2 text-gray-800 truncate max-w-[150px] sm:max-w-[250px]" title={video.title}>{video.title || 'Untitled'}</td>
                                              <td className="py-2 px-2 text-gray-600 truncate hidden sm:table-cell">{video.speaker || '—'}</td>
                                              <td className="py-2 px-2 text-center">
                                                {video.hasTranscript !== false ? (
                                                  <span className="inline-flex items-center gap-1 text-green-600" title="Transcript available">
                                                    <Icon name="check" size={12} /> <span className="hidden sm:inline text-[10px]">Yes</span>
                                                  </span>
                                                ) : (
                                                  <span className="inline-flex items-center gap-1 text-red-400" title="No transcript available">
                                                    <Icon name="x" size={12} /> <span className="hidden sm:inline text-[10px]">No</span>
                                                  </span>
                                                )}
                                              </td>
                                              <td className="py-2 px-2">
                                                <div className="flex items-center justify-center gap-1">
                                                  {/* View transcript popup */}
                                                  {video.hasTranscript !== false && video.path && (
                                                    <button
                                                      onClick={async (e) => {
                                                        e.stopPropagation()
                                                        try {
                                                          const apiPrefix = import.meta.env.BASE_URL || '/'
                                                          const res = await fetch(apiPrefix + video.path)
                                                          if (res.ok) {
                                                            const content = await res.text()
                                                            setChannelTranscriptPreview({ video, content })
                                                          } else {
                                                            alert('Could not load transcript')
                                                          }
                                                        } catch (err) {
                                                          alert('Error loading transcript: ' + err.message)
                                                        }
                                                      }}
                                                      className="p-1 text-blue-500 hover:text-blue-700 hover:bg-blue-50 rounded"
                                                      title="View transcript"
                                                    >
                                                      <Icon name="eye" size={14} />
                                                    </button>
                                                  )}
                                                  {/* YouTube video link */}
                                                  {(video.url || video.videoUrl) && (
                                                    <a
                                                      href={video.url || video.videoUrl}
                                                      target="_blank"
                                                      rel="noopener noreferrer"
                                                      onClick={(e) => e.stopPropagation()}
                                                      className="p-1 text-red-500 hover:text-red-700 hover:bg-red-50 rounded inline-flex"
                                                      title="Open YouTube video"
                                                    >
                                                      <Icon name="externalLink" size={14} />
                                                    </a>
                                                  )}
                                                  {/* Download individual transcript */}
                                                  {video.hasTranscript !== false && video.path && (
                                                    <button
                                                      onClick={async (e) => {
                                                        e.stopPropagation()
                                                        try {
                                                          const apiPrefix = import.meta.env.BASE_URL || '/'
                                                          const res = await fetch(apiPrefix + video.path)
                                                          if (res.ok) {
                                                            const content = await res.text()
                                                            const filename = video.path.split('/').pop() || 'transcript.txt'
                                                            const blob = new Blob([content], { type: 'text/plain;charset=utf-8' })
                                                            const url = URL.createObjectURL(blob)
                                                            const a = document.createElement('a')
                                                            a.href = url
                                                            a.download = filename
                                                            a.click()
                                                            URL.revokeObjectURL(url)
                                                          } else {
                                                            alert('Could not download transcript')
                                                          }
                                                        } catch (err) {
                                                          alert('Error downloading: ' + err.message)
                                                        }
                                                      }}
                                                      className="p-1 text-gray-500 hover:text-gray-700 hover:bg-gray-100 rounded"
                                                      title="Download transcript"
                                                    >
                                                      <Icon name="download" size={14} />
                                                    </button>
                                                  )}
                                                </div>
                                              </td>
                                            </tr>
                                          ))}
                                        </tbody>
                                      </table>
                                    </div>
                                    
                                    {/* Summary footer */}
                                    <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2 text-xs text-gray-500 mt-2 pt-2 border-t">
                                      <div>
                                        {searchTerm ? (
                                          <span>Showing {filteredVideos.length.toLocaleString()} of {channelVideos.length.toLocaleString()} videos</span>
                                        ) : (
                                          <span>{channelVideos.length.toLocaleString()} total videos • {videosWithTranscripts.length.toLocaleString()} with transcripts</span>
                                        )}
                                      </div>
                                      <div className="text-gray-400">
                                        Click column headers to sort • Use search to filter
                                      </div>
                                    </div>
                                  </div>
                                )}
                              </div>
                            )}
                          </div>
                        </li>
                      )
                    })
                  })()}
                </ul>
                {/* channel preview modal removed — clicking opens channel URL in a new tab */}
                
                {/* Transcript preview popup for expanded channel videos */}
                {channelTranscriptPreview && (
                  <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4" onClick={() => setChannelTranscriptPreview(null)}>
                    <div 
                      className="bg-white rounded-lg shadow-xl max-w-4xl w-full max-h-[85vh] flex flex-col"
                      onClick={(e) => e.stopPropagation()}
                    >
                      {/* Header */}
                      <div className="flex items-start justify-between p-4 border-b bg-gray-50 rounded-t-lg">
                        <div className="min-w-0 flex-1 pr-4">
                          <h3 className="font-bold text-gray-800 truncate">{channelTranscriptPreview.video.title || 'Transcript'}</h3>
                          <div className="flex flex-wrap gap-2 mt-1 text-xs text-gray-500">
                            {channelTranscriptPreview.video.date && <span>{channelTranscriptPreview.video.date}</span>}
                            {channelTranscriptPreview.video.speaker && <span>• {channelTranscriptPreview.video.speaker}</span>}
                            {channelTranscriptPreview.video.church && <span>• {channelTranscriptPreview.video.church.replace(/_/g, ' ')}</span>}
                          </div>
                        </div>
                        <div className="flex items-center gap-2">
                          {/* Download button */}
                          <button
                            onClick={() => {
                              const filename = channelTranscriptPreview.video.path?.split('/').pop() || 'transcript.txt'
                              const blob = new Blob([channelTranscriptPreview.content], { type: 'text/plain;charset=utf-8' })
                              const url = URL.createObjectURL(blob)
                              const a = document.createElement('a')
                              a.href = url
                              a.download = filename
                              a.click()
                              URL.revokeObjectURL(url)
                            }}
                            className="p-2 text-gray-600 hover:text-gray-800 hover:bg-gray-200 rounded transition"
                            title="Download transcript"
                          >
                            <Icon name="download" size={18} />
                          </button>
                          {/* YouTube link */}
                          {(channelTranscriptPreview.video.url || channelTranscriptPreview.video.videoUrl) && (
                            <button
                              onClick={() => window.open(channelTranscriptPreview.video.url || channelTranscriptPreview.video.videoUrl, '_blank', 'noopener')}
                              className="p-2 text-red-500 hover:text-red-700 hover:bg-red-50 rounded transition"
                              title="Watch on YouTube"
                            >
                              <Icon name="video" size={18} />
                            </button>
                          )}
                          {/* Close button */}
                          <button
                            onClick={() => setChannelTranscriptPreview(null)}
                            className="p-2 text-gray-500 hover:text-gray-700 hover:bg-gray-200 rounded transition"
                            title="Close"
                          >
                            <Icon name="x" size={18} />
                          </button>
                        </div>
                      </div>
                      {/* Content */}
                      <div className="flex-1 overflow-y-auto p-4">
                        <pre className="whitespace-pre-wrap text-sm text-gray-700 font-sans leading-relaxed">
                          {channelTranscriptPreview.content}
                        </pre>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}

          {view === 'about' && (
            <div className="bg-white p-8 rounded-xl border shadow-sm max-w-4xl mx-auto">
              <h2 className="text-2xl font-bold text-gray-900 mb-6 flex items-center gap-3">
                <Icon name="info" size={28} />
                About This Project
              </h2>
              
              <div className="prose prose-blue max-w-none space-y-6">
                <p className="text-gray-700 leading-relaxed">
                  This site is part of an independent, non-commercial journalistic and research project that analyzes publicly available sermon content published on Message Church YouTube channels.
                </p>
                <p className="text-gray-700 leading-relaxed">
                  The purpose of the project is to support research, reporting, and public understanding by identifying patterns, recurring references, and trends in religious messaging over time.
                </p>
                <p className="text-gray-700 leading-relaxed">
                  The project transforms publicly available speech into analytical datasets that help readers understand what is being said, how often, and across which organizations.
                </p>

                <h3 className="text-xl font-bold text-gray-800 mt-8 mb-3">Source Attribution</h3>
                <p className="text-gray-700 leading-relaxed">
                  This project emphasizes transparency and verification. Analytical outputs include direct links to the original YouTube videos so that readers can review the full source material in its original context.
                </p>
                <p className="text-gray-700 leading-relaxed">
                  The project is not intended to replace original content and encourages readers to consult primary sources when evaluating findings or claims.
                </p>

                <h3 className="text-xl font-bold text-gray-800 mt-8 mb-3">Fair Use & Purpose</h3>
                <p className="text-gray-700 leading-relaxed">
                  This project asserts fair use under U.S. copyright law (17 U.S.C. § 107).
                </p>
                <p className="text-gray-700 leading-relaxed">
                  Any copyrighted material referenced or analyzed is used solely for purposes of news reporting, criticism, commentary, research, and scholarship. The use is transformative, focusing on analysis and aggregation rather than simple reproduction of original works.
                </p>
                <p className="text-gray-700 leading-relaxed">
                  The project is non-commercial and does not monetize content. Full transcripts are provided for research and verification purposes, but the project does not host or stream original videos and is not intended to serve as a substitute for viewing content at the original source.
                </p>

                <h3 className="text-xl font-bold text-gray-800 mt-8 mb-3">Methodology</h3>
                <ul className="list-disc list-inside space-y-2 text-gray-700">
                  <li>Analysis is based on publicly available video metadata and transcripts</li>
                  <li>Content is categorized by church, speaker, topic, date, language, and type</li>
                  <li>Outputs emphasize aggregated statistics and timelines</li>
                  <li>The project is independent and unaffiliated with any church or platform</li>
                </ul>

                <h3 className="text-xl font-bold text-gray-800 mt-8 mb-3">Transparency & Good-Faith Compliance</h3>
                <p className="text-gray-700 leading-relaxed">
                  Requests for review, correction, or removal of specific material will be evaluated promptly and in good faith.
                </p>
                <p className="text-gray-700 leading-relaxed">
                  All content remains the property of its respective owners.
                </p>

                <h3 className="text-xl font-bold text-gray-800 mt-8 mb-3">Contact</h3>
                <p className="text-gray-700 leading-relaxed">
                  For inquiries, corrections, or feedback, please contact:<br />
                  <a href="mailto:messageanalyticsproject@gmail.com" className="text-blue-600 hover:text-blue-800 underline">
                    messageanalyticsproject@gmail.com
                  </a>
                </p>
              </div>
            </div>
          )}
        </main>

        {/* Footer */}
        <footer className="bg-gray-100 border-t border-gray-200 mt-12 py-4">
          <div className="max-w-7xl mx-auto px-4 text-center text-xs text-gray-500">
            This is a non-commercial journalistic research project. All use of copyrighted material is claimed as fair use for purposes of analysis, reporting, and commentary.
          </div>
        </footer>
        {selectedSermon && <SermonModal sermon={selectedSermon} focusMatchIndex={selectedSermonFocus} wholeWords={selectedSermon.wholeWords !== false} onClose={()=>{ setSelectedSermon(null); setSelectedSermonFocus(0); }} />}
        {expandedChart && (()=>{
          // Re-resolve the chart from latest channelTrends so mention counts reflect current `customCounts`.
          // Normalize church names to avoid mismatch due to punctuation/casing/whitespace.
          const normalize = (v) => ('' + (v || '')).replace(/[^0-9A-Za-z]+/g, ' ').trim().toLowerCase()
          const target = normalize(expandedChart.church || expandedChart.name)
          const fresh = channelTrends && channelTrends.find(c => normalize(c.church) === target)
          const chartToShow = fresh ? { ...fresh, showRaw: expandedChart.showRaw || false } : expandedChart
          return (<ChartModal chart={chartToShow} domain={dateDomain} searchTerm={activeTerm || (lastAnalysisRef.current && lastAnalysisRef.current.term) || ''} onClose={()=>setExpandedChart(null)} onSelectSermon={(s, focusIndex)=>{ setExpandedChart(null); setSelectedSermon({ ...s, searchTerm: activeRegex || activeTerm || (lastAnalysisRef.current && lastAnalysisRef.current.term) || null, wholeWords: lastAnalysisRef.current?.wholeWords !== false }); setSelectedSermonFocus(focusIndex || 0); }} />)
        })()}
      </div>
    </ErrorBoundary>
  )
}
