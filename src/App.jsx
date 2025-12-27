import React, { useState, useEffect, useMemo, useRef, useCallback, Component } from 'react'
import {
  ComposedChart, Line, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, Area
} from 'recharts'
import JSZip from 'jszip'
import resampleData from './utils/resample'
import Icon from './components/Icon'
import staticChannels from '../channels.json'
import MultiSelect from './components/MultiSelect'
import VirtualizedTable from './components/VirtualizedTable'
import TopicAnalyzer from './components/TopicAnalyzerDefault'
import StatCard from './components/StatCard'
import SermonModal from './components/SermonModal'
import ChartModal from './components/ChartModal'
import HeatmapDetails from './components/HeatmapDetails'
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
  const [dataDate, setDataDate] = useState(null)
  const [channels, setChannels] = useState([])

  // GLOBAL
  const [activeTerm, setActiveTerm] = useState(DEFAULT_TERM)
  const [activeRegex, setActiveRegex] = useState(DEFAULT_REGEX_STR)
  const [customCounts, setCustomCounts] = useState(null)
  const [isAnalyzing, setIsAnalyzing] = useState(false)
  const [analysisProgress, setAnalysisProgress] = useState('')
  const lastAnalysisRef = useRef({ term: null, regex: null })

  // FILTERS
  const [selChurches, setSelChurches] = useState([])
  const [selSpeakers, setSelSpeakers] = useState([])
  const [selYears, setSelYears] = useState([])
  const [selTypes, setSelTypes] = useState([])
  const [selLangs, setSelLangs] = useState([])
  const [rollingWeeks, setRollingWeeks] = useState(26)
  const [aggregateWindow, setAggregateWindow] = useState(26)
  const [heatmapMode, setHeatmapMode] = useState('month')
  const [view, setView] = useState('dashboard')
  const [expandedChart, setExpandedChart] = useState(null)
  const [selectedSermon, setSelectedSermon] = useState(null)
  const [selectedSermonFocus, setSelectedSermonFocus] = useState(0)
  const [heatmapModalData, setHeatmapModalData] = useState(null)
  const [channelSearch, setChannelSearch] = useState('')
  const [channelSort, setChannelSort] = useState('name_asc')
  // preview modal removed: clicking a channel now opens its URL directly
  const [transcriptSummaryCounts, setTranscriptSummaryCounts] = useState({})

  // TABLE
  const [tableFilters, setTableFilters] = useState({ date:'', church:'', title:'', speaker:'', category:'', mentions:'', rate:'' })
  const [page, setPage] = useState(1)
  const [sortConfig, setSortConfig] = useState({ key: 'mentionCount', direction: 'desc' })
  const PAGE_SIZE = 50

  const [isZipping, setIsZipping] = useState(false)
  const [zipProgress, setZipProgress] = useState('')
  const [isExportingMaster, setIsExportingMaster] = useState(false)

  // When a custom analysis runs and `customCounts` is set, default the table
  // to sort by mentions (highest first) so the most relevant sermons surface.
  useEffect(() => {
    if(customCounts && typeof customCounts === 'object'){
      setSortConfig({ key: 'mentionCount', direction: 'desc' })
      setPage(1)
    }
  }, [customCounts])

  useEffect(()=>{
    const init = async ()=>{
      try{
        let res = await fetch('site_api/metadata.json')
        let prefix='site_api/'
        if(!res.ok){ res = await fetch('metadata.json'); prefix = '' }
        if(!res.ok) throw new Error('Metadata not found.')
        const json = await res.json()
        setDataDate(json.generated || 'Unknown')
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
          const tries = [prefix + 'channels.json', '/channels.json', 'channels.json']
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
        setSelChurches([...new Set(list.map(s=>s.church))]); setSelSpeakers([...new Set(list.map(s=>s.speaker))])
        const currentYear = new Date().getFullYear(); const years = [...new Set(list.map(s=>s.year))].filter(y=>parseInt(y) <= currentYear).sort().reverse(); const defaultYears = years.filter(y=>parseInt(y) >= 2020); setSelYears(defaultYears.length>0?defaultYears:years)
        const types = [...new Set(list.map(s=>s.type))]; setSelTypes(types)
        const langs = [...new Set(list.map(s=>s.language))]; const defaultLangs = langs.filter(l => ['English','Spanish'].includes(l)); setSelLangs(defaultLangs.length>0?defaultLangs:langs)
        
        setLoading(false)
      }catch(e){ setError(e.message); setLoading(false) }
    }
    init()
  },[])

  // Build per-channel transcript availability map once `channels` is available
  useEffect(()=>{
    if(!channels || channels.length===0) return
    const build = async () => {
        const rawGitPrefix = 'https://raw.githubusercontent.com/messageanalytics/wmbmentions.github.io/main/docs/'
      try{
        const map = {}
        const churchNames = channels.map(c=>c.name).filter(Boolean)
        for(const name of churchNames){
          const base = ('' + name).trim()
          // Prefer underscore-normalized filenames first (matches files in repo `data/`)
          const variants = [ base.replace(/\s+/g,'_'), base.replace(/[^0-9A-Za-z]+/g,'_').replace(/^_+|_+$/g,''), base.replace(/\s+/g,'-'), base.toLowerCase().replace(/\s+/g,'_'), base ]
          let found = false
            for(const v of variants){
              if(!v) continue
              // Prefer repository `data/` files first, then absolute `/data/`, then `site_api/data/` (apiPrefix)
              const candidates = [ `data/${v}_Summary.csv`, `/data/${v}_Summary.csv`, `${apiPrefix}data/${v}_Summary.csv` ]
              let txt = null
              for(const tryPath of candidates){
                try{
                  console.debug('Trying summary path', tryPath)
                  const r = await fetch(tryPath)
                  console.debug('Response', tryPath, r && r.status)
                  // If the normal fetch fails, try the raw GitHubusercontent path (fallback)
                  if(!r.ok){
                    try{
                      const rawPath = rawGitPrefix + (('' + tryPath).replace(/^\/+/, ''))
                      console.debug('Trying raw GitHub path', rawPath)
                      const rr = await fetch(rawPath)
                      console.debug('Response raw', rawPath, rr && rr.status)
                      if(!rr.ok) continue
                      txt = await rr.text()
                    }catch(e){ console.debug('Raw fetch error', tryPath, e && e.message); continue }
                  } else {
                    txt = await r.text()
                  }
                  // Basic validation: reject HTML responses (SPA fallback) or non-CSV content
                  const tl = (txt||'').trim()
                  if(tl.startsWith('<') || !tl.includes('date,') || tl.length < 100){
                    console.debug('Rejecting fetched summary as non-CSV or too small', tryPath, 'len', tl.length)
                    txt = null
                    continue
                  }
                  console.debug('Loaded summary for', name, 'from', tryPath, 'length', (txt||'').length)
                  break
                }catch(e){ console.debug('Fetch error', tryPath, e && e.message) }
              }
            if(!txt) continue
            const lines = txt.split(/\r?\n/).filter(Boolean)
            if(lines.length <= 1){ const counts = { total: 0, withTranscript: 0, withoutTranscript: 0 }; map[name] = counts; map[base.replace(/\s+/g,'_')] = counts; map[base.replace(/[^0-9A-Za-z]+/g,'_').toLowerCase()] = counts; found = true; break }
            const header = lines[0].split(',').map(h=>h.trim())
            const statusIdx = header.findIndex(h => /status/i.test(h))
            let withT = 0
            for(let i=1;i<lines.length;i++){ const cols = lines[i].split(','); const status = (cols[statusIdx] || '').trim(); if(/success/i.test(status)) withT++ }
            const total = Math.max(0, lines.length-1)
            const counts = { total, withTranscript: withT, withoutTranscript: total - withT }
            map[name] = counts
            map[base.replace(/\s+/g,'_')] = counts
            map[base.replace(/[^0-9A-Za-z]+/g,'_').toLowerCase()] = counts
            found = true
            break
          }
          if(!found) map[name] = null
        }
      	console.debug('Transcript summary counts map built, keys:', Object.keys(map).length)
        setTranscriptSummaryCounts(map)
      }catch(e){ console.warn('Failed to build transcript summary counts', e) }
    }
    build()
  }, [channels, apiPrefix])

  const options = useMemo(()=>{ const getUnique = (k) => [...new Set(rawData.map(s=>s[k]))].filter(Boolean).sort(); return { churches: getUnique('church'), speakers: getUnique('speaker'), years: getUnique('year').reverse(), types: getUnique('type'), langs: getUnique('language') } }, [rawData])

  const enrichedData = useMemo(()=>{ if(!customCounts) return rawData; return rawData.map(s=>{ const newCount = customCounts.get(s.id) || 0; return { ...s, mentionCount: newCount, mentionsPerHour: s.durationHrs > 0 ? parseFloat((newCount / s.durationHrs).toFixed(1)) : 0, searchTerm: activeRegex } }) }, [rawData, customCounts, activeRegex])

  const filteredData = useMemo(()=> enrichedData.filter(s => selChurches.includes(s.church) && selSpeakers.includes(s.speaker) && selYears.includes(s.year) && selTypes.includes(s.type) && selLangs.includes(s.language)), [enrichedData, selChurches, selSpeakers, selYears, selTypes, selLangs])

  const totalsMemo = useMemo(()=>{
    const map = transcriptSummaryCounts || {}
    // Prefer channels list if present to avoid duplicate keys in map
    if(channels && channels.length>0){
      let total = 0, withT = 0, without = 0
      for(const c of channels){
        const name = c && c.name
        if(!name) continue
        const v = map[name] || map[(name||'').replace(/\s+/g,'_')] || map[(name||'').replace(/[^0-9A-Za-z]+/g,'_').toLowerCase()]
        if(!v) continue
        total += v.total || 0; withT += v.withTranscript || 0; without += v.withoutTranscript || 0
      }
      if(total > 0) return { total, with: withT, without }
    }
    const keys = Object.keys(map)
    if(keys.length>0){
      // sum unique entries only
      const seen = new Set()
      let total = 0, withT = 0, without = 0
      for(const k of keys){
        const v = map[k]
        if(!v || !v.total) continue
        const keySig = `${v.total}-${v.withTranscript}-${v.withoutTranscript}`
        if(seen.has(keySig)) continue
        seen.add(keySig)
        total += v.total || 0; withT += v.withTranscript || 0; without += v.withoutTranscript || 0
      }
      if(total > 0) return { total, with: withT, without }
    }
    const total = rawData.length
    const withT = rawData.filter(s=>s && s.path).length
    return { total, with: withT, without: Math.max(0, total - withT) }
  }, [transcriptSummaryCounts, rawData])

  const dateDomain = useMemo(()=>{ if(selYears.length===0) return ['auto','auto']; const validYears = selYears.map(y=>parseInt(y)).filter(y=>!isNaN(y)); if(validYears.length===0) return ['auto','auto']; const minYear = Math.min(...validYears); const maxYear = Math.max(...validYears); return [new Date(minYear,0,1).getTime(), new Date(maxYear,11,31).getTime()] }, [selYears])

  const handleAnalysis = async (term, variations, rawRegex = null) => {
    if(!term || !term.trim()) return
    lastAnalysisRef.current = { term, regex: rawRegex || null }
    setIsAnalyzing(true)
    setAnalysisProgress('Starting...')
    // Run the main-thread scanner directly (worker removed)
    await scanOnMainThread(term, variations, rawRegex)
  }

  // Fallback: scan chunks on the main thread in batches (used when worker isn't available)
  const scanOnMainThread = async (term, variations, rawRegex = null) => {
    try{
      const BATCH = 250
      let regex
      if(rawRegex && (''+rawRegex).trim()){
        try{ regex = new RegExp(rawRegex, 'gi') }catch(e){ setAnalysisProgress('Invalid regex'); setIsAnalyzing(false); return }
      } else {
        const vars = Array.isArray(variations) ? variations.map(v=>(''+v).trim()).filter(Boolean) : (''+(variations||'')).split(',').map(v=>v.trim()).filter(Boolean)
        const isRegexLike = (s) => /[\\\(\)\[\]\|\^\$\.\*\+\?]/.test(s)
        const escapeRe = (s) => (''+s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
        const patterns = [term, ...vars].map(t => isRegexLike(t) ? t : escapeRe(t))
        regex = new RegExp(`(${patterns.join('|')})`, 'gi')
      }
      const counts = Object.create(null)
      let processedChunks = 0
      const total = totalChunks || 0
      if(!total || total === 0){ setAnalysisProgress('No chunks to scan (fallback)'); setIsAnalyzing(false); return }
      for(let i=0;i<total;i+=BATCH){
        const promises = []
        for(let j=i;j<Math.min(i+BATCH, total); j++){
          const p = fetch(`${apiPrefix}text_chunk_${j}.json`).then(r=> r.ok? r.json(): []).catch(()=>[])
          promises.push(p)
        }
        const results = await Promise.all(promises)
        for(const chunk of results){
          for(const item of chunk){
            try{
              const matches = (item.text && item.text.match(regex)) ? item.text.match(regex).length : 0
              if(matches>0) counts[item.id] = (counts[item.id]||0) + matches
            }catch(e){}
          }
        }
        processedChunks += results.length
        setAnalysisProgress(`Scanning ${total?Math.round((processedChunks/total)*100):0}% (${processedChunks}/${total})`)
        // yield to UI briefly so the browser can remain responsive
        await new Promise(r => setTimeout(r, 20))
      }
      const entries = Object.entries(counts)
      const map = new Map(entries.map(([id,c])=>[id, c]))
      setCustomCounts(map)
      setActiveTerm(term)
      setActiveRegex(rawRegex && rawRegex.trim() ? rawRegex : regex.source)
      setIsAnalyzing(false)
      setAnalysisProgress('Completed (fallback)')
    }catch(e){ console.error('Main-thread scan failed', e); setIsAnalyzing(false); setAnalysisProgress('Error') }
  }

  const stats = useMemo(()=>{ if(!filteredData.length) return null; const total = filteredData.length; const mentions = filteredData.reduce((acc,s)=>acc+s.mentionCount,0); const max = Math.max(...filteredData.map(s=>s.mentionCount)); return { totalSermons: total, totalMentions: mentions, maxMentions: max, avg: (mentions / total).toFixed(1) } }, [filteredData])

  const aggregatedChartData = useMemo(()=>{
    if(!filteredData.length) return []
    const sorted = [...filteredData].sort((a,b)=>a.timestamp - b.timestamp)
    const monthly = resampleData(sorted, undefined, activeTerm)
    return monthly.map((item, idx, arr)=>{ let sum=0, count=0; for(let i=idx;i>=Math.max(0, idx-6); i--){ sum += arr[i].mentionCount; count++ } return { ...item, rollingAvg: count>0?parseFloat((sum/count).toFixed(1)):0 } })
  }, [filteredData, aggregateWindow])

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
      const tcounts = transcriptSummaryCounts && (transcriptSummaryCounts[church] || transcriptSummaryCounts[(church||'').replace(/\s+/g,'_')] || transcriptSummaryCounts[(church||'').replace(/[^0-9A-Za-z]+/g,'_').toLowerCase()])
      charts.push({ church, raw, data: resampleData(raw, undefined, activeTerm), color: getColor(index), transcriptCounts: tcounts || null })
    })
    return charts
  }, [filteredData, selChurches, rollingWeeks, transcriptSummaryCounts, activeTerm])

  const heatmapData = useMemo(()=>{
    if(!filteredData.length) return { labels: [], rows: [] }
    const times = filteredData.map(s=>s.timestamp); const min = Math.min(...times); const max = Math.max(...times);
    const timeSlots = []; let current = new Date(min); const end = new Date(max);
    if(heatmapMode === 'week'){ current.setDate(current.getDate() - current.getDay() + 1); while(current <= end){ const next = new Date(current.getTime() + 7*24*60*60*1000); timeSlots.push({ start: current.getTime(), end: next.getTime(), label: `${current.getMonth()+1}/${current.getDate()}/${current.getFullYear().toString().substr(2)}` }); current = next } } else { current.setDate(1); while(current <= end){ const next = new Date(current); next.setMonth(next.getMonth() + 1); timeSlots.push({ start: current.getTime(), end: next.getTime(), label: `${current.toLocaleString('default', { month: 'short' })} '${current.getFullYear().toString().substr(2)}` }); current = next } }
    const churchRows = selChurches.map(c=>({ church: c, cells: timeSlots.map(t=>{ const sermons = filteredData.filter(s=>s.church===c && s.timestamp >= t.start && s.timestamp < t.end); if(!sermons.length) return { val: -1 }; const tm = sermons.reduce((a,s)=>a+s.mentionCount,0), td = sermons.reduce((a,s)=>a+s.durationHrs,0); return { val: td>0?tm/td:0, count: sermons.length, sermons: sermons } }) }));
    return { labels: timeSlots, rows: churchRows }
  }, [filteredData, selChurches, heatmapMode])

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
    if(tableFilters.title) data = data.filter(s=>s.title.toLowerCase().includes(tableFilters.title.toLowerCase()))
    if(tableFilters.speaker) data = data.filter(s=>s.speaker.toLowerCase().includes(tableFilters.speaker.toLowerCase()))
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

  if(loading) return <div className="h-screen flex items-center justify-center font-medium text-gray-500">Loading Database...</div>
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
        <div className="bg-yellow-50 border-b border-yellow-100 text-yellow-800 text-xs text-center py-2 px-4"><span className="font-bold">Disclaimer:</span> This data is based solely on available YouTube automated transcripts and does not represent all sermons preached during this timeframe.</div>

        <main className="max-w-7xl mx-auto px-4 mt-8">
          <div className="bg-white p-6 rounded-xl border shadow-sm mb-8">
            <div className="flex justify-between items-center mb-4">
              <div>
                <h3 className="font-bold text-gray-800 flex items-center gap-2"><Icon name="filter" /> Filter Database</h3>
                <div className="text-xs text-gray-500 mt-1">Selected Churches: <span className="font-medium text-gray-700">{selChurches.length.toLocaleString()}</span> • Selected Speakers: <span className="font-medium text-gray-700">{selSpeakers.length.toLocaleString()}</span></div>
              </div>
              <button onClick={()=>{ setSelChurches(options.churches); setSelSpeakers(options.speakers); setSelYears(options.years); setSelTypes(options.types); setSelLangs(options.langs) }} className="text-xs text-blue-600 font-medium hover:underline">Reset All</button>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-4">
              <MultiSelect label="Churches" options={options.churches} selected={selChurches} onChange={setSelChurches} />
              <MultiSelect label="Speakers" options={options.speakers} selected={selSpeakers} onChange={setSelSpeakers} />
              <MultiSelect label="Years" options={options.years} selected={selYears} onChange={setSelYears} />
              <MultiSelect label="Categories" options={options.types} selected={selTypes} onChange={setSelTypes} />
              <MultiSelect label="Languages" options={options.langs} selected={selLangs} onChange={setSelLangs} />
            </div>
          </div>

          {view === 'dashboard' && stats && (
            <>
              <TopicAnalyzer onAnalyze={handleAnalysis} isAnalyzing={isAnalyzing} progress={analysisProgress} initialTerm={DEFAULT_TERM} initialVariations={DEFAULT_VARIATIONS} />
              <div className="grid grid-cols-2 md:grid-cols-7 gap-4 mb-8">
                <StatCard title="Total Sermons" value={stats.totalSermons.toLocaleString()} icon="fileText" color="blue" />
                <StatCard title={`Total ${activeTerm}`} value={stats.totalMentions.toLocaleString()} icon="users" color="green" />
                <StatCard title="Avg / Sermon" value={stats.avg} icon="barChart" color="indigo" />
                <StatCard title="Peak Count" value={stats.maxMentions} icon="activity" color="purple" />
                <StatCard title="Videos Analyzed" value={(() => totalsMemo.total).call ? totalsMemo.total.toLocaleString() : totalsMemo.total.toLocaleString()} icon="fileText" color="gray" />
                <StatCard title="With transcript" value={(() => totalsMemo.with).call ? totalsMemo.with.toLocaleString() : totalsMemo.with.toLocaleString()} icon="download" color="green" />
                <StatCard title="Without transcript" value={(() => totalsMemo.without).call ? totalsMemo.without.toLocaleString() : totalsMemo.without.toLocaleString()} icon="x" color="red" />
              </div>

              <div className="bg-white p-6 rounded-xl border shadow-sm h-[500px] mb-8">
                <div className="flex justify-between items-center mb-6"><div><h3 className="font-bold text-gray-800 flex items-center gap-2"><Icon name="activity" /> Detailed Activity & Trend ({activeTerm})</h3><p className="text-xs text-gray-500 mt-1">Area = Volume. Line = {aggregateWindow < 5 ? '1 Mo' : aggregateWindow < 20 ? '3 Mo' : '6 Mo'} Trend.</p></div><div className="flex bg-gray-100 rounded-lg p-1">{[4,12,26].map(w=> <button key={w} onClick={()=>setAggregateWindow(w)} className={`px-2 py-1 text-xs font-medium rounded ${aggregateWindow===w ? 'bg-white shadow text-blue-600' : 'text-gray-500'}`}>{w===4? '1 Mo' : w===12? '3 Mo' : '6 Mo'}</button>)}</div></div>
                <ResponsiveContainer width="100%" height="85%">
                  <ComposedChart data={aggregatedChartData}>
                    <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e5e7eb" />
                    <XAxis dataKey="timestamp" type="number" scale="time" domain={dateDomain} tickFormatter={formatDate} tick={{fontSize:10}} minTickGap={120} />
                    <YAxis tick={{fontSize:10}} />
                    <Tooltip labelFormatter={formatDate} contentStyle={{borderRadius:'8px', border:'none', boxShadow:'0 4px 6px -1px rgba(0,0,0,0.1)'}} />
                    <Legend wrapperStyle={{fontSize:'11px', paddingTop:'10px'}} />
                    <Area type="monotone" dataKey="mentionCount" name="Volume" stroke="#93c5fd" fill="#bfdbfe" fillOpacity={1} isAnimationActive={false} />
                    <Line type="monotone" dataKey="rollingAvg" name="Aggregate Rolling Avg" stroke="#1d4ed8" strokeWidth={3} dot={false} connectNulls={true} isAnimationActive={false} />
                  </ComposedChart>
                </ResponsiveContainer>
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
                <div className="flex justify-between items-center mb-4">
                  <h3 className="font-bold text-gray-800">Sermon List ({processedTableData.length.toLocaleString()})</h3>
                  {(customCounts && (activeTerm || (lastAnalysisRef.current && lastAnalysisRef.current.term))) ? (
                    <div className="text-sm bg-blue-50 text-blue-700 px-3 py-1 rounded font-medium">Sorted by mentions for: <span className="font-semibold">{activeTerm || (lastAnalysisRef.current && lastAnalysisRef.current.term)}</span></div>
                  ) : (sortConfig && sortConfig.key === 'mentionCount' && sortConfig.direction === 'desc') ? (
                    <div className="text-sm bg-blue-50 text-blue-700 px-3 py-1 rounded font-medium">Sorted by mentions (high → low)</div>
                  ) : null}
                </div>
                  <VirtualizedTable
                  columns={[
                    { key: 'date', label: 'Date', width: '120px', filterKey: 'date', filterPlaceholder: 'YYYY-MM', render: (r) => r.date },
                    { key: 'church', label: 'Church', width: '220px', filterKey: 'church', render: (r) => (<span className="bg-gray-100 px-2 py-1 rounded text-xs font-semibold text-gray-600">{r.church}</span>) },
                    { key: 'title', label: 'Title', width: '1fr', filterKey: 'title', render: (r) => (<div className="font-medium text-gray-900 truncate">{r.title}</div>) },
                    { key: 'type', label: 'Type', width: '140px', filterKey: 'category', render: (r) => (<span className="bg-gray-50 px-2 py-1 rounded text-xs border">{r.type}</span>) },
                    { key: 'speaker', label: 'Speaker', width: '200px', filterKey: 'speaker', render: (r) => r.speaker },
                    { key: 'mentionCount', label: 'Mentions', width: '120px', filterKey: 'mentions', filterType: 'number', render: (r) => (<div className={`text-right font-bold ${r.mentionCount===0 ? 'text-red-500' : 'text-blue-600'}`}>{r.mentionCount}</div>) },
                    { key: 'mentionsPerHour', label: 'Rate/Hr', width: '120px', filterKey: 'rate', filterType: 'number', render: (r) => (<div className="text-right text-xs">{r.mentionsPerHour}</div>) },
                    { key: 'action', label: 'Action', width: '80px', render: (r) => (<button onClick={(e)=>{ e.stopPropagation(); const a = document.createElement('a'); a.href = r.path; a.download = `${r.date} - ${r.title}.txt`; a.click(); }} className="text-gray-400 hover:text-blue-600"><Icon name="download" size={16} /></button>) }
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
        {heatmapModalData && <HeatmapDetails data={heatmapModalData} onClose={()=>setHeatmapModalData(null)} onSelect={(s)=>{ setHeatmapModalData(null); setSelectedSermon(s) }} />}
        {expandedChart && (()=>{
          // Re-resolve the chart from latest channelTrends so mention counts reflect current `customCounts`.
          // Normalize church names to avoid mismatch due to punctuation/casing/whitespace.
          const normalize = (v) => ('' + (v || '')).replace(/[^0-9A-Za-z]+/g, ' ').trim().toLowerCase()
          const target = normalize(expandedChart.church || expandedChart.name)
          const fresh = channelTrends && channelTrends.find(c => normalize(c.church) === target)
          const chartToShow = fresh ? { ...fresh, showRaw: expandedChart.showRaw || false } : expandedChart
          return (<ChartModal chart={chartToShow} domain={dateDomain} searchTerm={activeRegex || activeTerm || (lastAnalysisRef.current && lastAnalysisRef.current.term) || ''} onClose={()=>setExpandedChart(null)} onSelectSermon={(s, focusIndex)=>{ setExpandedChart(null); setSelectedSermon({ ...s, searchTerm: activeRegex || activeTerm || (lastAnalysisRef.current && lastAnalysisRef.current.term) || null }); setSelectedSermonFocus(focusIndex || 0); }} />)
        })()}
      </div>
    </ErrorBoundary>
  )
}
