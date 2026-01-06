import React, { useState, useEffect, useMemo, useRef, useCallback, Component, useTransition } from 'react'
import {
  ComposedChart, Line, Bar, BarChart, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, Area, Cell
} from 'recharts'
import JSZip from 'jszip'
import resampleData from './utils/resample'
import { getCachedChunks, cacheChunks, getCacheStats, isCacheValid, setCacheVersion, clearCache } from './utils/chunkCache'
import Icon from './components/Icon'
import MultiSelect from './components/MultiSelect'
import VirtualizedTable from './components/VirtualizedTable'
import TopicAnalyzer from './components/TopicAnalyzerDefault'
import StatCard from './components/StatCard'
import SermonModal from './components/SermonModal'
import ChartModal from './components/ChartModal'
import useDebouncedCallback from './hooks/useDebouncedCallback'
import { WORDS_PER_MINUTE, CHART_POINT_THRESHOLD, getColor } from './constants_local'

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

// Default search for WMB archive - empty by default (just browse)
const DEFAULT_TERM = ''
const DEFAULT_REGEX_STR = ''
const DEFAULT_VARIATIONS = ''

// Helper to encode file paths for URLs (handles spaces, #, etc.)
// First decode (in case already encoded) then selectively encode only problematic chars
// Don't use encodeURIComponent - it encodes commas to %2C but files have literal commas
const encodeFilePath = (path) => {
  const decoded = decodeURIComponent(path)
  return decoded.split('/').map(part => 
    part.replace(/ /g, '%20').replace(/#/g, '%23')
  ).join('/')
}

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

// Scripture reference regex pattern
const SCRIPTURE_REGEX = /\b(Genesis|Exodus|Leviticus|Numbers|Deuteronomy|Joshua|Judges|Ruth|(?:1|2|I|II)\s*Samuel|(?:1|2|I|II)\s*Kings|(?:1|2|I|II)\s*Chronicles|Ezra|Nehemiah|Esther|Job|Psalms?|Proverbs|Ecclesiastes|Song\s*of\s*Solomon|Isaiah|Jeremiah|Lamentations|Ezekiel|Daniel|Hosea|Joel|Amos|Obadiah|Jonah|Micah|Nahum|Habakkuk|Zephaniah|Haggai|Zechariah|Malachi|Matthew|Mark|Luke|John|Acts|Romans|(?:1|2|I|II)\s*Corinthians|Galatians|Ephesians|Philippians|Colossians|(?:1|2|I|II)\s*Thessalonians|(?:1|2|I|II)\s*Timothy|Titus|Philemon|Hebrews|James|(?:1|2|I|II)\s*Peter|(?:1|2|3|I|II|III)\s*John|Jude|Revelation|Rev\.?|Gen\.?|Ex\.?|Lev\.?|Num\.?|Deut\.?|Josh\.?|Judg\.?|(?:1|2)\s*Sam\.?|(?:1|2)\s*Kgs\.?|(?:1|2)\s*Chr\.?|Neh\.?|Esth\.?|Ps\.?|Prov\.?|Eccl\.?|Isa\.?|Jer\.?|Lam\.?|Ezek\.?|Dan\.?|Hos\.?|Obad\.?|Mic\.?|Nah\.?|Hab\.?|Zeph\.?|Hag\.?|Zech\.?|Mal\.?|Matt\.?|Mk\.?|Lk\.?|Jn\.?|Rom\.?|(?:1|2)\s*Cor\.?|Gal\.?|Eph\.?|Phil\.?|Col\.?|(?:1|2)\s*Thess\.?|(?:1|2)\s*Tim\.?|Tit\.?|Phlm\.?|Heb\.?|Jas\.?|(?:1|2)\s*Pet\.?|(?:1|2|3)\s*Jn\.?|Rev\.?)\s*(\d+)(?::(\d+)(?:-(\d+))?)?\b/gi

// Helper to normalize book names
const normalizeBookName = (book) => {
  const normalized = book.toLowerCase().replace(/\./g, '').replace(/\s+/g, ' ').trim()
  const bookMap = {
    'gen': 'Genesis', 'genesis': 'Genesis', 'ex': 'Exodus', 'exodus': 'Exodus',
    'lev': 'Leviticus', 'leviticus': 'Leviticus', 'num': 'Numbers', 'numbers': 'Numbers',
    'deut': 'Deuteronomy', 'deuteronomy': 'Deuteronomy', 'josh': 'Joshua', 'joshua': 'Joshua',
    'judg': 'Judges', 'judges': 'Judges', 'ruth': 'Ruth',
    '1 sam': '1 Samuel', '2 sam': '2 Samuel', 'i samuel': '1 Samuel', 'ii samuel': '2 Samuel', '1 samuel': '1 Samuel', '2 samuel': '2 Samuel',
    '1 kgs': '1 Kings', '2 kgs': '2 Kings', '1 kings': '1 Kings', '2 kings': '2 Kings', 'i kings': '1 Kings', 'ii kings': '2 Kings',
    '1 chr': '1 Chronicles', '2 chr': '2 Chronicles', '1 chronicles': '1 Chronicles', '2 chronicles': '2 Chronicles',
    'ezra': 'Ezra', 'neh': 'Nehemiah', 'nehemiah': 'Nehemiah', 'esth': 'Esther', 'esther': 'Esther',
    'job': 'Job', 'ps': 'Psalms', 'psalm': 'Psalms', 'psalms': 'Psalms',
    'prov': 'Proverbs', 'proverbs': 'Proverbs', 'eccl': 'Ecclesiastes', 'ecclesiastes': 'Ecclesiastes',
    'song of solomon': 'Song of Solomon', 'isa': 'Isaiah', 'isaiah': 'Isaiah',
    'jer': 'Jeremiah', 'jeremiah': 'Jeremiah', 'lam': 'Lamentations', 'lamentations': 'Lamentations',
    'ezek': 'Ezekiel', 'ezekiel': 'Ezekiel', 'dan': 'Daniel', 'daniel': 'Daniel',
    'hos': 'Hosea', 'hosea': 'Hosea', 'joel': 'Joel', 'amos': 'Amos',
    'obad': 'Obadiah', 'obadiah': 'Obadiah', 'jonah': 'Jonah', 'mic': 'Micah', 'micah': 'Micah',
    'nah': 'Nahum', 'nahum': 'Nahum', 'hab': 'Habakkuk', 'habakkuk': 'Habakkuk',
    'zeph': 'Zephaniah', 'zephaniah': 'Zephaniah', 'hag': 'Haggai', 'haggai': 'Haggai',
    'zech': 'Zechariah', 'zechariah': 'Zechariah', 'mal': 'Malachi', 'malachi': 'Malachi',
    'matt': 'Matthew', 'matthew': 'Matthew', 'mk': 'Mark', 'mark': 'Mark',
    'lk': 'Luke', 'luke': 'Luke', 'jn': 'John', 'john': 'John', 'acts': 'Acts',
    'rom': 'Romans', 'romans': 'Romans',
    '1 cor': '1 Corinthians', '2 cor': '2 Corinthians', '1 corinthians': '1 Corinthians', '2 corinthians': '2 Corinthians',
    'gal': 'Galatians', 'galatians': 'Galatians', 'eph': 'Ephesians', 'ephesians': 'Ephesians',
    'phil': 'Philippians', 'philippians': 'Philippians', 'col': 'Colossians', 'colossians': 'Colossians',
    '1 thess': '1 Thessalonians', '2 thess': '2 Thessalonians', '1 thessalonians': '1 Thessalonians', '2 thessalonians': '2 Thessalonians',
    '1 tim': '1 Timothy', '2 tim': '2 Timothy', '1 timothy': '1 Timothy', '2 timothy': '2 Timothy',
    'tit': 'Titus', 'titus': 'Titus', 'phlm': 'Philemon', 'philemon': 'Philemon',
    'heb': 'Hebrews', 'hebrews': 'Hebrews', 'jas': 'James', 'james': 'James',
    '1 pet': '1 Peter', '2 pet': '2 Peter', '1 peter': '1 Peter', '2 peter': '2 Peter',
    '1 jn': '1 John', '2 jn': '2 John', '3 jn': '3 John', '1 john': '1 John', '2 john': '2 John', '3 john': '3 John', 'i john': '1 John', 'ii john': '2 John', 'iii john': '3 John',
    'jude': 'Jude', 'rev': 'Revelation', 'revelation': 'Revelation'
  }
  return bookMap[normalized] || book.charAt(0).toUpperCase() + book.slice(1)
}

// Helper: Build regex for a single term (with wildcard support)
const hasWildcard = (t) => /[*?]/.test(t)
const wildcardToRegex = (pattern) => {
  let escaped = pattern.replace(/[.+^${}()|[\]\\]/g, '\\$&')
  escaped = escaped.replace(/\*/g, '\\S*').replace(/\?/g, '.')
  return escaped
}
// Strip surrounding quotes and handle phrase matching
const stripQuotes = (t) => {
  const trimmed = t.trim()
  if ((trimmed.startsWith('"') && trimmed.endsWith('"')) || (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
    return trimmed.slice(1, -1)
  }
  return trimmed
}
const buildTermRegex = (t, whole = true) => {
  // Strip quotes if present (for phrase matching in OR/AND)
  const term = stripQuotes(t)
  if (hasWildcard(term)) {
    const pattern = wildcardToRegex(term)
    return whole ? new RegExp(`\\b(${pattern})\\b`, 'gi') : new RegExp(`(${pattern})`, 'gi')
  }
  let escaped = term.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  // Handle apostrophes - make them optional or match curly quotes too
  escaped = escaped.replace(/'/g, "[''']?")
  // Handle multi-word phrases - allow flexible whitespace
  const flexiblePattern = escaped.replace(/\s+/g, '\\s+')
  return whole ? new RegExp(`\\b(${flexiblePattern})\\b`, 'gi') : new RegExp(`(${flexiblePattern})`, 'gi')
}

// Cache name for WMB data (separate from main app)
const WMB_CACHE_NAME = 'wmb-sermon-chunks-v1'

export default function BranhamApp({ onSwitchToMain }) {
  const [rawData, setRawData] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [apiPrefix, setApiPrefix] = useState('wmb_api/')
  const [totalChunks, setTotalChunks] = useState(0)
  const [totalSermons, setTotalSermons] = useState(0)
  const [dataDate, setDataDate] = useState(null)
  const [yearRange, setYearRange] = useState('')
  const [totalWords, setTotalWords] = useState(0)
  const [totalDuration, setTotalDuration] = useState(0)
  const [topicCounts, setTopicCounts] = useState({})
  const [venues, setVenues] = useState([])

  // GLOBAL
  const [activeTerm, setActiveTerm] = useState(DEFAULT_TERM)
  const [activeRegex, setActiveRegex] = useState(DEFAULT_REGEX_STR)
  const [customCounts, setCustomCounts] = useState(null)
  const [matchedTerms, setMatchedTerms] = useState([])
  const [isAnalyzing, setIsAnalyzing] = useState(false)
  const [analysisProgress, setAnalysisProgress] = useState({ status: '', percent: 0 })
  const lastAnalysisRef = useRef({ term: null, regex: null })
  const [searchFieldTerm, setSearchFieldTerm] = useState('')  // For populating search input

  // FILTERS
  const [isPending, startFilterTransition] = useTransition()
  const [selTitles, setSelTitlesRaw] = useState([])
  const [selYears, setSelYearsRaw] = useState([])
  const [selVenues, setSelVenuesRaw] = useState([])
  const [selTopics, setSelTopicsRaw] = useState([])
  
  const setSelTitles = useCallback((v) => startFilterTransition(() => setSelTitlesRaw(v)), [])
  const setSelYears = useCallback((v) => startFilterTransition(() => setSelYearsRaw(v)), [])
  const setSelVenues = useCallback((v) => startFilterTransition(() => setSelVenuesRaw(v)), [])
  const setSelTopics = useCallback((v) => startFilterTransition(() => setSelTopicsRaw(v)), [])
  
  const [aggregateWindow, setAggregateWindow] = useState(26)
  const [view, setView] = useState('dashboard')
  const [expandedChart, setExpandedChart] = useState(null)
  const [selectedSermon, setSelectedSermon] = useState(null)
  const [selectedSermonFocus, setSelectedSermonFocus] = useState(0)
  const [chartsCollapsed, setChartsCollapsed] = useState(true)
  const [venueTab, setVenueTab] = useState('bars') // 'bars' or 'heatmap'
  const [browseVenue, setBrowseVenue] = useState(null) // For venue browse popup from bar chart
  const [scriptureView, setScriptureView] = useState('grid') // 'grid' or 'heatmap'

  // DATA TAB
  const [expandedYears, setExpandedYears] = useState(new Set())
  const [yearSearch, setYearSearch] = useState({})
  const [yearSort, setYearSort] = useState({})
  const [expandedTopics, setExpandedTopics] = useState(new Set(['Seven Church Ages', 'Seven Seals']))
  const [downloadProgress, setDownloadProgress] = useState(null) // { current, total, year? }
  const [topicSearches, setTopicSearches] = useState({}) // Search within expanded topics/venues
  
  // Global searchable video database
  const [showGlobalSermonTable, setShowGlobalSermonTable] = useState(false)
  const [globalSermonSearch, setGlobalSermonSearch] = useState('')
  const [globalSermonSort, setGlobalSermonSort] = useState({ key: 'date', direction: 'desc' })

  // NEW FEATURES
  // 1. Search History (localStorage)
  const [searchHistory, setSearchHistory] = useState(() => {
    try { return JSON.parse(localStorage.getItem('wmb-search-history') || '[]') } catch { return [] }
  })
  useEffect(() => { localStorage.setItem('wmb-search-history', JSON.stringify(searchHistory.slice(0, 20))) }, [searchHistory])
  const addToSearchHistory = useCallback((term) => {
    if (!term || term.trim().length < 2) return
    setSearchHistory(prev => [{ term: term.trim(), timestamp: Date.now() }, ...prev.filter(h => h.term !== term.trim())].slice(0, 20))
  }, [])

  // 3. Reading Progress (localStorage)
  const [readingProgress, setReadingProgress] = useState(() => {
    try { return JSON.parse(localStorage.getItem('wmb-reading-progress') || '{}') } catch { return {} }
  })
  useEffect(() => { localStorage.setItem('wmb-reading-progress', JSON.stringify(readingProgress)) }, [readingProgress])
  const saveReadingProgress = useCallback((sermonId, position) => {
    setReadingProgress(prev => ({ ...prev, [sermonId]: { position, timestamp: Date.now() } }))
  }, [])
  const getReadingProgress = useCallback((sermonId) => readingProgress[sermonId], [readingProgress])

  // Scripture reference tracking (on-demand scan)
  const [scriptureScanning, setScriptureScanning] = useState(false)
  const [scriptureCounts, setScriptureCounts] = useState(null) // Map of book -> { count, refs: [{ref, sermonId}] }

  // Download transcripts as ZIP
  const downloadTranscriptsZip = useCallback(async (sermonsToDownload, zipFileName, yearLabel = null) => {
    if (!sermonsToDownload || sermonsToDownload.length === 0) {
      alert('No transcripts available to download')
      return
    }
    
    const sermonsWithPaths = sermonsToDownload.filter(s => s.path)
    if (sermonsWithPaths.length === 0) {
      alert('No downloadable transcripts found')
      return
    }
    
    setDownloadProgress({ current: 0, total: sermonsWithPaths.length, year: yearLabel })
    
    try {
      const zip = new JSZip()
      const baseUrl = import.meta.env.BASE_URL || '/'
      let successCount = 0
      
      for (let i = 0; i < sermonsWithPaths.length; i++) {
        const sermon = sermonsWithPaths[i]
        setDownloadProgress({ current: i + 1, total: sermonsWithPaths.length, year: yearLabel })
        
        try {
          const url = `${baseUrl}${encodeFilePath(sermon.path)}`
          const response = await fetch(url)
          if (response.ok) {
            const text = await response.text()
            // Extract filename from path
            const filename = sermon.path.split('/').pop()
            zip.file(filename, text)
            successCount++
          }
        } catch (err) {
          console.warn(`Failed to fetch ${sermon.path}:`, err)
        }
      }
      
      if (successCount === 0) {
        alert('Could not download any transcripts. Please try downloading from Living Word Broadcast.')
        setDownloadProgress(null)
        return
      }
      
      // Generate and download zip
      const content = await zip.generateAsync({ type: 'blob' })
      const url = URL.createObjectURL(content)
      const a = document.createElement('a')
      a.href = url
      a.download = zipFileName
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      URL.revokeObjectURL(url)
      
      setDownloadProgress(null)
    } catch (err) {
      console.error('Error creating zip:', err)
      alert('Error creating download. Please try again or download from Living Word Broadcast.')
      setDownloadProgress(null)
    }
  }, [])

  // Featured sermon series (order matters - Seven Church Ages first chronologically)
  const featuredSeries = useMemo(() => ({
    'Seven Church Ages': {
      dateCodes: ['60-1205', '60-1206', '60-1207', '60-1208', '60-1209', '60-1210', '60-1211E'],
      color: 'indigo',
      description: 'December 5-11, 1960 - Branham Tabernacle'
    },
    'Seven Seals': {
      dateCodes: ['63-0317E', '63-0318', '63-0319', '63-0320', '63-0321', '63-0322', '63-0323', '63-0324E'],
      color: 'amber',
      description: 'March 17-24, 1963 - Branham Tabernacle'
    }
  }), [])

  // Get sermons for a featured series
  const getSeriesSermons = useCallback((seriesName) => {
    const series = featuredSeries[seriesName]
    if (!series) return []
    return rawData
      .filter(s => series.dateCodes.some(code => s.dateCode === code || s.id?.startsWith(code)))
      .sort((a, b) => (a.date || '').localeCompare(b.date || ''))
  }, [rawData, featuredSeries])

  // 5. Related sermons helper
  const getRelatedSermons = useCallback((sermon, limit = 5) => {
    if (!sermon || !rawData.length) return []
    const related = []
    const sermonDate = new Date(sermon.timestamp)
    
    // Score each sermon based on relatedness
    rawData.forEach(s => {
      if (s.id === sermon.id) return
      let score = 0
      
      // Same topic tags
      if (sermon.topics && s.topics) {
        const common = sermon.topics.filter(t => s.topics.includes(t)).length
        score += common * 10
      }
      
      // Same venue
      if (sermon.venue && s.venue === sermon.venue) score += 5
      
      // Date proximity (within 30 days = higher score)
      const otherDate = new Date(s.timestamp)
      const daysDiff = Math.abs((sermonDate - otherDate) / (1000 * 60 * 60 * 24))
      if (daysDiff <= 7) score += 8
      else if (daysDiff <= 30) score += 4
      else if (daysDiff <= 90) score += 2
      
      if (score > 0) related.push({ ...s, relatedScore: score })
    })
    
    return related.sort((a, b) => b.relatedScore - a.relatedScore).slice(0, limit)
  }, [rawData])

  // Scripture scanning function
  const scanForScriptures = useCallback(async () => {
    if (scriptureScanning) return
    setScriptureScanning(true)
    
    const bookCounts = new Map()
    const BATCH = 50
    
    try {
      for (let i = 0; i < totalChunks; i += BATCH) {
        const promises = []
        for (let j = i; j < Math.min(i + BATCH, totalChunks); j++) {
          promises.push(
            fetch(`${apiPrefix}text_chunk_${j}.json`)
              .then(r => r.ok ? r.json() : [])
              .catch(() => [])
          )
        }
        const results = await Promise.all(promises)
        
        for (const chunk of results) {
          for (const item of chunk) {
            // Find all scripture references in this text
            let match
            const regex = new RegExp(SCRIPTURE_REGEX.source, 'gi')
            while ((match = regex.exec(item.text)) !== null) {
              const book = normalizeBookName(match[1])
              const chapter = match[2]
              const verse = match[3] || ''
              const ref = verse ? `${book} ${chapter}:${verse}` : `${book} ${chapter}`
              
              if (!bookCounts.has(book)) {
                bookCounts.set(book, { count: 0, refs: new Map() })
              }
              const bc = bookCounts.get(book)
              bc.count++
              if (!bc.refs.has(ref)) {
                bc.refs.set(ref, { count: 0, sermons: new Set() })
              }
              bc.refs.get(ref).count++
              bc.refs.get(ref).sermons.add(item.id)
            }
          }
        }
        await new Promise(r => setTimeout(r, 10))
      }
      
      // Convert to sorted arrays for display
      const sorted = Array.from(bookCounts.entries())
        .map(([book, data]) => ({
          book,
          count: data.count,
          refs: Array.from(data.refs.entries())
            .map(([ref, d]) => ({ ref, count: d.count, sermons: Array.from(d.sermons) }))
            .sort((a, b) => b.count - a.count)
            .slice(0, 10)
        }))
        .sort((a, b) => b.count - a.count)
      
      setScriptureCounts(sorted)
    } catch (err) {
      console.error('Scripture scan failed:', err)
    }
    setScriptureScanning(false)
  }, [totalChunks, apiPrefix, scriptureScanning])

  // TABLE
  const [tableFilters, setTableFilters] = useState({ date:'', venue:'', title:'', topic:'' })
  const [sortConfig, setSortConfig] = useState({ key: 'timestamp', direction: 'desc' })
  const [mainChartPinnedBucket, setMainChartPinnedBucket] = useState(null)
  const [mainChartPinnedPosition, setMainChartPinnedPosition] = useState({ x: 0, y: 0 })
  const mainChartPinnedRef = useRef(null)

  // Auto-switch sort when search is performed or cleared
  useEffect(() => {
    if (customCounts !== null) {
      // Search is active - sort by mentions (highest first)
      setSortConfig({ key: 'mentionCount', direction: 'desc' })
    } else {
      // No search - sort by date (most recent first)
      setSortConfig({ key: 'timestamp', direction: 'desc' })
    }
  }, [customCounts])

  // Keyboard shortcuts (Escape to close modal)
  useEffect(() => {
    const handleKeyDown = (e) => {
      if (e.key === 'Escape' && selectedSermon) {
        setSelectedSermon(null)
      }
    }
    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [selectedSermon])

  // Load sermon data on mount
  useEffect(() => {
    let isMounted = true
    const loadData = async () => {
      try {
        const metaPaths = [
          'wmb_api/metadata.json',
          '/wmbmentions.github.io/wmb_api/metadata.json',
          'https://raw.githubusercontent.com/messageanalytics/wmbmentions.github.io/main/docs/wmb_api/metadata.json',
        ]
        let res = null
        let prefix = 'wmb_api/'
        for (const path of metaPaths) {
          try {
            res = await fetch(path)
            if (res.ok) {
              prefix = path.includes('raw.githubusercontent') ? 'https://raw.githubusercontent.com/messageanalytics/wmbmentions.github.io/main/docs/wmb_api/' : (path.includes('/wmbmentions.github.io/') ? '/wmbmentions.github.io/wmb_api/' : 'wmb_api/')
              break
            }
          } catch (e) {}
        }
        if (!res || !res.ok) throw new Error('WMB Metadata not found.')
        const json = await res.json()
        if (!isMounted) return
        
        const dataVersion = json.generated || 'Unknown'
        setDataDate(dataVersion)
        setTotalChunks(json.totalChunks || 0)
        setTotalSermons(json.totalSermons || 0)
        setYearRange(json.yearRange || '')
        setTotalWords(json.totalWords || 0)
        setTotalDuration(json.totalDurationMinutes || 0)
        setTopicCounts(json.topicCounts || {})
        setVenues(json.venues || [])
        setApiPrefix(prefix)
        
        // Process sermon list
        const list = (json.sermons || []).map(s => {
          // Use actual duration (in minutes) if available, otherwise estimate from word count
          const actualDurationMinutes = s.duration && s.duration > 0 ? s.duration : null
          const estimatedDurationHrs = (s.wordCount / WORDS_PER_MINUTE) / 60
          const durationHrs = actualDurationMinutes ? actualDurationMinutes / 60 : (estimatedDurationHrs > 0 ? estimatedDurationHrs : 0.5)
          return { 
            ...s, 
            durationHrs,
            durationMinutes: actualDurationMinutes, // Actual duration in minutes when available
            mentionCount: 0, // Will be set by search
            mentionsPerHour: 0
          }
        })
        
        setRawData(list)
        setSelTitlesRaw([...new Set(list.map(s => s.title))].filter(Boolean))
        const years = [...new Set(list.map(s => s.year))].filter(y => y !== 'Unknown').sort()
        setSelYearsRaw(years)
        setSelVenuesRaw([...new Set(list.map(s => s.venue))].filter(Boolean))
        // Select all topics by default
        const allTopics = [...new Set(list.flatMap(s => s.topics || []))]
        setSelTopicsRaw(allTopics)
        
        setLoading(false)
      } catch (e) {
        console.error('Failed to load WMB data:', e)
        if (isMounted) {
          setError(e.message)
          setLoading(false)
        }
      }
    }
    loadData()
    return () => { isMounted = false }
  }, [])

  // Filter options
  const options = useMemo(() => ({
    titles: [...new Set(rawData.map(s => s.title))].filter(Boolean).sort(),
    years: [...new Set(rawData.map(s => s.year))].filter(y => y !== 'Unknown').sort(),
    venues: [...new Set(rawData.map(s => s.venue))].filter(Boolean).sort(),
    topics: [...new Set(rawData.flatMap(s => s.topics || []))].sort()
  }), [rawData])

  // Enriched data with custom counts
  const enrichedData = useMemo(() => { 
    if (customCounts === null) return rawData
    return rawData.map(s => { 
      const newCount = customCounts.get(s.id) || 0
      return { ...s, mentionCount: newCount, mentionsPerHour: s.durationHrs > 0 ? parseFloat((newCount / s.durationHrs).toFixed(1)) : 0, searchTerm: activeRegex } 
    }) 
  }, [rawData, customCounts, activeRegex])

  // Filtered data
  const filterSets = useMemo(() => ({
    titles: new Set(selTitles),
    years: new Set(selYears),
    venues: new Set(selVenues),
    topics: new Set(selTopics)
  }), [selTitles, selYears, selVenues, selTopics])

  const filteredData = useMemo(() => {
    const { titles, years, venues, topics } = filterSets
    return enrichedData.filter(s => {
      const matchesTopic = !topics.size || (s.topics && s.topics.some(t => topics.has(t)))
      return titles.has(s.title) && years.has(s.year) && venues.has(s.venue) && matchesTopic
    })
  }, [enrichedData, filterSets])

  // Stats
  const stats = useMemo(() => {
    if (!filteredData.length) return null
    const total = filteredData.length
    const mentions = filteredData.reduce((acc, s) => acc + (s.mentionCount || 0), 0)
    let max = 0
    let peakSermon = null
    filteredData.forEach(s => {
      if ((s.mentionCount || 0) > max) { max = s.mentionCount; peakSermon = s }
    })
    return { 
      totalSermons: total, 
      totalMentions: mentions, 
      maxMentions: max, 
      peakSermon, 
      avg: total > 0 ? (mentions / total).toFixed(1) : '0',
      totalWords: filteredData.reduce((acc, s) => acc + (s.wordCount || 0), 0),
      totalDuration: filteredData.reduce((acc, s) => acc + (s.duration || 0), 0)
    }
  }, [filteredData])

  // Chart data
  const chartData = useMemo(() => {
    if (!filteredData.length) return []
    const sorted = [...filteredData].sort((a, b) => a.timestamp - b.timestamp)
    return sorted.map(s => ({
      timestamp: s.timestamp,
      mentionCount: s.mentionCount || 0,
      title: s.title,
      ...s
    }))
  }, [filteredData])

  // Aggregated chart data for main chart
  const aggregatedChartData = useMemo(() => {
    if (!chartData.length) return []
    
    // Group by week
    const buckets = new Map()
    chartData.forEach(s => {
      const weekStart = new Date(s.timestamp)
      weekStart.setHours(0, 0, 0, 0)
      weekStart.setDate(weekStart.getDate() - weekStart.getDay())
      const key = weekStart.getTime()
      
      if (!buckets.has(key)) {
        buckets.set(key, { timestamp: key, mentionCount: 0, count: 0, sermons: [] })
      }
      const b = buckets.get(key)
      b.mentionCount += s.mentionCount || 0
      b.count++
      b.sermons.push(s)
    })
    
    const sorted = Array.from(buckets.values()).sort((a, b) => a.timestamp - b.timestamp)
    
    // Add rolling average
    const windowSize = aggregateWindow
    sorted.forEach((bucket, i) => {
      const start = Math.max(0, i - windowSize + 1)
      const slice = sorted.slice(start, i + 1)
      const avgMentions = slice.reduce((sum, b) => sum + b.mentionCount, 0) / slice.length
      const avgVolume = slice.reduce((sum, b) => sum + b.count, 0) / slice.length
      bucket.rollingAvg = parseFloat(avgMentions.toFixed(1))
      bucket.volumeRollingAvg = parseFloat(avgVolume.toFixed(1))
    })
    
    return sorted
  }, [chartData, aggregateWindow])

  const dateDomain = useMemo(() => {
    if (!aggregatedChartData.length) return ['auto', 'auto']
    return [aggregatedChartData[0].timestamp, aggregatedChartData[aggregatedChartData.length - 1].timestamp]
  }, [aggregatedChartData])

  const formatDate = (ts) => new Date(ts).toLocaleDateString(undefined, { month: 'short', year: '2-digit' })

  // Venue breakdown for bar chart
  const venueBarData = useMemo(() => {
    if (!filteredData.length) return []
    const venueMap = new Map()
    filteredData.forEach(s => {
      const venue = s.venue || 'Unknown'
      if (!venueMap.has(venue)) {
        venueMap.set(venue, { count: 0, totalWords: 0, years: new Set() })
      }
      const v = venueMap.get(venue)
      v.count++
      v.totalWords += s.wordCount || 0
      if (s.date) v.years.add(s.date.slice(0, 4))
    })
    
    const totalSermons = filteredData.length
    return Array.from(venueMap.entries())
      .map(([venue, data]) => {
        const yearsArr = Array.from(data.years).sort()
        return {
          venue,
          count: data.count,
          percent: ((data.count / totalSermons) * 100).toFixed(1),
          totalWords: data.totalWords,
          dateRange: yearsArr.length > 0 ? `${yearsArr[0]} - ${yearsArr[yearsArr.length - 1]}` : 'N/A'
        }
      })
      .sort((a, b) => b.count - a.count)
  }, [filteredData])

  // Venue × Year heatmap data
  const venueYearHeatmap = useMemo(() => {
    if (!rawData.length) return { venues: [], years: [], data: new Map(), maxCount: 0 }
    
    const heatmapMap = new Map() // "venue|year" -> count
    const yearsSet = new Set()
    const venueCounts = new Map()
    
    for (const s of rawData) {
      const venue = s.venue || 'Unknown'
      const year = s.year || 'Unknown'
      yearsSet.add(year)
      const key = `${venue}|${year}`
      heatmapMap.set(key, (heatmapMap.get(key) || 0) + 1)
      venueCounts.set(venue, (venueCounts.get(venue) || 0) + 1)
    }
    
    // Sort venues by total count (descending), years ascending
    const venues = Array.from(venueCounts.entries())
      .sort((a, b) => b[1] - a[1])
      .map(([venue]) => venue)
    const years = Array.from(yearsSet).filter(y => y !== 'Unknown').sort()
    
    let maxCount = 0
    for (const count of heatmapMap.values()) {
      if (count > maxCount) maxCount = count
    }
    
    return { venues, years, data: heatmapMap, maxCount, venueTotals: venueCounts }
  }, [rawData])

  // Location × Year heatmap data
  const locationYearHeatmap = useMemo(() => {
    if (!rawData.length) return { locations: [], years: [], data: new Map(), maxCount: 0 }
    
    const heatmapMap = new Map() // "location|year" -> count
    const yearsSet = new Set()
    const locationCounts = new Map()
    
    for (const s of rawData) {
      const location = s.location || 'Unknown'
      const year = s.year || 'Unknown'
      yearsSet.add(year)
      const key = `${location}|${year}`
      heatmapMap.set(key, (heatmapMap.get(key) || 0) + 1)
      locationCounts.set(location, (locationCounts.get(location) || 0) + 1)
    }
    
    // Sort locations by total count (descending), years ascending
    const locations = Array.from(locationCounts.entries())
      .sort((a, b) => b[1] - a[1])
      .map(([location]) => location)
    const years = Array.from(yearsSet).filter(y => y !== 'Unknown').sort()
    
    let maxCount = 0
    for (const count of heatmapMap.values()) {
      if (count > maxCount) maxCount = count
    }
    
    return { locations, years, data: heatmapMap, maxCount, locationTotals: locationCounts }
  }, [rawData])

  // Sermons grouped by year for Data tab
  const sermonsByYear = useMemo(() => {
    const yearMap = new Map()
    rawData.forEach(s => {
      const year = s.year || 'Unknown'
      if (!yearMap.has(year)) yearMap.set(year, [])
      yearMap.get(year).push(s)
    })
    return Array.from(yearMap.entries())
      .map(([year, sermons]) => ({
        year,
        sermons: sermons.sort((a, b) => (a.date || '').localeCompare(b.date || '')),
        count: sermons.length,
        totalWords: sermons.reduce((acc, s) => acc + (s.wordCount || 0), 0)
      }))
      .sort((a, b) => b.year.localeCompare(a.year)) // Most recent first
  }, [rawData])

  // Search handler
  const handleAnalysis = async (term, variations, rawRegex = null, options = {}) => {
    if (!term && !rawRegex) {
      setCustomCounts(null)
      setActiveTerm('')
      setActiveRegex('')
      setMatchedTerms([])
      return
    }
    // Add to search history
    addToSearchHistory(term || rawRegex)
    lastAnalysisRef.current = { term, rawRegex, variations, ...options }
    setIsAnalyzing(true)
    setAnalysisProgress({ status: 'Preparing search...', percent: 0 })
    setMatchedTerms([])
    await scanOnMainThread(term, variations, rawRegex, options)
  }

  // Main thread search (adapted from App.jsx)
  const scanOnMainThread = async (term, variations, rawRegex = null, options = {}) => {
    const { wholeWords = true } = options
    try {
      const BATCH = 50
      const termCounts = Object.create(null)
      
      // Determine search input - use term if it's a boolean search, otherwise use rawRegex
      const searchInput = (term && parseBooleanSearch(term)) ? term : (rawRegex || term || '')
      
      // Check for boolean search
      const booleanSearch = parseBooleanSearch(searchInput)
      
      let matchFn // Function to match text and return count
      
      if (booleanSearch) {
        // Boolean search mode
        const { type, required, excluded, terms: proximityTerms, distance, ordered, phrase } = booleanSearch
        
        if (type === 'or') {
          // OR: match any of the required terms
          const regexes = required.map(t => buildTermRegex(t, wholeWords))
          matchFn = (text) => {
            // Check exclusions first
            for (const ex of excluded) {
              const exRegex = buildTermRegex(ex, wholeWords)
              if (exRegex.test(text)) return 0
            }
            let total = 0
            const matched = []
            for (let i = 0; i < regexes.length; i++) {
              regexes[i].lastIndex = 0
              const matches = text.match(regexes[i])
              if (matches && matches.length > 0) {
                total += matches.length
                for (const m of matches) {
                  matched.push(m.toLowerCase())
                }
              }
            }
            return { count: total, matched }
          }
        } else if (type === 'and') {
          // AND: all required terms must be present
          const regexes = required.map(t => buildTermRegex(t, wholeWords))
          matchFn = (text) => {
            // Check exclusions first
            for (const ex of excluded) {
              const exRegex = buildTermRegex(ex, wholeWords)
              if (exRegex.test(text)) return 0
            }
            // All required must match
            let total = 0
            const matched = []
            for (let i = 0; i < regexes.length; i++) {
              regexes[i].lastIndex = 0
              const matches = text.match(regexes[i])
              if (!matches || matches.length === 0) return 0 // AND requires all
              total += matches.length
              for (const m of matches) {
                matched.push(m.toLowerCase())
              }
            }
            return { count: total, matched }
          }
        } else if (type === 'phrase') {
          // Exact phrase
          const phraseRegex = wholeWords 
            ? new RegExp(`\\b(${phrase.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})\\b`, 'gi')
            : new RegExp(`(${phrase.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi')
          matchFn = (text) => {
            const matches = text.match(phraseRegex)
            if (!matches) return 0
            return { count: matches.length, matched: matches.map(m => m.toLowerCase()) }
          }
        } else if (type === 'near' || type === 'onear') {
          // Proximity search
          const [t1, t2] = proximityTerms
          const r1 = buildTermRegex(t1, wholeWords)
          const r2 = buildTermRegex(t2, wholeWords)
          matchFn = (text) => {
            const words = text.split(/\s+/)
            let matches = 0
            const matched = []
            for (let i = 0; i < words.length; i++) {
              r1.lastIndex = 0
              if (r1.test(words[i])) {
                const start = ordered ? i + 1 : Math.max(0, i - distance)
                const end = Math.min(words.length, i + distance + 1)
                for (let j = start; j < end; j++) {
                  if (j === i) continue
                  r2.lastIndex = 0
                  if (r2.test(words[j])) {
                    matches++
                    matched.push(words[i].toLowerCase(), words[j].toLowerCase())
                  }
                }
              }
            }
            return { count: matches, matched: [...new Set(matched)] }
          }
        } else {
          // Fallback to simple regex
          const regex = buildTermRegex(searchInput, wholeWords)
          matchFn = (text) => {
            const matches = text.match(regex)
            if (!matches) return 0
            return { count: matches.length, matched: matches.map(m => m.toLowerCase()) }
          }
        }
      } else if (rawRegex && rawRegex.trim()) {
        // Raw regex mode
        let regex
        try {
          regex = wholeWords ? new RegExp(`\\b(${rawRegex})\\b`, 'gi') : new RegExp(`(${rawRegex})`, 'gi')
        } catch (e) {
          setAnalysisProgress({ status: `Invalid regex: ${e.message}`, percent: 0, error: true })
          setIsAnalyzing(false)
          return
        }
        matchFn = (text) => {
          regex.lastIndex = 0
          const matches = text.match(regex)
          if (!matches) return 0
          return { count: matches.length, matched: matches.map(m => m.toLowerCase()) }
        }
      } else {
        // Simple term + variations
        const escapeRe = (s) => ('' + s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
        let terms = [term].filter(Boolean)
        if (variations) {
          const varArray = Array.isArray(variations) 
            ? variations 
            : variations.split(',').map(v => v.trim()).filter(Boolean)
          terms = [...terms, ...varArray.filter(Boolean)]
        }
        if (terms.length === 0) {
          setIsAnalyzing(false)
          return
        }
        const pattern = terms.map(escapeRe).join('|')
        const regex = wholeWords ? new RegExp(`\\b(${pattern})\\b`, 'gi') : new RegExp(`(${pattern})`, 'gi')
        matchFn = (text) => {
          regex.lastIndex = 0
          const matches = text.match(regex)
          if (!matches) return 0
          return { count: matches.length, matched: matches.map(m => m.toLowerCase()) }
        }
      }

      const counts = Object.create(null)
      const total = totalChunks || 0
      if (!total || total === 0) {
        setAnalysisProgress({ status: 'No data available', percent: 0, error: true })
        setIsAnalyzing(false)
        return
      }

      setAnalysisProgress({ status: 'Searching transcripts...', percent: 10 })

      let processedChunks = 0
      for (let i = 0; i < total; i += BATCH) {
        const promises = []
        for (let j = i; j < Math.min(i + BATCH, total); j++) {
          promises.push(
            fetch(`${apiPrefix}text_chunk_${j}.json`)
              .then(r => r.ok ? r.json() : [])
              .catch(() => [])
          )
        }
        const results = await Promise.all(promises)
        
        for (const chunk of results) {
          for (const item of chunk) {
            try {
              const result = matchFn(item.text)
              if (result && result.count > 0) {
                counts[item.id] = (counts[item.id] || 0) + result.count
                if (result.matched) {
                  for (const m of result.matched) {
                    termCounts[m] = (termCounts[m] || 0) + 1
                  }
                }
              }
            } catch (e) {}
          }
          processedChunks++
        }
        
        const pct = Math.round((processedChunks / total) * 100)
        setAnalysisProgress({ status: 'Searching transcripts...', percent: 10 + Math.round(pct * 0.85), detail: `${processedChunks} of ${total} chunks` })
        await new Promise(r => setTimeout(r, 5))
      }

      setAnalysisProgress({ status: 'Counting matches...', percent: 95 })

      const map = new Map()
      for (const id in counts) {
        if (Object.hasOwn(counts, id)) {
          map.set(id, counts[id])
        }
      }

      setCustomCounts(map)
      setActiveTerm(term)
      setActiveRegex(rawRegex && rawRegex.trim() ? rawRegex : searchInput)

      // Process term counts
      const termEntries = []
      for (const t in termCounts) {
        if (Object.hasOwn(termCounts, t)) {
          termEntries.push({ term: t, count: termCounts[t] })
        }
      }
      termEntries.sort((a, b) => b.count - a.count)
      setMatchedTerms(termEntries.slice(0, 50))
      
      setIsAnalyzing(false)
      setAnalysisProgress({ status: 'Done!', percent: 100 })
    } catch (e) {
      console.error('Search failed', e)
      setIsAnalyzing(false)
      setAnalysisProgress({ status: 'Search failed', percent: 0, error: true })
    }
  }

  // Table data
  const processedTableData = useMemo(() => {
    let data = [...filteredData]
    if (tableFilters.date) data = data.filter(s => s.date.includes(tableFilters.date))
    if (tableFilters.venue) data = data.filter(s => s.venue && s.venue.toLowerCase().includes(tableFilters.venue.toLowerCase()))
    if (tableFilters.title) data = data.filter(s => s.title.toLowerCase().includes(tableFilters.title.toLowerCase()))
    
    // Global table search filter
    if (tableFilters.search) {
      const term = tableFilters.search.toLowerCase()
      data = data.filter(s => 
        (s.title || '').toLowerCase().includes(term) ||
        (s.venue || '').toLowerCase().includes(term) ||
        (s.location || '').toLowerCase().includes(term) ||
        (s.date || '').toLowerCase().includes(term)
      )
    }
    
    // Sort
    if (sortConfig.key) {
      data.sort((a, b) => {
        let aVal = a[sortConfig.key]
        let bVal = b[sortConfig.key]
        if (typeof aVal === 'string') aVal = aVal.toLowerCase()
        if (typeof bVal === 'string') bVal = bVal.toLowerCase()
        if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1
        if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1
        return 0
      })
    }
    return data
  }, [filteredData, tableFilters, sortConfig])

  // 8. Export search results to CSV
  const exportSearchResults = useCallback(() => {
    if (!processedTableData || processedTableData.length === 0) {
      alert('No results to export')
      return
    }
    const headers = ['Date', 'Title', 'Venue', 'Words', 'Matches', 'Duration (min)']
    const rows = processedTableData.map(s => [
      s.date || '',
      `"${(s.title || '').replace(/"/g, '""')}"`,
      `"${(s.venue || '').replace(/"/g, '""')}"`,
      s.wordCount || 0,
      s.mentionCount || 0,
      Math.round((s.wordCount || 0) / WORDS_PER_MINUTE)
    ])
    const csv = [headers.join(','), ...rows.map(r => r.join(','))].join('\n')
    const blob = new Blob([csv], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `wmb_search_results_${(activeTerm || 'all').replace(/[^a-zA-Z0-9]/g, '_')}_${new Date().toISOString().split('T')[0]}.csv`
    a.click()
    URL.revokeObjectURL(url)
  }, [processedTableData, activeTerm])

  // Clear filters
  const clearAllFilters = () => {
    setSelTitles(options.titles)
    setSelYears(options.years)
    setSelVenues(options.venues)
    setSelTopics(options.topics)
  }

  const hasActiveFilters = selTitles.length !== options.titles.length || selYears.length !== options.years.length || selVenues.length !== options.venues.length || selTopics.length !== options.topics.length

  // Loading screen
  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-amber-50 to-orange-100">
        <div className="text-center p-8 bg-white rounded-2xl shadow-xl border border-amber-200 max-w-md w-full mx-4">
          <div className="w-16 h-16 border-4 border-amber-200 border-t-amber-600 rounded-full animate-spin mx-auto mb-6"></div>
          <h2 className="text-2xl font-bold text-amber-800 mb-2">Loading Branham Archive</h2>
          <p className="text-amber-600 text-sm">1,181 sermons • 17.5M words • 1947-1965</p>
        </div>
      </div>
    )
  }

  // Error screen
  if (error) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-red-50 p-4">
        <div className="bg-white p-8 rounded-xl shadow-xl border border-red-200 max-w-lg w-full text-center">
          <h2 className="text-2xl font-bold text-red-600 mb-4">Failed to Load</h2>
          <p className="text-gray-600 mb-4">{error}</p>
          <button onClick={() => window.location.reload()} className="bg-red-600 text-white font-bold py-2 px-6 rounded hover:bg-red-700 transition">Retry</button>
        </div>
      </div>
    )
  }

  return (
    <ErrorBoundary>
      <div className="min-h-screen bg-gradient-to-br from-amber-50 via-orange-50 to-yellow-50">
        {/* Header */}
        <div className="bg-gradient-to-r from-amber-700 to-orange-600 text-white shadow-lg sticky top-0 z-40">
          <div className="max-w-7xl mx-auto px-4 py-3 flex flex-col md:flex-row justify-between items-start md:items-center gap-3">
            <div className="flex items-center gap-3 min-w-0">
              <div className="bg-white/20 backdrop-blur p-2 rounded-lg shadow-lg flex-shrink-0">
                <Icon name="book" className="text-white" />
              </div>
              <div className="min-w-0 flex-1">
                <h1 className="font-bold text-base md:text-lg text-white leading-tight truncate">William Branham Sermon Archive</h1>
                <p className="text-xs text-amber-100 truncate">{yearRange} • {totalSermons.toLocaleString()} sermons • {(totalWords / 1000000).toFixed(1)}M words</p>
              </div>
            </div>
            <div className="flex flex-col md:flex-row items-stretch md:items-center gap-3 md:gap-4 w-full md:w-auto">
              {dataDate && <div className="text-xs text-amber-200 text-center md:text-right">Data: {dataDate}</div>}
              <div className="flex bg-white/10 backdrop-blur p-1 rounded-lg w-full md:w-auto">
                {[{key:'dashboard',label:'Search & Stats'},{key:'data',label:'Sources'},{key:'about',label:'About'}].map(tab => (
                  <button key={tab.key} onClick={() => setView(tab.key)} className={`px-3 md:px-4 py-1.5 rounded-md text-xs md:text-sm font-medium transition flex-1 md:flex-none ${view === tab.key ? 'bg-white text-amber-700 shadow-sm' : 'text-amber-100 hover:text-white hover:bg-white/10'}`}>{tab.label}</button>
                ))}
              </div>
              {onSwitchToMain && (
                <button onClick={onSwitchToMain} className="px-3 py-1.5 bg-white/10 hover:bg-white/20 rounded-md text-xs font-medium text-amber-100 hover:text-white transition">
                  ← Back to Main
                </button>
              )}
            </div>
          </div>
        </div>

        {/* Info banner */}
        <div className="bg-amber-100 border-b border-amber-200 text-amber-800 text-xs text-center py-2 px-4">
          <span className="font-bold">Source:</span> Transcripts from <a href="https://www.livingwordbroadcast.org/wbtextindex" target="_blank" rel="noopener noreferrer" className="text-amber-700 hover:text-amber-900 underline">Living Word Broadcast</a> • Click any sermon to read the full transcript • <kbd className="px-1 py-0.5 bg-amber-200 rounded text-xs">⌘K</kbd> to search
        </div>

        {/* Main content */}
        <div className="max-w-7xl mx-auto px-4 py-6">
          
          {/* Filters */}
          {view === 'dashboard' && (
            <div className="bg-white p-4 rounded-xl border shadow-sm mb-6">
              <div className="flex flex-wrap items-center justify-between gap-2 mb-3">
                <h3 className="font-bold text-gray-800 flex items-center gap-2 text-sm">
                  <Icon name="filter" size={16} /> Filters
                </h3>
                <div className="flex items-center gap-2">
                  {hasActiveFilters && (
                    <button onClick={clearAllFilters} className="text-xs text-amber-600 hover:text-amber-800 font-medium">
                      Clear all filters
                    </button>
                  )}
                </div>
              </div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                <MultiSelect label="Years" options={options.years} selected={selYears} onChange={setSelYears} />
                <MultiSelect label="Venues" options={options.venues} selected={selVenues} onChange={setSelVenues} />
                <MultiSelect label="Topics" options={options.topics} selected={selTopics} onChange={setSelTopics} />
                <MultiSelect label="Titles" options={options.titles} selected={selTitles} onChange={setSelTitles} />
              </div>
              
              {/* Continue Reading */}
              {Object.keys(readingProgress).length > 0 && (
                <div className="mt-4 pt-4 border-t">
                  <h4 className="text-sm font-bold text-gray-700 flex items-center gap-1 mb-2">
                    <Icon name="clock" size={14} className="text-blue-500" /> Continue Reading
                  </h4>
                  <div className="flex flex-wrap gap-2">
                    {Object.entries(readingProgress)
                      .sort((a, b) => b[1].timestamp - a[1].timestamp)
                      .slice(0, 5)
                      .map(([sermonId, progress]) => {
                        const sermon = rawData.find(s => s.id === sermonId)
                        if (!sermon) return null
                        return (
                          <button
                            key={sermonId}
                            onClick={() => { setSelectedSermon({ ...sermon, resumePosition: progress.position }); setSelectedSermonFocus(0) }}
                            className="px-2 py-1 bg-blue-50 hover:bg-blue-100 border border-blue-200 rounded text-xs text-blue-800 flex items-center gap-1 max-w-xs"
                          >
                            <Icon name="arrowRight" size={12} />
                            <span className="truncate">{sermon.title}</span>
                            <span className="text-blue-500">({Math.round(progress.position * 100)}%)</span>
                          </button>
                        )
                      })}
                  </div>
                </div>
              )}
            </div>
          )}

          {view === 'dashboard' && stats && (
            <>
              {/* Venue breakdown bar chart / heatmap */}
              <div className="bg-white rounded-xl border shadow-sm mb-6">
                <button
                  onClick={() => setChartsCollapsed(!chartsCollapsed)}
                  className={`w-full flex items-center justify-between p-4 transition-colors ${chartsCollapsed ? 'bg-gradient-to-r from-amber-50 to-orange-50 hover:from-amber-100 hover:to-orange-100' : 'hover:bg-gray-50'}`}
                >
                  <div className="flex items-center gap-2">
                    <Icon name={chartsCollapsed ? 'chevronRight' : 'chevronDown'} size={18} className={chartsCollapsed ? 'text-amber-600' : 'text-gray-500'} />
                    <h3 className={`font-bold ${chartsCollapsed ? 'text-amber-800' : 'text-gray-800'}`}>
                      <Icon name="barChart" className="inline mr-1" size={16} /> Sermons by Venue
                    </h3>
                    <span className="text-sm text-gray-500">({venueBarData.length} venues)</span>
                  </div>
                  <span className={`text-sm font-medium ${chartsCollapsed ? 'text-amber-600 bg-amber-100 px-3 py-1 rounded-full' : 'text-xs text-gray-400'}`}>
                    {chartsCollapsed ? '▶ Click to view' : 'Click to collapse'}
                  </span>
                </button>
                {!chartsCollapsed && (
                  <div className="p-4 pt-2">
                    {/* Tab selector */}
                    <div className="flex gap-2 mb-3">
                      <button
                        onClick={() => setVenueTab('bars')}
                        className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors ${venueTab === 'bars' ? 'bg-amber-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}`}
                      >
                        📊 Bar Chart
                      </button>
                      <button
                        onClick={() => setVenueTab('heatmap')}
                        className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors ${venueTab === 'heatmap' ? 'bg-amber-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}`}
                      >
                        🗓️ Venue × Year
                      </button>
                      <button
                        onClick={() => setVenueTab('location')}
                        className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors ${venueTab === 'location' ? 'bg-amber-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}`}
                      >
                        📍 Location × Year
                      </button>
                    </div>
                    
                    {/* Bar Chart View */}
                    {venueTab === 'bars' && venueBarData.length > 0 && (() => {
                      const maxCount = Math.max(...venueBarData.map(d => d.count))
                      const maxTick = Math.ceil(maxCount / 50) * 50
                      const ticks = []
                      for (let i = 0; i <= maxTick; i += 50) ticks.push(i)
                      return (
                        <>
                          <p className="text-xs text-gray-500 mb-3">Click a bar to filter sermons by that venue. Darker bars = more sermons.</p>
                      <div style={{ height: Math.max(400, venueBarData.length * 20) }}>
                        <ResponsiveContainer width="100%" height="100%">
                          <BarChart data={venueBarData} layout="vertical" margin={{ top: 10, right: 30, left: 5, bottom: 5 }} barSize={14}>
                            <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                            <XAxis 
                              type="number" 
                              tickFormatter={v => v.toLocaleString()} 
                              orientation="top"
                              axisLine={false}
                              tickLine={false}
                              tick={{ fontSize: 10 }}
                              ticks={ticks}
                              domain={[0, maxTick]}
                            />
                            <YAxis 
                              type="category" 
                              dataKey="venue" 
                              width={typeof window !== 'undefined' && window.innerWidth < 640 ? 160 : 280} 
                              tick={{ fontSize: typeof window !== 'undefined' && window.innerWidth < 640 ? 9 : 11 }} 
                              interval={0}
                            />
                            <Tooltip 
                              content={({ active, payload }) => {
                                if (!active || !payload || !payload.length) return null
                                const d = payload[0].payload
                                return (
                                  <div className="bg-white border rounded-lg shadow-lg p-3 text-sm max-w-xs">
                                    <p className="font-bold text-gray-800 mb-2">{d.venue}</p>
                                    <div className="space-y-1">
                                      <p className="text-amber-600">📊 Sermons: <span className="font-semibold">{d.count.toLocaleString()}</span> <span className="text-gray-400">({d.percent}%)</span></p>
                                      <p className="text-gray-600">📅 Years: {d.dateRange}</p>
                                      <p className="text-blue-600">📝 Words: <span className="font-semibold">{d.totalWords.toLocaleString()}</span></p>
                                    </div>
                                    <p className="text-xs text-gray-400 mt-2 italic">Click to browse sermons</p>
                                  </div>
                                )
                              }}
                            />
                            <Bar dataKey="count" radius={[0, 4, 4, 0]}>
                              {venueBarData.map((entry, index) => {
                                // Gradient: darker amber for more sermons
                                const ratio = 1 - (index / (venueBarData.length - 1 || 1))
                                const r = Math.round(254 + (180 - 254) * ratio)
                                const g = Math.round(243 + (83 - 243) * ratio)
                                const b = Math.round(199 + (9 - 199) * ratio)
                                const gradientColor = `rgb(${r},${g},${b})`
                                const isSelected = browseVenue === entry.venue
                                return (
                                  <Cell 
                                    key={entry.venue} 
                                    fill={isSelected ? '#d97706' : gradientColor}
                                    stroke={isSelected ? '#92400e' : 'none'}
                                    strokeWidth={isSelected ? 2 : 0}
                                    style={{ cursor: 'pointer' }}
                                    onClick={() => setBrowseVenue(entry.venue)}
                                  />
                                )
                              })}
                            </Bar>
                          </BarChart>
                        </ResponsiveContainer>
                      </div>
                        </>
                      )
                    })()}
                    
                    {/* Heatmap View */}
                    {venueTab === 'heatmap' && venueYearHeatmap && (
                      <div className="overflow-x-auto">
                        <p className="text-xs text-gray-500 mb-3">Sermon distribution by venue and year. Darker = more sermons. Click to filter.</p>
                        <div 
                          className="grid border border-gray-300 rounded overflow-hidden" 
                          style={{ 
                            gridTemplateColumns: `280px repeat(${venueYearHeatmap.years.length}, minmax(32px, 1fr)) 50px`,
                            minWidth: `${280 + venueYearHeatmap.years.length * 36 + 50}px`
                          }}
                        >
                          {/* Header row */}
                          <div className="bg-gray-100 p-1.5 text-xs font-bold text-gray-700 sticky left-0 z-10 border-b border-r border-gray-300">
                            Venue
                          </div>
                          {venueYearHeatmap.years.map(year => (
                            <div key={year} className="bg-gray-100 p-1.5 text-xs font-bold text-gray-700 text-center border-b border-r border-gray-300">
                              {year.toString().slice(-2)}
                            </div>
                          ))}
                          <div className="bg-gray-100 p-1.5 text-xs font-bold text-gray-700 text-center border-b border-gray-300">
                            Total
                          </div>
                          
                          {/* Data rows */}
                          {venueYearHeatmap.venues.map((venue, vi) => (
                            <React.Fragment key={venue}>
                              <div 
                                className={`bg-white p-1.5 text-xs truncate sticky left-0 z-10 cursor-pointer hover:bg-amber-50 border-b border-r border-gray-200 ${selVenues.includes(venue) ? 'bg-amber-100 font-semibold' : ''}`}
                                title={`${venue} (${venueYearHeatmap.venueTotals.get(venue) || 0} sermons)`}
                                onClick={() => {
                                  if (selVenues.includes(venue)) {
                                    setSelVenues(selVenues.filter(v => v !== venue))
                                  } else {
                                    setSelVenues([...selVenues, venue])
                                  }
                                }}
                              >
                                {venue}
                              </div>
                              {venueYearHeatmap.years.map(year => {
                                const count = venueYearHeatmap.data.get(`${venue}|${year}`) || 0
                                const intensity = count > 0 ? Math.min(0.15 + (count / venueYearHeatmap.maxCount) * 0.85, 1) : 0
                                return (
                                  <div 
                                    key={`${venue}-${year}`}
                                    className="p-1.5 text-center text-xs cursor-pointer transition-colors hover:ring-1 hover:ring-amber-400 border-b border-r border-gray-200"
                                    style={{ 
                                      backgroundColor: count > 0 ? `rgba(217, 119, 6, ${intensity})` : 'white',
                                      color: intensity > 0.5 ? 'white' : (count > 0 ? '#92400e' : '#9ca3af')
                                    }}
                                    title={`${venue} (${year}): ${count} sermon${count !== 1 ? 's' : ''}`}
                                    onClick={() => {
                                      // Filter to this venue and year
                                      setSelVenues([venue])
                                      setSelYears([year.toString()])
                                    }}
                                  >
                                    {count > 0 ? count : '·'}
                                  </div>
                                )
                              })}
                              {/* Total column */}
                              <div 
                                className="p-1.5 text-center text-xs font-semibold bg-gray-50 border-b border-gray-200 text-gray-700"
                                title={`${venue}: ${venueYearHeatmap.venueTotals.get(venue) || 0} total sermons`}
                              >
                                {venueYearHeatmap.venueTotals.get(venue) || 0}
                              </div>
                            </React.Fragment>
                          ))}
                        </div>
                        
                        {/* Legend */}
                        <div className="flex items-center justify-end gap-2 mt-3 text-xs text-gray-500">
                          <span>Fewer</span>
                          <div className="flex">
                            {[0.15, 0.35, 0.55, 0.75, 0.95].map((intensity, i) => (
                              <div 
                                key={i} 
                                className="w-5 h-4" 
                                style={{ backgroundColor: `rgba(217, 119, 6, ${intensity})` }}
                              />
                            ))}
                          </div>
                          <span>More sermons</span>
                        </div>
                      </div>
                    )}

                    {/* Location Heatmap View */}
                    {venueTab === 'location' && locationYearHeatmap && (
                      <div className="overflow-x-auto">
                        <p className="text-xs text-gray-500 mb-3">Sermon distribution by location (city) and year. Darker = more sermons. Click to filter.</p>
                        <div 
                          className="grid border border-gray-300 rounded overflow-hidden" 
                          style={{ 
                            gridTemplateColumns: `200px repeat(${locationYearHeatmap.years.length}, minmax(32px, 1fr)) 50px`,
                            minWidth: `${200 + locationYearHeatmap.years.length * 36 + 50}px`
                          }}
                        >
                          {/* Header row */}
                          <div className="bg-gray-100 p-1.5 text-xs font-bold text-gray-700 sticky left-0 z-10 border-b border-r border-gray-300">
                            Location
                          </div>
                          {locationYearHeatmap.years.map(year => (
                            <div key={year} className="bg-gray-100 p-1.5 text-xs font-bold text-gray-700 text-center border-b border-r border-gray-300">
                              {year.toString().slice(-2)}
                            </div>
                          ))}
                          <div className="bg-gray-100 p-1.5 text-xs font-bold text-gray-700 text-center border-b border-gray-300">
                            Total
                          </div>
                          
                          {/* Data rows */}
                          {locationYearHeatmap.locations.map((location, li) => (
                            <React.Fragment key={location}>
                              <div 
                                className={`bg-white p-1.5 text-xs truncate sticky left-0 z-10 cursor-pointer hover:bg-amber-50 border-b border-r border-gray-200`}
                                title={`${location} (${locationYearHeatmap.locationTotals.get(location) || 0} sermons)`}
                                onClick={() => {
                                  // Filter sermons by this location
                                  setTableFilters(prev => ({ ...prev, search: location }))
                                }}
                              >
                                {location}
                              </div>
                              {locationYearHeatmap.years.map(year => {
                                const count = locationYearHeatmap.data.get(`${location}|${year}`) || 0
                                const intensity = count > 0 ? Math.min(0.15 + (count / locationYearHeatmap.maxCount) * 0.85, 1) : 0
                                return (
                                  <div 
                                    key={`${location}-${year}`}
                                    className="p-1.5 text-center text-xs cursor-pointer transition-colors hover:ring-1 hover:ring-amber-400 border-b border-r border-gray-200"
                                    style={{ 
                                      backgroundColor: count > 0 ? `rgba(16, 185, 129, ${intensity})` : 'white',
                                      color: intensity > 0.5 ? 'white' : (count > 0 ? '#065f46' : '#9ca3af')
                                    }}
                                    title={`${location} (${year}): ${count} sermon${count !== 1 ? 's' : ''}`}
                                    onClick={() => {
                                      // Filter to this location and year
                                      setTableFilters(prev => ({ ...prev, search: location }))
                                      setSelYears([year.toString()])
                                    }}
                                  >
                                    {count > 0 ? count : '·'}
                                  </div>
                                )
                              })}
                              {/* Total column */}
                              <div 
                                className="p-1.5 text-center text-xs font-semibold bg-gray-50 border-b border-gray-200 text-gray-700"
                                title={`${location}: ${locationYearHeatmap.locationTotals.get(location) || 0} total sermons`}
                              >
                                {locationYearHeatmap.locationTotals.get(location) || 0}
                              </div>
                            </React.Fragment>
                          ))}
                        </div>
                        
                        {/* Legend */}
                        <div className="flex items-center justify-end gap-2 mt-3 text-xs text-gray-500">
                          <span>Fewer</span>
                          <div className="flex">
                            {[0.15, 0.35, 0.55, 0.75, 0.95].map((intensity, i) => (
                              <div 
                                key={i} 
                                className="w-5 h-4" 
                                style={{ backgroundColor: `rgba(16, 185, 129, ${intensity})` }}
                              />
                            ))}
                          </div>
                          <span>More sermons</span>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </div>

              {/* Search History */}
              {searchHistory.length > 0 && (
                <div className="flex flex-wrap items-center gap-2 mb-3">
                  <span className="text-xs text-gray-500">Recent:</span>
                  {searchHistory.slice(0, 8).map((h, i) => (
                    <button
                      key={i}
                      onClick={() => { setSearchFieldTerm(h.term); handleAnalysis(h.term, '', null, { wholeWords: true }) }}
                      className="px-2 py-0.5 bg-gray-100 hover:bg-gray-200 text-gray-600 rounded text-xs transition"
                    >
                      {h.term.length > 20 ? h.term.slice(0, 20) + '...' : h.term}
                    </button>
                  ))}
                  <button
                    onClick={() => { if (confirm('Clear search history?')) setSearchHistory([]) }}
                    className="text-xs text-gray-400 hover:text-red-500"
                  >
                    ✕
                  </button>
                </div>
              )}

              {/* Topic Analyzer */}
              <TopicAnalyzer 
                onAnalyze={handleAnalysis} 
                isAnalyzing={isAnalyzing} 
                progress={analysisProgress} 
                initialTerm="" 
                initialVariations="" 
                matchedTerms={matchedTerms} 
                totalTranscripts={totalSermons}
                defaultTerm=""
                defaultRegex=""
                externalTerm={searchFieldTerm}
              />

              {/* Quick Topic Buttons */}
              <div className="flex flex-wrap gap-2 mb-6">
                <span className="text-sm text-gray-500 font-medium self-center mr-2">Featured Series:</span>
                <button
                  onClick={() => {
                    // Filter to Seven Seals series (March 17-24, 1963)
                    const sealsSermons = [
                      '63-0317E', '63-0318', '63-0319', '63-0320', 
                      '63-0321', '63-0322', '63-0323', '63-0324E'
                    ]
                    const sealsTitles = rawData
                      .filter(s => sealsSermons.some(code => s.dateCode === code || s.id?.startsWith(code)))
                      .map(s => s.title)
                    if (sealsTitles.length > 0) {
                      setSelTitles(sealsTitles)
                      setSelYears(['1963'])
                    }
                  }}
                  className="px-3 py-1.5 bg-amber-100 hover:bg-amber-200 text-amber-800 rounded-lg text-sm font-medium transition flex items-center gap-1.5"
                >
                  <Icon name="book" size={14} /> The Seven Seals
                </button>
                <button
                  onClick={() => {
                    // Filter to Seven Church Ages series (December 5-11, 1960)
                    const ageSermons = [
                      '60-1205', '60-1206', '60-1207', '60-1208',
                      '60-1209', '60-1210', '60-1211E'
                    ]
                    const ageTitles = rawData
                      .filter(s => ageSermons.some(code => s.dateCode === code || s.id?.startsWith(code)))
                      .map(s => s.title)
                    if (ageTitles.length > 0) {
                      setSelTitles(ageTitles)
                      setSelYears(['1960'])
                    }
                  }}
                  className="px-3 py-1.5 bg-indigo-100 hover:bg-indigo-200 text-indigo-800 rounded-lg text-sm font-medium transition flex items-center gap-1.5"
                >
                  <Icon name="book" size={14} /> The Seven Church Ages
                </button>
                {(selTitles.length !== options.titles.length || selYears.length !== options.years.length) && (
                  <button
                    onClick={() => {
                      setSelTitles(options.titles)
                      setSelYears(options.years)
                    }}
                    className="px-3 py-1.5 bg-gray-100 hover:bg-gray-200 text-gray-600 rounded-lg text-sm font-medium transition flex items-center gap-1.5"
                  >
                    <Icon name="x" size={14} /> Show All Sermons
                  </button>
                )}
              </div>

              {/* Hot Topics Search Buttons */}
              <div className="flex flex-wrap gap-2 mb-6">
                <span className="text-sm text-gray-500 font-medium self-center mr-2">Hot Topics:</span>
                <button
                  onClick={() => { setSearchFieldTerm('"THUS SAITH THE LORD"'); handleAnalysis('"THUS SAITH THE LORD"', '', null, { wholeWords: true }) }}
                  className="px-3 py-1.5 bg-red-100 hover:bg-red-200 text-red-800 rounded-lg text-sm font-medium transition flex items-center gap-1.5"
                >
                  <Icon name="search" size={14} /> "THUS SAITH THE LORD"
                </button>
                <button
                  onClick={() => { setSearchFieldTerm('faith'); handleAnalysis('faith', '', null, { wholeWords: true }) }}
                  className="px-2.5 py-1 bg-purple-100 hover:bg-purple-200 text-purple-800 rounded-lg text-xs font-medium transition"
                >
                  Faith
                </button>
                <button
                  onClick={() => { setSearchFieldTerm('angel'); handleAnalysis('angel', '', null, { wholeWords: true }) }}
                  className="px-2.5 py-1 bg-blue-100 hover:bg-blue-200 text-blue-800 rounded-lg text-xs font-medium transition"
                >
                  Angel
                </button>
                <button
                  onClick={() => { setSearchFieldTerm('"Holy Ghost" OR "Holy Spirit"'); handleAnalysis('"Holy Ghost" OR "Holy Spirit"', '', null, { wholeWords: true }) }}
                  className="px-2.5 py-1 bg-sky-100 hover:bg-sky-200 text-sky-800 rounded-lg text-xs font-medium transition"
                >
                  Holy Ghost
                </button>
                <button
                  onClick={() => { setSearchFieldTerm('"divine healing" OR healed OR healing'); handleAnalysis('"divine healing" OR healed OR healing', '', null, { wholeWords: true }) }}
                  className="px-2.5 py-1 bg-green-100 hover:bg-green-200 text-green-800 rounded-lg text-xs font-medium transition"
                >
                  Divine Healing
                </button>
                <button
                  onClick={() => { setSearchFieldTerm('prophet'); handleAnalysis('prophet', '', null, { wholeWords: true }) }}
                  className="px-2.5 py-1 bg-orange-100 hover:bg-orange-200 text-orange-800 rounded-lg text-xs font-medium transition"
                >
                  Prophet
                </button>
                <button
                  onClick={() => { setSearchFieldTerm('"end time" OR "last days"'); handleAnalysis('"end time" OR "last days"', '', null, { wholeWords: true }) }}
                  className="px-2.5 py-1 bg-rose-100 hover:bg-rose-200 text-rose-800 rounded-lg text-xs font-medium transition"
                >
                  End Time
                </button>
                <button
                  onClick={() => { setSearchFieldTerm('baptism OR baptized OR baptize'); handleAnalysis('baptism OR baptized OR baptize', '', null, { wholeWords: true }) }}
                  className="px-2.5 py-1 bg-cyan-100 hover:bg-cyan-200 text-cyan-800 rounded-lg text-xs font-medium transition"
                >
                  Baptism
                </button>
                <button
                  onClick={() => { setSearchFieldTerm('bride'); handleAnalysis('bride', '', null, { wholeWords: true }) }}
                  className="px-2.5 py-1 bg-pink-100 hover:bg-pink-200 text-pink-800 rounded-lg text-xs font-medium transition"
                >
                  Bride
                </button>
                <button
                  onClick={() => { setSearchFieldTerm('"serpent\'s seed" OR "serpent seed"'); handleAnalysis('"serpent\'s seed" OR "serpent seed"', '', null, { wholeWords: true }) }}
                  className="px-2.5 py-1 bg-lime-100 hover:bg-lime-200 text-lime-800 rounded-lg text-xs font-medium transition"
                >
                  Serpent's Seed
                </button>
                <button
                  onClick={() => { setSearchFieldTerm('godhead OR trinity OR "oneness"'); handleAnalysis('godhead OR trinity OR "oneness"', '', null, { wholeWords: true }) }}
                  className="px-2.5 py-1 bg-violet-100 hover:bg-violet-200 text-violet-800 rounded-lg text-xs font-medium transition"
                >
                  Godhead
                </button>
                <button
                  onClick={() => { setSearchFieldTerm('rapture'); handleAnalysis('rapture', '', null, { wholeWords: true }) }}
                  className="px-2.5 py-1 bg-fuchsia-100 hover:bg-fuchsia-200 text-fuchsia-800 rounded-lg text-xs font-medium transition"
                >
                  Rapture
                </button>
                <button
                  onClick={() => { setSearchFieldTerm('"third pull"'); handleAnalysis('"third pull"', '', null, { wholeWords: true }) }}
                  className="px-2.5 py-1 bg-teal-100 hover:bg-teal-200 text-teal-800 rounded-lg text-xs font-medium transition"
                >
                  Third Pull
                </button>
                <button
                  onClick={() => { setSearchFieldTerm('"seven thunders"'); handleAnalysis('"seven thunders"', '', null, { wholeWords: true }) }}
                  className="px-2.5 py-1 bg-indigo-100 hover:bg-indigo-200 text-indigo-800 rounded-lg text-xs font-medium transition"
                >
                  Seven Thunders
                </button>
                <button
                  onClick={() => { setSearchFieldTerm('revelation OR revelations'); handleAnalysis('revelation OR revelations', '', null, { wholeWords: true }) }}
                  className="px-2.5 py-1 bg-emerald-100 hover:bg-emerald-200 text-emerald-800 rounded-lg text-xs font-medium transition"
                >
                  Revelation
                </button>
                {activeTerm && (
                  <button
                    onClick={() => {
                      setActiveTerm('')
                      setActiveRegex('')
                      setCustomCounts(null)
                      setMatchedTerms([])
                      setSearchFieldTerm('')
                    }}
                    className="px-2.5 py-1 bg-gray-200 hover:bg-gray-300 text-gray-700 rounded-lg text-xs font-medium transition flex items-center gap-1"
                  >
                    <Icon name="x" size={12} /> Clear Search
                  </button>
                )}
              </div>

              {/* No Results banner */}
              {customCounts && stats.totalMentions === 0 && !isAnalyzing && (
                <div className="bg-amber-50 border border-amber-200 rounded-xl p-4 mb-6 flex items-start gap-3">
                  <div className="flex-shrink-0 w-10 h-10 bg-amber-100 rounded-full flex items-center justify-center">
                    <Icon name="alertCircle" size={20} className="text-amber-600" />
                  </div>
                  <div className="flex-1">
                    <h3 className="font-bold text-amber-800 text-sm">No Results Found</h3>
                    <p className="text-amber-700 text-xs mt-1">
                      Your search for "<span className="font-semibold">{activeTerm}</span>" did not match any transcripts.
                    </p>
                  </div>
                </div>
              )}

              {/* Stats cards */}
              <div className="grid grid-cols-3 md:grid-cols-6 gap-1.5 sm:gap-2 mb-6">
                <StatCard title="Sermons" value={stats.totalSermons.toLocaleString()} icon="book" color="amber" sub={`of ${totalSermons.toLocaleString()} total`} />
                {activeTerm && (
                  <StatCard title={`"${activeTerm}" Matches`} value={stats.totalMentions.toLocaleString()} icon="search" color={stats.totalMentions === 0 ? "red" : "green"} sub="in filtered results" />
                )}
                <StatCard title="Total Words" value={`${(stats.totalWords / 1000000).toFixed(1)}M`} icon="fileText" color="blue" sub="in filtered sermons" />
                <StatCard title="Total Hours" value={Math.round(stats.totalDuration / 60).toLocaleString()} icon="clock" color="purple" sub="of preaching" />
                {activeTerm && (
                  <StatCard title="Avg Mentions" value={stats.avg} icon="barChart" color="indigo" sub="per sermon (filtered)" />
                )}
                {stats.peakSermon && activeTerm && (
                  <StatCard 
                    title="Peak Count" 
                    value={stats.maxMentions} 
                    icon="activity" 
                    color="orange" 
                    sub="Click to view transcript mentions"
                    onClick={() => { setSelectedSermon({ ...stats.peakSermon, searchTerm: activeRegex || activeTerm }); setSelectedSermonFocus(0); }}
                  />
                )}
              </div>

              {/* Main timeline chart */}
              <div className="bg-white p-3 sm:p-6 rounded-xl border shadow-sm h-[350px] sm:h-[450px] mb-6 relative">
                <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-2 mb-4">
                  <div>
                    <h3 className="font-bold text-gray-800 flex items-center gap-2 text-sm sm:text-base">
                      <Icon name="activity" /> Sermon Timeline (1947-1965)
                    </h3>
                    <p className="text-xs text-gray-500 mt-1">
                      {activeTerm ? `Showing "${activeTerm}" mentions over time` : 'Sermon distribution over time'} • Click a point to browse
                    </p>
                  </div>
                  <div className="flex items-center gap-2 flex-wrap">
                    {activeTerm && (
                      <button
                        onClick={() => {
                          setActiveTerm('')
                          setActiveRegex('')
                          setCustomCounts(null)
                          setMatchedTerms([])
                        }}
                        className="text-xs bg-red-100 text-red-700 hover:bg-red-200 px-3 py-1.5 rounded-lg font-medium flex items-center gap-1 transition-colors"
                      >
                        <Icon name="x" size={12} /> Clear Search
                      </button>
                    )}
                    <span className="text-xs text-gray-500">Smoothing:</span>
                    <div className="flex bg-gray-100 rounded-lg p-1">
                      {[{ w: 4, l: '1 Mo' }, { w: 12, l: '3 Mo' }, { w: 26, l: '6 Mo' }].map(({ w, l }) => (
                        <button key={w} onClick={() => setAggregateWindow(w)} className={`px-2 py-1 text-xs font-medium rounded ${aggregateWindow === w ? 'bg-white shadow text-amber-600' : 'text-gray-500'}`}>{l}</button>
                      ))}
                    </div>
                  </div>
                </div>
                <ResponsiveContainer width="100%" height="85%">
                  <ComposedChart
                    data={aggregatedChartData}
                    onClick={(e, _, event) => {
                      if (e && e.activePayload && e.activePayload[0]) {
                        const bucket = aggregatedChartData.find(d => d.timestamp === e.activePayload[0].payload.timestamp)
                        if (bucket) {
                          const mouseX = event?.clientX || (e.chartX + 100)
                          const mouseY = event?.clientY || (e.chartY + 200)
                          setMainChartPinnedBucket(bucket)
                          setMainChartPinnedPosition({ x: mouseX, y: mouseY })
                        }
                      }
                    }}
                  >
                    <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e5e7eb" />
                    <XAxis dataKey="timestamp" type="number" scale="time" domain={dateDomain} tickFormatter={formatDate} tick={{ fontSize: 10 }} minTickGap={80} />
                    <YAxis tick={{ fontSize: 10 }} label={{ value: activeTerm ? 'Mentions' : 'Sermons', angle: -90, position: 'insideLeft', style: { textAnchor: 'middle', fontSize: 11, fill: '#6b7280' } }} />
                    <Tooltip
                      content={({ active, payload }) => {
                        if (mainChartPinnedBucket) return null
                        if (!active || !payload || !payload.length) return null
                        const p = payload[0].payload
                        const dateLabel = `Week of ${new Date(p.timestamp).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })}`
                        return (
                          <div className="bg-white p-3 rounded-lg shadow-lg text-sm border border-gray-200">
                            <div className="font-bold text-gray-900">{dateLabel}</div>
                            <div className="text-xs text-gray-500 mt-1">
                              {activeTerm ? <>{p.mentionCount} mentions • </> : null}{p.count} sermon{p.count !== 1 ? 's' : ''}
                            </div>
                            <div className="mt-2 text-amber-600 text-xs font-medium">Click to browse →</div>
                          </div>
                        )
                      }}
                    />
                    <Legend wrapperStyle={{ fontSize: '11px', paddingTop: '8px' }} />
                    {activeTerm ? (
                      <>
                        <Area type="monotone" dataKey="mentionCount" name="Mentions" stroke="#f59e0b" fill="#fcd34d" fillOpacity={0.6} isAnimationActive={false} cursor="pointer" />
                        <Line type="monotone" dataKey="rollingAvg" name="Trend" stroke="#d97706" strokeWidth={3} dot={false} isAnimationActive={false} />
                      </>
                    ) : (
                      <>
                        <Area type="monotone" dataKey="count" name="Sermons" stroke="#f59e0b" fill="#fcd34d" fillOpacity={0.6} isAnimationActive={false} cursor="pointer" />
                        <Line type="monotone" dataKey="volumeRollingAvg" name="Trend" stroke="#d97706" strokeWidth={3} dot={false} isAnimationActive={false} />
                      </>
                    )}
                  </ComposedChart>
                </ResponsiveContainer>

                {/* Pinned popup */}
                {mainChartPinnedBucket && (
                  <div
                    ref={mainChartPinnedRef}
                    className="fixed bg-white rounded-xl shadow-2xl border border-gray-200 w-[calc(100%-16px)] sm:w-96 max-h-[60vh] sm:max-h-[400px] flex flex-col z-50"
                    style={{
                      left: typeof window !== 'undefined' ? Math.min(Math.max(20, mainChartPinnedPosition.x + 15), window.innerWidth - 420) : 20,
                      top: typeof window !== 'undefined' ? Math.min(Math.max(20, mainChartPinnedPosition.y - 50), window.innerHeight - 450) : 100
                    }}
                  >
                    <div className="p-4 border-b border-gray-100">
                      <div className="flex justify-between items-start">
                        <div>
                          <div className="font-bold text-gray-900">
                            Week of {new Date(mainChartPinnedBucket.timestamp).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })}
                          </div>
                          <div className="text-sm text-gray-500 mt-1">{mainChartPinnedBucket.sermons?.length || 0} sermon{(mainChartPinnedBucket.sermons?.length || 0) !== 1 ? 's' : ''}</div>
                        </div>
                        <button onClick={() => setMainChartPinnedBucket(null)} className="p-1 hover:bg-gray-100 rounded-full"><Icon name="x" /></button>
                      </div>
                    </div>
                    <div className="text-xs font-medium text-gray-600 px-3 pt-2 pb-1">Click a sermon to view transcript:</div>
                    <div className="flex-1 overflow-auto px-2 pb-2">
                      <div className="space-y-1">
                        {[...(mainChartPinnedBucket.sermons || [])].sort((a, b) => (b.mentionCount || 0) - (a.mentionCount || 0)).map((s, i) => (
                          <button
                            key={s.id || i}
                            onClick={() => { setSelectedSermon({ ...s, searchTerm: activeRegex || activeTerm }); setSelectedSermonFocus(0); setMainChartPinnedBucket(null) }}
                            className="w-full text-left px-2 py-1.5 bg-amber-50 rounded border border-amber-200 hover:bg-amber-100 hover:border-amber-300 transition-all group"
                          >
                            <div className="text-sm font-medium text-gray-900 group-hover:text-amber-700 line-clamp-1">{s.title || 'Untitled'}</div>
                            <div className="flex justify-between items-center text-xs text-gray-500">
                              <span className="truncate max-w-[140px]">{s.venue || 'Unknown'}</span>
                              <div className="flex items-center gap-2 shrink-0">
                                {activeTerm && s.mentionCount > 0 && <span className="text-amber-600 font-medium">{s.mentionCount}</span>}
                                <span className="text-gray-400">{new Date(s.timestamp).toLocaleDateString()}</span>
                              </div>
                            </div>
                          </button>
                        ))}
                      </div>
                    </div>
                  </div>
                )}
              </div>

              {/* Sermon table */}
              <div className="bg-white rounded-xl border shadow-sm mb-8 p-4 sm:p-6 overflow-x-auto w-full">
                <div className="flex flex-col gap-3 mb-4">
                  <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-2">
                    <h3 className="font-bold text-gray-800">Sermon Search Results ({processedTableData.length.toLocaleString()})</h3>
                    <div className="flex items-center gap-2 flex-wrap">
                      {activeTerm && (
                        <div className="text-sm bg-amber-50 text-amber-700 px-3 py-1 rounded font-medium">
                          Searching for: <span className="font-semibold">{activeTerm}</span>
                        </div>
                      )}
                      {/* Download All Transcripts button */}
                      <button
                        onClick={() => downloadTranscriptsZip(processedTableData, `WMB_All_Transcripts_${processedTableData.length}.zip`, `${processedTableData.length} sermons`)}
                        disabled={downloadProgress !== null || processedTableData.length === 0}
                        className="text-xs bg-amber-500 text-white hover:bg-amber-600 px-3 py-1.5 rounded font-medium flex items-center gap-1 disabled:opacity-50"
                        title={`Download all ${processedTableData.length} transcripts as ZIP`}
                      >
                        <Icon name="download" size={12} /> Download All ({processedTableData.length})
                      </button>
                      {/* Export button */}
                      <button
                        onClick={exportSearchResults}
                        className="text-xs bg-green-100 text-green-700 hover:bg-green-200 px-3 py-1.5 rounded font-medium flex items-center gap-1"
                        title="Export search results as CSV"
                      >
                        <Icon name="download" size={12} /> Export CSV
                      </button>
                      <a 
                        href="https://www.livingwordbroadcast.org/wbtextindex" 
                        target="_blank" 
                        rel="noopener noreferrer"
                        className="text-xs bg-amber-100 text-amber-700 hover:bg-amber-200 px-3 py-1.5 rounded font-medium flex items-center gap-1"
                      >
                        <Icon name="externalLink" size={12} /> Browse LWB Transcripts
                      </a>
                    </div>
                  </div>
                  {/* Table search */}
                  <div className="relative max-w-md">
                    <Icon name="search" size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
                    <input
                      type="text"
                      placeholder="Search table by title, venue, date..."
                      value={tableFilters.search || ''}
                      onChange={(e) => setTableFilters(prev => ({ ...prev, search: e.target.value }))}
                      className="w-full pl-9 pr-8 py-2 text-sm border rounded-lg focus:outline-none focus:ring-2 focus:ring-amber-500 focus:border-transparent"
                      data-search-input
                    />
                    {tableFilters.search && (
                      <button 
                        onClick={() => setTableFilters(prev => ({ ...prev, search: '' }))}
                        className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
                      >
                        <Icon name="x" size={14} />
                      </button>
                    )}
                  </div>
                </div>
                <VirtualizedTable
                  columns={[
                    { key: 'date', label: 'Date', width: '100px', filterKey: 'date', filterPlaceholder: 'YYYY', render: (r) => r.date },
                    { key: 'title', label: 'Title', width: '1fr', filterKey: 'title', render: (r) => (
                      <div className="flex items-center gap-1">
                        <span className="text-sm font-medium text-gray-900 truncate block" title={r.title}>{r.title}</span>
                      </div>
                    )},
                    { key: 'venue', label: 'Venue', width: '200px', filterKey: 'venue', hideOnMobile: true, render: (r) => (
                      <span className="text-xs text-gray-600 truncate">{r.venue || '—'}</span>
                    )},
                    { key: 'location', label: 'Location', width: '140px', hideOnMobile: true, render: (r) => (
                      <span className="text-xs text-gray-500">{r.location || '—'}</span>
                    )},
                    ...(activeTerm ? [{ key: 'mentionCount', label: 'Mentions', width: '80px', render: (r) => (
                      <span className={`text-xs font-bold ${r.mentionCount > 0 ? 'text-amber-600' : 'text-gray-400'}`}>{r.mentionCount || 0}</span>
                    )}] : []),
                    { key: 'readTime', label: 'Read Time', width: '75px', hideOnMobile: true, render: (r) => {
                      const mins = Math.round((r.wordCount || 0) / WORDS_PER_MINUTE)
                      return <span className="text-xs text-gray-500">{mins > 60 ? `${Math.round(mins/60)}h ${mins%60}m` : `${mins}m`}</span>
                    }},
                    { key: 'wordCount', label: 'Words', width: '90px', hideOnMobile: true, render: (r) => (
                      <span className="text-xs text-gray-500">{r.wordCount?.toLocaleString() || '—'}</span>
                    )},
                    { key: 'download', label: 'Download', width: '90px', render: (r) => r.path ? ((
                      () => {
                        const baseUrl = import.meta.env.BASE_URL || '/'
                        return (
                          <a 
                            href={`${baseUrl}${encodeFilePath(r.path)}`} 
                            download 
                            onClick={(e) => e.stopPropagation()}
                            className="text-xs text-blue-600 hover:text-blue-800 font-medium flex items-center gap-1"
                          >
                            <Icon name="download" size={12} /> TXT
                          </a>
                        )
                      })()
                    ) : '—' }
                  ]}
                  data={processedTableData}
                  filters={tableFilters}
                  onFilterChange={(key, val) => setTableFilters(prev => ({ ...prev, [key]: val }))}
                  sortConfig={sortConfig}
                  onSort={(key) => setSortConfig(prev => ({ key, direction: prev.key === key && prev.direction === 'asc' ? 'desc' : 'asc' }))}
                  onRowClick={(row) => { setSelectedSermon({ ...row, searchTerm: activeRegex || activeTerm }); setSelectedSermonFocus(0) }}
                  rowHeight={48}
                />
              </div>

              {/* Archive Statistics */}
              <div className="bg-white rounded-xl border shadow-sm p-6">
                <h2 className="text-xl font-bold text-gray-800 mb-4">Archive Statistics</h2>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
                  <div className="bg-amber-50 p-4 rounded-lg">
                    <div className="text-3xl font-bold text-amber-700">{totalSermons.toLocaleString()}</div>
                    <div className="text-sm text-amber-600">Total Sermons</div>
                  </div>
                  <div className="bg-blue-50 p-4 rounded-lg">
                    <div className="text-3xl font-bold text-blue-700">{(totalWords / 1000000).toFixed(1)}M</div>
                    <div className="text-sm text-blue-600">Total Words</div>
                  </div>
                  <div className="bg-purple-50 p-4 rounded-lg">
                    <div className="text-3xl font-bold text-purple-700">{Math.round(totalDuration / 60).toLocaleString()}</div>
                    <div className="text-sm text-purple-600">Hours of Preaching</div>
                  </div>
                  <div className="bg-green-50 p-4 rounded-lg">
                    <div className="text-3xl font-bold text-green-700">{venues.length}</div>
                    <div className="text-sm text-green-600">Unique Venues</div>
                  </div>
                </div>
                
                {/* Scripture References */}
                <div className="mb-6 p-4 bg-indigo-50 rounded-lg border border-indigo-200">
                  <div className="flex items-center justify-between mb-3">
                    <h3 className="text-lg font-bold text-indigo-800 flex items-center gap-2">
                      <Icon name="book" size={18} /> Scripture References
                    </h3>
                    <div className="flex items-center gap-2">
                      {scriptureCounts && (
                        <div className="flex bg-white rounded-lg border overflow-hidden">
                          <button
                            onClick={() => setScriptureView('grid')}
                            className={`px-3 py-1.5 text-sm font-medium transition-colors ${scriptureView === 'grid' ? 'bg-indigo-600 text-white' : 'text-gray-600 hover:bg-gray-100'}`}
                          >
                            Grid
                          </button>
                          <button
                            onClick={() => setScriptureView('heatmap')}
                            className={`px-3 py-1.5 text-sm font-medium transition-colors ${scriptureView === 'heatmap' ? 'bg-indigo-600 text-white' : 'text-gray-600 hover:bg-gray-100'}`}
                          >
                            Heatmap
                          </button>
                        </div>
                      )}
                      {!scriptureCounts && (
                        <button
                          onClick={scanForScriptures}
                          disabled={scriptureScanning}
                          className="px-4 py-2 bg-indigo-600 text-white rounded-lg text-sm font-medium hover:bg-indigo-700 disabled:opacity-50 flex items-center gap-2"
                        >
                          {scriptureScanning ? (
                            <><div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin"></div> Scanning...</>
                          ) : (
                            <><Icon name="search" size={14} /> Scan Transcripts</>
                          )}
                        </button>
                      )}
                    </div>
                  </div>
                  
                  {!scriptureCounts && !scriptureScanning && (
                    <p className="text-sm text-indigo-600">Click "Scan Transcripts" to find all Bible references mentioned across the archive. This may take a few minutes.</p>
                  )}
                  
                  {scriptureCounts && scriptureView === 'grid' && (
                    <div>
                      <p className="text-sm text-indigo-600 mb-3">
                        Found references to {scriptureCounts.length} books of the Bible
                      </p>
                      <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-2 max-h-64 overflow-y-auto">
                        {scriptureCounts.slice(0, 24).map(({ book, count, refs }) => {
                          const isExpanded = expandedTopics.has(`scripture-${book}`)
                          return (
                            <div key={book} className="bg-white rounded border overflow-hidden">
                              <button
                                onClick={() => setExpandedTopics(prev => {
                                  const next = new Set(prev)
                                  const key = `scripture-${book}`
                                  if (next.has(key)) next.delete(key)
                                  else next.add(key)
                                  return next
                                })}
                                className={`w-full p-2 flex justify-between items-center text-sm ${isExpanded ? 'bg-indigo-100' : 'hover:bg-gray-50'}`}
                              >
                                <span className="font-medium text-gray-800 truncate">{book}</span>
                                <span className="text-xs text-indigo-600 ml-1">{count.toLocaleString()}</span>
                              </button>
                              {isExpanded && (
                                <div className="border-t p-2 text-xs space-y-1 max-h-40 overflow-y-auto bg-gray-50">
                                  {refs.map(({ ref, count: refCount }) => (
                                    <button
                                      key={ref}
                                      onClick={() => { setSearchFieldTerm(ref); handleAnalysis(ref, '', null, { wholeWords: false }) }}
                                      className="w-full text-left px-2 py-1 hover:bg-indigo-100 rounded flex justify-between"
                                    >
                                      <span className="truncate">{ref}</span>
                                      <span className="text-indigo-500 ml-1">({refCount})</span>
                                    </button>
                                  ))}
                                </div>
                              )}
                            </div>
                          )
                        })}
                      </div>
                    </div>
                  )}
                  
                  {scriptureCounts && scriptureView === 'heatmap' && (() => {
                    const OT_BOOKS = ['Genesis', 'Exodus', 'Leviticus', 'Numbers', 'Deuteronomy', 'Joshua', 'Judges', 'Ruth', '1 Samuel', '2 Samuel', '1 Kings', '2 Kings', '1 Chronicles', '2 Chronicles', 'Ezra', 'Nehemiah', 'Esther', 'Job', 'Psalms', 'Proverbs', 'Ecclesiastes', 'Song of Solomon', 'Isaiah', 'Jeremiah', 'Lamentations', 'Ezekiel', 'Daniel', 'Hosea', 'Joel', 'Amos', 'Obadiah', 'Jonah', 'Micah', 'Nahum', 'Habakkuk', 'Zephaniah', 'Haggai', 'Zechariah', 'Malachi']
                    const NT_BOOKS = ['Matthew', 'Mark', 'Luke', 'John', 'Acts', 'Romans', '1 Corinthians', '2 Corinthians', 'Galatians', 'Ephesians', 'Philippians', 'Colossians', '1 Thessalonians', '2 Thessalonians', '1 Timothy', '2 Timothy', 'Titus', 'Philemon', 'Hebrews', 'James', '1 Peter', '2 Peter', '1 John', '2 John', '3 John', 'Jude', 'Revelation']
                    
                    const maxCount = Math.max(...scriptureCounts.map(s => s.count))
                    const bookMap = new Map(scriptureCounts.map(s => [s.book, s]))
                    
                    const renderBookRow = (books, label) => {
                      const booksWithData = books.filter(b => bookMap.has(b))
                      if (booksWithData.length === 0) return null
                      
                      return (
                        <div key={label} className="mb-4">
                          <h4 className="text-xs font-bold text-indigo-700 mb-2">{label}</h4>
                          <div className="flex flex-wrap gap-1">
                            {books.map(book => {
                              const data = bookMap.get(book)
                              const count = data?.count || 0
                              const intensity = count > 0 ? Math.min(0.15 + (count / maxCount) * 0.85, 1) : 0
                              return (
                                <button
                                  key={book}
                                  onClick={() => {
                                    if (data) {
                                      setExpandedTopics(prev => {
                                        const next = new Set(prev)
                                        const key = `scripture-${book}`
                                        if (next.has(key)) next.delete(key)
                                        else next.add(key)
                                        return next
                                      })
                                      setScriptureView('grid')
                                    }
                                  }}
                                  className={`px-2 py-1 rounded text-xs font-medium transition-colors ${count > 0 ? 'cursor-pointer hover:ring-1 hover:ring-indigo-400' : 'cursor-default opacity-30'}`}
                                  style={{ 
                                    backgroundColor: count > 0 ? `rgba(79, 70, 229, ${intensity})` : '#e5e7eb',
                                    color: intensity > 0.5 ? 'white' : (count > 0 ? '#3730a3' : '#9ca3af')
                                  }}
                                  title={count > 0 ? `${book}: ${count.toLocaleString()} references` : `${book}: No references found`}
                                >
                                  {book.length > 10 ? book.slice(0, 8) + '...' : book}
                                </button>
                              )
                            })}
                          </div>
                        </div>
                      )
                    }
                    
                    return (
                      <div>
                        <p className="text-sm text-indigo-600 mb-3">
                          Found references to {scriptureCounts.length} books of the Bible. Click a book to see specific verses.
                        </p>
                        {renderBookRow(OT_BOOKS, 'Old Testament')}
                        {renderBookRow(NT_BOOKS, 'New Testament')}
                        
                        <div className="flex items-center justify-end gap-2 mt-3 text-xs text-gray-500">
                          <span>Fewer</span>
                          <div className="flex">
                            {[0.15, 0.35, 0.55, 0.75, 0.95].map((intensity, i) => (
                              <div 
                                key={i} 
                                className="w-5 h-4" 
                                style={{ backgroundColor: `rgba(79, 70, 229, ${intensity})` }}
                              />
                            ))}
                          </div>
                          <span>More references</span>
                        </div>
                      </div>
                    )
                  })()}
                </div>
                
                {/* Featured Series */}
                <h3 className="text-lg font-bold text-gray-800 mb-3">Featured Sermon Series</h3>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mb-6">
                  {Object.entries(featuredSeries).map(([seriesName, series]) => {
                    const sermons = getSeriesSermons(seriesName)
                    const isExpanded = expandedTopics.has(seriesName)
                    const colorClasses = {
                      amber: { bg: 'bg-amber-50', border: 'border-amber-200', header: 'bg-amber-100', text: 'text-amber-800', badge: 'bg-amber-600' },
                      indigo: { bg: 'bg-indigo-50', border: 'border-indigo-200', header: 'bg-indigo-100', text: 'text-indigo-800', badge: 'bg-indigo-600' }
                    }[series.color] || { bg: 'bg-gray-50', border: 'border-gray-200', header: 'bg-gray-100', text: 'text-gray-800', badge: 'bg-gray-600' }
                    
                    return (
                      <div key={seriesName} className={`rounded-lg border ${colorClasses.border} overflow-hidden`}>
                        <button
                          onClick={() => setExpandedTopics(prev => {
                            const next = new Set(prev)
                            if (next.has(seriesName)) next.delete(seriesName)
                            else next.add(seriesName)
                            return next
                          })}
                          className={`w-full p-3 ${colorClasses.header} flex items-center justify-between transition hover:opacity-90`}
                        >
                          <div className="flex items-center gap-2">
                            <Icon name="book" size={18} className={colorClasses.text} />
                            <span className={`font-bold ${colorClasses.text}`}>{seriesName}</span>
                            <span className={`text-xs px-2 py-0.5 rounded-full text-white ${colorClasses.badge}`}>
                              {sermons.length} sermons
                            </span>
                          </div>
                          <Icon name={isExpanded ? 'chevronUp' : 'chevronDown'} size={18} className={colorClasses.text} />
                        </button>
                        {isExpanded && (
                          <div className={`${colorClasses.bg} p-3`}>
                            <div className="flex justify-between items-center mb-2">
                              <p className="text-xs text-gray-500">{series.description}</p>
                              <div className="flex gap-1 flex-shrink-0 ml-2">
                                <button
                                  onClick={(e) => { e.stopPropagation(); downloadTranscriptsZip(sermons, `WMB_${seriesName.replace(/[^a-zA-Z0-9]+/g, '_')}.zip`, seriesName) }}
                                  disabled={downloadProgress !== null}
                                  className="px-2 py-0.5 text-xs bg-amber-500 text-white rounded hover:bg-amber-600 transition disabled:opacity-50 flex items-center gap-1"
                                  title={`Download all ${sermons.length} sermons as ZIP`}
                                >
                                  <Icon name="download" size={12} /> ZIP ({sermons.length})
                                </button>
                              </div>
                            </div>
                            <div className="space-y-1">
                              {sermons.map(sermon => (
                                <button
                                  key={sermon.id}
                                  onClick={() => { setSelectedSermon(sermon); setSelectedSermonFocus(0) }}
                                  className="w-full text-left p-2 bg-white rounded border hover:bg-gray-50 transition flex justify-between items-center group"
                                >
                                  <div className="flex-1 min-w-0">
                                    <div className="font-medium text-gray-800 text-sm truncate">{sermon.title}</div>
                                    <div className="text-xs text-gray-500">{sermon.date}</div>
                                  </div>
                                  <Icon name="chevronRight" size={14} className="text-gray-400 group-hover:text-gray-600 flex-shrink-0 ml-2" />
                                </button>
                              ))}
                            </div>
                          </div>
                        )}
                      </div>
                    )
                  })}
                </div>
                
                <h3 className="text-lg font-bold text-gray-800 mb-3">Top Topics</h3>
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2">
                  {Object.entries(topicCounts).slice(0, 12).map(([topic, count]) => {
                    const isExpanded = expandedTopics.has(`topic-${topic}`)
                    const searchKey = `topic-${topic}`
                    const searchTerm = topicSearches[searchKey] || ''
                    const allTopicSermons = rawData.filter(s => s.topics && s.topics.includes(topic)).sort((a, b) => (a.date || '').localeCompare(b.date || ''))
                    const topicSermons = isExpanded ? (searchTerm 
                      ? allTopicSermons.filter(s => s.title.toLowerCase().includes(searchTerm.toLowerCase()) || (s.date || '').includes(searchTerm))
                      : allTopicSermons
                    ) : []
                    
                    return (
                      <div key={topic} className="border rounded-lg overflow-hidden">
                        <button
                          onClick={() => setExpandedTopics(prev => {
                            const next = new Set(prev)
                            const key = `topic-${topic}`
                            if (next.has(key)) next.delete(key)
                            else next.add(key)
                            return next
                          })}
                          className={`w-full p-3 flex justify-between items-center transition ${isExpanded ? 'bg-gray-100' : 'bg-gray-50 hover:bg-gray-100'}`}
                        >
                          <span className="font-medium text-gray-700">{topic}</span>
                          <div className="flex items-center gap-2">
                            <span className="text-sm text-gray-500">{count} sermons</span>
                            <Icon name={isExpanded ? 'chevronUp' : 'chevronDown'} size={14} className="text-gray-400" />
                          </div>
                        </button>
                        {isExpanded && (
                          <div className="bg-white border-t max-h-80 overflow-y-auto">
                            <div className="sticky top-0 bg-gray-50 p-2 border-b space-y-2">
                              <div className="flex justify-between items-center">
                                <span className="text-xs text-gray-500">{topicSermons.length} sermons{searchTerm && ` (filtered)`}</span>
                                <div className="flex gap-1">
                                  <button
                                    onClick={(e) => { e.stopPropagation(); downloadTranscriptsZip(topicSermons, `WMB_${topic.replace(/[^a-zA-Z0-9]+/g, '_')}_Sermons.zip`, topic) }}
                                    disabled={downloadProgress !== null}
                                    className="px-2 py-0.5 text-xs bg-amber-500 text-white rounded hover:bg-amber-600 transition disabled:opacity-50 flex items-center gap-1"
                                    title={`Download all ${topicSermons.length} sermons about ${topic} as ZIP`}
                                  >
                                    <Icon name="download" size={12} /> ZIP ({topicSermons.length})
                                  </button>
                                  <button
                                    onClick={(e) => {
                                      e.stopPropagation()
                                      const content = topicSermons.map(s => `${s.date} - ${s.title}`).join('\n')
                                      const blob = new Blob([content], { type: 'text/plain' })
                                      const url = URL.createObjectURL(blob)
                                      const a = document.createElement('a')
                                      a.href = url; a.download = `WMB_${topic.replace(/[^a-zA-Z0-9]+/g, '_')}_Index.txt`; a.click()
                                      URL.revokeObjectURL(url)
                                    }}
                                    className="px-2 py-0.5 text-xs bg-gray-400 text-white rounded hover:bg-gray-500 transition"
                                    title={`Download index of ${topicSermons.length} sermons`}
                                  >
                                    Index
                                  </button>
                                </div>
                              </div>
                              <input
                                type="text"
                                placeholder="Search sermons..."
                                value={searchTerm}
                                onClick={(e) => e.stopPropagation()}
                                onChange={(e) => setTopicSearches(prev => ({ ...prev, [searchKey]: e.target.value }))}
                                className="w-full text-xs border rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-amber-500"
                              />
                            </div>
                            <div className="p-2">
                              {topicSermons.map(sermon => (
                                <div key={sermon.id} className="flex items-center gap-1 hover:bg-gray-50 rounded transition">
                                  <button
                                    onClick={() => { setSelectedSermon(sermon); setSelectedSermonFocus(0) }}
                                    className="flex-1 text-left p-2 flex justify-between items-center text-sm min-w-0"
                                  >
                                    <span className="truncate text-gray-800">{sermon.title}</span>
                                    <span className="text-xs text-gray-500 ml-2 flex-shrink-0">{sermon.date}</span>
                                  </button>
                                  <a
                                    href={sermon.lwbUrl || `https://www.livingwordbroadcast.org/wbtextindex`}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    title="Download from Living Word Broadcast"
                                    className="p-2 text-gray-400 hover:text-amber-600 flex-shrink-0"
                                  >
                                    <Icon name="download" size={14} />
                                  </a>
                                </div>
                              ))}
                              {topicSermons.length === 0 && searchTerm && (
                                <p className="text-xs text-gray-400 text-center py-4">No sermons match "{searchTerm}"</p>
                              )}
                            </div>
                          </div>
                        )}
                      </div>
                    )
                  })}
                </div>
              </div>

              {/* Browse by Venue */}
              <div className="bg-white rounded-xl border shadow-sm p-6">
                <h2 className="text-xl font-bold text-gray-800 mb-4">
                  <Icon name="location" className="inline mr-2" size={20} />
                  Browse by Venue ({venues.length})
                </h2>
                <p className="text-sm text-gray-500 mb-4">Click a venue to see all sermons preached there</p>
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2 max-h-96 overflow-y-auto">
                  {venueBarData.slice(0, 30).map(venue => {
                    const isExpanded = expandedTopics.has(`venue-${venue.venue}`)
                    const searchKey = `venue-${venue.venue}`
                    const searchTerm = topicSearches[searchKey] || ''
                    const allVenueSermons = rawData.filter(s => s.venue === venue.venue).sort((a, b) => (a.date || '').localeCompare(b.date || ''))
                    const venueSermons = isExpanded ? (searchTerm 
                      ? allVenueSermons.filter(s => s.title.toLowerCase().includes(searchTerm.toLowerCase()) || (s.date || '').includes(searchTerm))
                      : allVenueSermons
                    ) : []
                    
                    return (
                      <div key={venue.venue} className="border rounded-lg overflow-hidden">
                        <button
                          onClick={() => setExpandedTopics(prev => {
                            const next = new Set(prev)
                            const key = `venue-${venue.venue}`
                            if (next.has(key)) next.delete(key)
                            else next.add(key)
                            return next
                          })}
                          className={`w-full p-3 flex justify-between items-center transition ${isExpanded ? 'bg-blue-50' : 'bg-gray-50 hover:bg-gray-100'}`}
                        >
                          <div className="text-left min-w-0 flex-1">
                            <span className="font-medium text-gray-700 truncate block">{venue.venue}</span>
                            {venue.dateRange && venue.dateRange !== 'N/A' && (
                              <span className="text-xs text-gray-400">{venue.dateRange}</span>
                            )}
                          </div>
                          <div className="flex items-center gap-2 flex-shrink-0 ml-2">
                            <span className="text-sm text-gray-500">{venue.count}</span>
                            <Icon name={isExpanded ? 'chevronUp' : 'chevronDown'} size={14} className="text-gray-400" />
                          </div>
                        </button>
                        {isExpanded && (
                          <div className="bg-white border-t max-h-80 overflow-y-auto">
                            <div className="sticky top-0 bg-gray-50 p-2 border-b">
                              <div className="flex justify-between items-center mb-2">
                                <span className="text-xs text-gray-500">{venueSermons.length}{searchTerm ? ` of ${allVenueSermons.length}` : ''} sermons</span>
                                <div className="flex gap-1">
                                  <button
                                    onClick={(e) => { e.stopPropagation(); downloadTranscriptsZip(allVenueSermons, `WMB_${venue.venue.replace(/[^a-zA-Z0-9]+/g, '_')}_Sermons.zip`, venue.venue) }}
                                    disabled={downloadProgress !== null}
                                    className="px-2 py-0.5 text-xs bg-amber-500 text-white rounded hover:bg-amber-600 transition disabled:opacity-50 flex items-center gap-1"
                                    title={`Download all ${allVenueSermons.length} sermons from ${venue.venue} as ZIP`}
                                  >
                                    <Icon name="download" size={12} /> ZIP
                                  </button>
                                </div>
                              </div>
                              <input
                                type="text"
                                placeholder="Search sermons..."
                                value={searchTerm}
                                onChange={(e) => setTopicSearches(prev => ({ ...prev, [searchKey]: e.target.value }))}
                                onClick={(e) => e.stopPropagation()}
                                className="w-full px-2 py-1 text-xs border rounded focus:outline-none focus:ring-1 focus:ring-blue-500"
                              />
                            </div>
                            <div className="p-2">
                              {venueSermons.map(sermon => (
                                <button
                                  key={sermon.id}
                                  onClick={() => { setSelectedSermon(sermon); setSelectedSermonFocus(0) }}
                                  className="w-full text-left p-2 hover:bg-gray-50 rounded transition flex justify-between items-center text-sm"
                                >
                                  <span className="truncate text-gray-800 flex-1 min-w-0">{sermon.title}</span>
                                  <span className="text-xs text-gray-500 ml-2 flex-shrink-0">{sermon.date}</span>
                                </button>
                              ))}
                              {venueSermons.length === 0 && searchTerm && (
                                <p className="text-xs text-gray-400 text-center py-2">No sermons match "{searchTerm}"</p>
                              )}
                            </div>
                          </div>
                        )}
                      </div>
                    )
                  })}
                </div>
              </div>
            </>
          )}

          {/* Data Tab */}
          {view === 'data' && (
            <div className="space-y-6">
              {/* Transcripts by Year */}
              <div className="bg-white rounded-xl border shadow-sm p-6">
                <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-3 mb-4">
                  <div>
                    <h2 className="text-xl font-bold text-gray-800">
                      <Icon name="calendar" className="inline mr-2" size={20} />
                      All Transcripts by Year
                    </h2>
                    <p className="text-sm text-gray-500 mt-1">Click a year to expand and see all sermons. Click a sermon to view its full transcript.</p>
                  </div>
                  <div className="flex gap-2">
                    <button
                      onClick={() => downloadTranscriptsZip(rawData, 'WMB_All_Sermons.zip', 'all')}
                      disabled={downloadProgress !== null}
                      className="px-4 py-2 bg-amber-600 hover:bg-amber-700 disabled:bg-amber-400 text-white rounded-lg text-sm font-medium transition flex items-center gap-2 whitespace-nowrap"
                    >
                      <Icon name="download" size={16} /> 
                      {downloadProgress?.year === 'all' 
                        ? `Downloading ${downloadProgress.current}/${downloadProgress.total}...` 
                        : `Download All (${rawData.length})`}
                    </button>
                  </div>
                </div>
                
                {/* Download progress indicator */}
                {downloadProgress && (
                  <div className="mb-4 bg-amber-50 border border-amber-200 rounded-lg p-3">
                    <div className="flex items-center gap-3">
                      <div className="animate-spin w-5 h-5 border-2 border-amber-600 border-t-transparent rounded-full"></div>
                      <div className="flex-1">
                        <div className="text-sm font-medium text-amber-800">
                          Downloading {downloadProgress.year === 'all' ? 'all transcripts' : `${downloadProgress.year} transcripts`}...
                        </div>
                        <div className="text-xs text-amber-600">
                          {downloadProgress.current} of {downloadProgress.total} files
                        </div>
                      </div>
                      <div className="w-32 bg-amber-200 rounded-full h-2">
                        <div 
                          className="bg-amber-600 h-2 rounded-full transition-all" 
                          style={{ width: `${(downloadProgress.current / downloadProgress.total) * 100}%` }}
                        />
                      </div>
                    </div>
                  </div>
                )}

                {/* Global Searchable Sermon Database */}
                <div className="mb-6">
                  <button
                    onClick={() => setShowGlobalSermonTable(!showGlobalSermonTable)}
                    className="flex items-center gap-2 text-sm font-medium text-amber-600 hover:text-amber-800 mb-3"
                  >
                    <Icon name={showGlobalSermonTable ? 'chevronDown' : 'chevronRight'} size={16} />
                    <Icon name="search" size={14} />
                    {showGlobalSermonTable ? 'Hide' : 'Open'} Searchable Sermon Database ({rawData.length.toLocaleString()} sermons)
                  </button>
                  
                  {showGlobalSermonTable && (
                    <div className="border rounded-lg p-4 bg-amber-50">
                      <div className="flex flex-col sm:flex-row gap-3 mb-4">
                        <div className="relative flex-1">
                          <Icon name="search" size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
                          <input
                            type="text"
                            placeholder="Filter by title, venue, location, date..."
                            value={globalSermonSearch}
                            onChange={(e) => setGlobalSermonSearch(e.target.value)}
                            className="w-full pl-9 pr-8 py-2 text-sm border rounded-lg focus:outline-none focus:ring-2 focus:ring-amber-500"
                          />
                          {globalSermonSearch && (
                            <button
                              onClick={() => setGlobalSermonSearch('')}
                              className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
                            >
                              <Icon name="x" size={14} />
                            </button>
                          )}
                        </div>
                        <div className="text-xs text-gray-500">
                          Filters metadata only. Use the search engine on the Search & Stats tab to search sermon content.
                        </div>
                      </div>

                      {(() => {
                        const searchQ = globalSermonSearch.trim().toLowerCase()
                        let globalFilteredSermons = rawData.slice()
                        
                        // Filter by search query
                        if (searchQ) {
                          globalFilteredSermons = globalFilteredSermons.filter(s => 
                            (s.title || '').toLowerCase().includes(searchQ) ||
                            (s.venue || '').toLowerCase().includes(searchQ) ||
                            (s.location || '').toLowerCase().includes(searchQ) ||
                            (s.date || '').toLowerCase().includes(searchQ)
                          )
                        }
                        
                        // Sort
                        const gSortCfg = globalSermonSort
                        globalFilteredSermons.sort((a, b) => {
                          let aVal = a[gSortCfg.key] || ''
                          let bVal = b[gSortCfg.key] || ''
                          if (gSortCfg.key === 'date') {
                            if (!aVal) aVal = '0000-00-00'
                            if (!bVal) bVal = '0000-00-00'
                          }
                          if (gSortCfg.key === 'wordCount') {
                            aVal = a.wordCount || 0
                            bVal = b.wordCount || 0
                            const cmp = aVal - bVal
                            return gSortCfg.direction === 'desc' ? -cmp : cmp
                          }
                          const cmp = String(aVal).localeCompare(String(bVal))
                          return gSortCfg.direction === 'desc' ? -cmp : cmp
                        })
                        
                        // Limit to 100 results for performance
                        const displayLimit = 100
                        const hasMore = globalFilteredSermons.length > displayLimit
                        const displaySermons = globalFilteredSermons.slice(0, displayLimit)
                        
                        return (
                          <>
                            <div className="text-xs text-gray-500 mb-2">
                              {searchQ ? (
                                <>Showing {displaySermons.length.toLocaleString()}{hasMore ? '+' : ''} of {globalFilteredSermons.length.toLocaleString()} matching sermons</>
                              ) : (
                                <>Showing {displaySermons.length.toLocaleString()} of {rawData.length.toLocaleString()} sermons (use search to filter)</>
                              )}
                            </div>
                            <div className="max-h-96 overflow-y-auto border rounded bg-white overflow-x-auto">
                              <table className="w-full text-sm min-w-[800px]">
                                <thead className="sticky top-0 bg-gray-50 border-b z-10">
                                  <tr className="text-gray-600 text-left">
                                    <th 
                                      className="p-2 font-medium w-24 cursor-pointer hover:bg-gray-100 select-none"
                                      onClick={() => setGlobalSermonSort(prev => ({ 
                                        key: 'date', 
                                        direction: prev.key === 'date' && prev.direction === 'desc' ? 'asc' : 'desc' 
                                      }))}
                                    >
                                      Date {gSortCfg.key === 'date' && <span className="ml-1">{gSortCfg.direction === 'desc' ? '↓' : '↑'}</span>}
                                    </th>
                                    <th 
                                      className="p-2 font-medium cursor-pointer hover:bg-gray-100 select-none"
                                      onClick={() => setGlobalSermonSort(prev => ({ 
                                        key: 'title', 
                                        direction: prev.key === 'title' && prev.direction === 'asc' ? 'desc' : 'asc' 
                                      }))}
                                    >
                                      Title {gSortCfg.key === 'title' && <span className="ml-1">{gSortCfg.direction === 'asc' ? '↑' : '↓'}</span>}
                                    </th>
                                    <th 
                                      className="p-2 font-medium hidden sm:table-cell cursor-pointer hover:bg-gray-100 select-none"
                                      onClick={() => setGlobalSermonSort(prev => ({ 
                                        key: 'venue', 
                                        direction: prev.key === 'venue' && prev.direction === 'asc' ? 'desc' : 'asc' 
                                      }))}
                                    >
                                      Venue {gSortCfg.key === 'venue' && <span className="ml-1">{gSortCfg.direction === 'asc' ? '↑' : '↓'}</span>}
                                    </th>
                                    <th 
                                      className="p-2 font-medium hidden md:table-cell cursor-pointer hover:bg-gray-100 select-none"
                                      onClick={() => setGlobalSermonSort(prev => ({ 
                                        key: 'location', 
                                        direction: prev.key === 'location' && prev.direction === 'asc' ? 'desc' : 'asc' 
                                      }))}
                                    >
                                      Location {gSortCfg.key === 'location' && <span className="ml-1">{gSortCfg.direction === 'asc' ? '↑' : '↓'}</span>}
                                    </th>
                                    <th 
                                      className="p-2 font-medium text-right hidden md:table-cell cursor-pointer hover:bg-gray-100 select-none"
                                      onClick={() => setGlobalSermonSort(prev => ({ 
                                        key: 'wordCount', 
                                        direction: prev.key === 'wordCount' && prev.direction === 'desc' ? 'asc' : 'desc' 
                                      }))}
                                    >
                                      Words {gSortCfg.key === 'wordCount' && <span className="ml-1">{gSortCfg.direction === 'desc' ? '↓' : '↑'}</span>}
                                    </th>
                                    <th className="p-2 font-medium text-center">Source</th>
                                    <th className="p-2 font-medium text-center">Download</th>
                                  </tr>
                                </thead>
                                <tbody className="divide-y divide-gray-100">
                                  {displaySermons.length === 0 ? (
                                    <tr>
                                      <td colSpan="7" className="py-8 text-center text-gray-500">
                                        No sermons match your search.
                                      </td>
                                    </tr>
                                  ) : displaySermons.map((sermon, si) => (
                                    <tr key={si} className="hover:bg-amber-50 cursor-pointer" onClick={() => setSelectedSermon(sermon)}>
                                      <td className="p-2 text-gray-600 whitespace-nowrap">{sermon.date || 'Unknown'}</td>
                                      <td className="p-2 font-medium text-gray-800">{sermon.title || 'Untitled'}</td>
                                      <td className="p-2 text-gray-600 hidden sm:table-cell">{sermon.venue || '—'}</td>
                                      <td className="p-2 text-gray-600 hidden md:table-cell">{sermon.location || '—'}</td>
                                      <td className="p-2 text-right text-gray-500 hidden md:table-cell">{sermon.wordCount?.toLocaleString() || '—'}</td>
                                      <td className="p-2 text-center">
                                        <a 
                                          href="https://www.livingwordbroadcast.org/wbtextindex" 
                                          target="_blank" 
                                          rel="noopener noreferrer"
                                          onClick={(e) => e.stopPropagation()}
                                          className="text-xs text-amber-600 hover:text-amber-800 font-medium"
                                        >
                                          LWB ↗
                                        </a>
                                      </td>
                                      <td className="p-2 text-center">
                                        {sermon.path && (
                                          <a
                                            href={`${import.meta.env.BASE_URL || '/'}${encodeFilePath(sermon.path)}`}
                                            download
                                            onClick={(e) => e.stopPropagation()}
                                            className="text-xs text-blue-600 hover:text-blue-800 font-medium inline-flex items-center gap-1"
                                            title="Download transcript"
                                          >
                                            <Icon name="download" size={12} /> TXT
                                          </a>
                                        )}
                                      </td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                            {hasMore && (
                              <div className="text-xs text-gray-500 mt-2 text-center">
                                Showing first {displayLimit} results. Use search to narrow down.
                              </div>
                            )}
                          </>
                        )
                      })()}
                    </div>
                  )}
                </div>
                
                <div className="space-y-2">
                  {sermonsByYear.map(({ year, sermons, count, totalWords: yearWords }) => {
                    const isExpanded = expandedYears.has(year)
                    const searchTerm = yearSearch[year] || ''
                    const sortCfg = yearSort[year] || { key: 'date', direction: 'asc' }
                    
                    // Filter and sort sermons
                    const filteredSermons = sermons.filter(s => {
                      if (!searchTerm) return true
                      const term = searchTerm.toLowerCase()
                      return (s.title || '').toLowerCase().includes(term) ||
                             (s.venue || '').toLowerCase().includes(term) ||
                             (s.date || '').toLowerCase().includes(term)
                    }).sort((a, b) => {
                      const dir = sortCfg.direction === 'asc' ? 1 : -1
                      if (sortCfg.key === 'date') return dir * ((a.date || '').localeCompare(b.date || ''))
                      if (sortCfg.key === 'title') return dir * ((a.title || '').localeCompare(b.title || ''))
                      if (sortCfg.key === 'venue') return dir * ((a.venue || '').localeCompare(b.venue || ''))
                      return 0
                    })
                    
                    return (
                      <div key={year} className="border rounded-lg overflow-hidden">
                        {/* Year header */}
                        <div className={`flex items-center justify-between p-4 transition-colors ${isExpanded ? 'bg-amber-100' : 'bg-amber-50 hover:bg-amber-100'}`}>
                          <button
                            onClick={() => {
                              setExpandedYears(prev => {
                                const next = new Set(prev)
                                if (next.has(year)) next.delete(year)
                                else next.add(year)
                                return next
                              })
                            }}
                            className="flex items-center gap-3 flex-1 text-left"
                          >
                            <Icon name={isExpanded ? 'chevronDown' : 'chevronRight'} size={18} className="text-amber-600" />
                            <span className="text-lg font-bold text-amber-800">{year}</span>
                            <span className="text-sm text-amber-600">({count} sermons)</span>
                            <span className="text-sm text-amber-700 ml-auto mr-4">
                              {(yearWords / 1000).toFixed(0)}K words
                            </span>
                          </button>
                          <button
                            onClick={(e) => {
                              e.stopPropagation()
                              // Download all sermons for this year as individual files
                              const yearSermons = sermons.filter(s => s.path)
                              if (yearSermons.length === 0) {
                                alert('No downloadable transcripts for this year')
                                return
                              }
                              // Create a simple text file with links to all transcripts for this year
                              const baseUrl = window.location.origin + (import.meta.env.BASE_URL || '/')
                              const content = `William Branham Sermons - ${year}\n${'='.repeat(40)}\n\n${yearSermons.length} sermons\n\nDownload each transcript from Living Word Broadcast:\nhttps://www.livingwordbroadcast.org/wbtextindex\n\n--- Sermon List ---\n\n${yearSermons.map(s => `${s.date} - ${s.title}\n  Venue: ${s.venue || 'Unknown'}\n  Words: ${s.wordCount?.toLocaleString() || 'N/A'}`).join('\n\n')}`
                              const blob = new Blob([content], { type: 'text/plain' })
                              const url = URL.createObjectURL(blob)
                              const a = document.createElement('a')
                              a.href = url
                              a.download = `WMB_Sermons_${year}_Index.txt`
                              document.body.appendChild(a)
                              a.click()
                              document.body.removeChild(a)
                              URL.revokeObjectURL(url)
                            }}
                            className="px-2 py-1.5 bg-gray-100 hover:bg-gray-200 text-gray-700 rounded text-xs font-medium transition flex items-center gap-1"
                            title={`Download index for ${year}`}
                          >
                            <Icon name="fileText" size={12} /> Index
                          </button>
                          <button
                            onClick={(e) => {
                              e.stopPropagation()
                              downloadTranscriptsZip(sermons, `WMB_Sermons_${year}.zip`, year)
                            }}
                            disabled={downloadProgress !== null}
                            className="px-2 py-1.5 bg-amber-600 hover:bg-amber-700 disabled:bg-amber-400 text-white rounded text-xs font-medium transition flex items-center gap-1"
                            title={`Download all ${count} transcripts for ${year} as ZIP`}
                          >
                            <Icon name="download" size={12} /> 
                            {downloadProgress?.year === year 
                              ? `${downloadProgress.current}/${downloadProgress.total}` 
                              : `ZIP (${count})`}
                          </button>
                        </div>
                        
                        {/* Expanded sermon list */}
                        {isExpanded && (
                          <div className="bg-white border-t p-4">
                            {/* Search and sort controls */}
                            <div className="flex flex-col sm:flex-row gap-2 mb-3">
                              <div className="relative flex-1">
                                <Icon name="search" size={14} className="absolute left-2 top-1/2 -translate-y-1/2 text-gray-400" />
                                <input
                                  type="text"
                                  placeholder="Search sermons by title, venue, or date..."
                                  value={searchTerm}
                                  onChange={(e) => setYearSearch(prev => ({ ...prev, [year]: e.target.value }))}
                                  className="w-full pl-7 pr-3 py-1.5 text-sm border rounded focus:outline-none focus:ring-1 focus:ring-amber-500"
                                />
                                {searchTerm && (
                                  <button 
                                    onClick={() => setYearSearch(prev => ({ ...prev, [year]: '' }))}
                                    className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
                                  >
                                    <Icon name="x" size={12} />
                                  </button>
                                )}
                              </div>
                              <div className="flex gap-1 text-xs">
                                <span className="text-gray-500 self-center mr-1">Sort:</span>
                                {['date', 'title', 'venue'].map(key => (
                                  <button
                                    key={key}
                                    onClick={() => setYearSort(prev => ({
                                      ...prev,
                                      [year]: {
                                        key,
                                        direction: prev[year]?.key === key && prev[year]?.direction === 'asc' ? 'desc' : 'asc'
                                      }
                                    }))}
                                    className={`px-2 py-1 rounded ${sortCfg.key === key ? 'bg-amber-100 text-amber-700' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}`}
                                  >
                                    {key.charAt(0).toUpperCase() + key.slice(1)}
                                    {sortCfg.key === key && (sortCfg.direction === 'asc' ? ' ↑' : ' ↓')}
                                  </button>
                                ))}
                              </div>
                            </div>
                            
                            {/* Sermon list */}
                            <div className="max-h-96 overflow-y-auto overflow-x-auto">
                              <table className="w-full text-sm min-w-[800px]">
                                <thead className="bg-gray-50 sticky top-0">
                                  <tr>
                                    <th className="text-left p-2 font-medium text-gray-600">Date</th>
                                    <th className="text-left p-2 font-medium text-gray-600">Title</th>
                                    <th className="text-left p-2 font-medium text-gray-600 hidden sm:table-cell">Venue</th>
                                    <th className="text-left p-2 font-medium text-gray-600 hidden md:table-cell">Location</th>
                                    <th className="text-right p-2 font-medium text-gray-600 hidden md:table-cell">Words</th>
                                    <th className="text-center p-2 font-medium text-gray-600">Source</th>
                                    <th className="text-center p-2 font-medium text-gray-600">Download</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {filteredSermons.map((sermon, idx) => (
                                    <tr 
                                      key={sermon.id || idx}
                                      onClick={() => { setSelectedSermon(sermon); setSelectedSermonFocus(0) }}
                                      className="border-t cursor-pointer hover:bg-amber-50 transition-colors"
                                    >
                                      <td className="p-2 text-gray-600 whitespace-nowrap">{sermon.date}</td>
                                      <td className="p-2 font-medium text-gray-800">{sermon.title}</td>
                                      <td className="p-2 text-gray-600 hidden sm:table-cell">{sermon.venue}</td>
                                      <td className="p-2 text-gray-600 hidden md:table-cell">{sermon.location || '—'}</td>
                                      <td className="p-2 text-gray-500 text-right hidden md:table-cell">{sermon.wordCount?.toLocaleString()}</td>
                                      <td className="p-2 text-center">
                                        <a 
                                          href="https://www.livingwordbroadcast.org/wbtextindex" 
                                          target="_blank" 
                                          rel="noopener noreferrer"
                                          onClick={(e) => e.stopPropagation()}
                                          className="text-xs text-amber-600 hover:text-amber-800 font-medium"
                                        >
                                          LWB ↗
                                        </a>
                                      </td>
                                      <td className="p-2 text-center">
                                        {sermon.path && (
                                          <a 
                                            href={`${import.meta.env.BASE_URL || '/'}${encodeFilePath(sermon.path)}`} 
                                            download 
                                            onClick={(e) => e.stopPropagation()}
                                            className="text-xs text-blue-600 hover:text-blue-800 font-medium inline-flex items-center gap-1"
                                          >
                                            <Icon name="download" size={12} /> TXT
                                          </a>
                                        )}
                                      </td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                              {filteredSermons.length === 0 && (
                                <div className="text-center py-4 text-gray-500">No sermons match your search.</div>
                              )}
                            </div>
                            <div className="text-xs text-gray-500 mt-2 pt-2 border-t">
                              Showing {filteredSermons.length} of {count} sermons
                            </div>
                          </div>
                        )}
                      </div>
                    )
                  })}
                </div>
              </div>
            </div>
          )}

          {/* About Tab */}
          {view === 'about' && (
            <div className="bg-white rounded-xl border shadow-sm p-6">
              <h2 className="text-xl font-bold text-gray-800 mb-4">About This Archive</h2>
              <div className="prose prose-sm max-w-none text-gray-600">
                <p>This searchable archive contains <strong>{totalSermons.toLocaleString()} sermon transcripts</strong> from William Branham's ministry spanning <strong>{yearRange}</strong>.</p>
                
                <h3 className="text-lg font-semibold text-gray-800 mt-6 mb-2">Features</h3>
                <ul className="list-disc list-inside space-y-1">
                  <li><strong>Full-text search</strong> with regex and boolean operators (AND, OR, NOT, NEAR)</li>
                  <li><strong>Topic filtering</strong> by detected sermon themes</li>
                  <li><strong>Timeline visualization</strong> of sermons and search results</li>
                  <li><strong>Venue breakdown</strong> showing where sermons were preached</li>
                  <li><strong>Direct links</strong> to Living Word Broadcast for each sermon</li>
                </ul>

                <h3 className="text-lg font-semibold text-gray-800 mt-6 mb-2">Data Source</h3>
                <p>All transcripts are sourced from <a href="https://www.livingwordbroadcast.org/wbtextindex" target="_blank" rel="noopener noreferrer" className="text-amber-600 hover:text-amber-800">Living Word Broadcast</a>.</p>

                <h3 className="text-lg font-semibold text-gray-800 mt-6 mb-2">Search Tips</h3>
                <ul className="list-disc list-inside space-y-1">
                  <li><code className="bg-gray-100 px-1 rounded">seven seals</code> - Find exact phrase</li>
                  <li><code className="bg-gray-100 px-1 rounded">bride AND rapture</code> - Both terms required</li>
                  <li><code className="bg-gray-100 px-1 rounded">prophet OR messenger</code> - Either term</li>
                  <li><code className="bg-gray-100 px-1 rounded">healing NOT cancer</code> - Exclude a term</li>
                  <li><code className="bg-gray-100 px-1 rounded">seal* </code> - Wildcard (seals, sealed, etc.)</li>
                </ul>
              </div>
            </div>
          )}
        </div>

        {/* Sermon Modal */}
        {selectedSermon && (
          <SermonModal
            sermon={selectedSermon}
            searchTerm={selectedSermon.searchTerm}
            wholeWords={lastAnalysisRef.current?.wholeWords !== false}
            onClose={() => setSelectedSermon(null)}
            focusIndex={selectedSermonFocus}
            onFocusChange={setSelectedSermonFocus}
            onSaveProgress={saveReadingProgress}
            resumePosition={selectedSermon.resumePosition}
            relatedSermons={getRelatedSermons(selectedSermon)}
            onSelectRelated={(s) => { setSelectedSermon({ ...s, searchTerm: activeRegex || activeTerm }); setSelectedSermonFocus(0) }}
          />
        )}

        {/* Venue Browse Popup */}
        {browseVenue && (() => {
          const venueSermons = rawData.filter(s => s.venue === browseVenue).sort((a, b) => (a.date || '').localeCompare(b.date || ''))
          const venueInfo = venueBarData.find(v => v.venue === browseVenue)
          return (
            <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4" onClick={() => setBrowseVenue(null)}>
              <div className="bg-white rounded-xl shadow-2xl max-w-2xl w-full max-h-[80vh] overflow-hidden" onClick={e => e.stopPropagation()}>
                <div className="bg-gradient-to-r from-amber-500 to-amber-600 text-white p-4 flex justify-between items-center">
                  <div>
                    <h3 className="font-bold text-lg">{browseVenue}</h3>
                    <p className="text-amber-100 text-sm">{venueSermons.length} sermons • {venueInfo?.dateRange || 'Unknown dates'}</p>
                  </div>
                  <button onClick={() => setBrowseVenue(null)} className="text-white/80 hover:text-white p-1">
                    <Icon name="x" size={24} />
                  </button>
                </div>
                <div className="p-4 overflow-y-auto max-h-[calc(80vh-80px)]">
                  <div className="space-y-1">
                    {venueSermons.map(sermon => (
                      <button
                        key={sermon.id}
                        onClick={() => { setSelectedSermon(sermon); setSelectedSermonFocus(0); setBrowseVenue(null) }}
                        className="w-full text-left p-3 hover:bg-amber-50 rounded-lg transition flex justify-between items-center border border-transparent hover:border-amber-200"
                      >
                        <span className="text-gray-800 flex-1 min-w-0 truncate">{sermon.title}</span>
                        <span className="text-sm text-gray-500 ml-3 flex-shrink-0">{sermon.date}</span>
                      </button>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          )
        })()}

        {/* Chart Modal */}
        {expandedChart && (
          <ChartModal
            {...expandedChart}
            onClose={() => setExpandedChart(null)}
            onSermonSelect={(s) => { setSelectedSermon({ ...s, searchTerm: activeRegex || activeTerm }); setSelectedSermonFocus(0) }}
          />
        )}
      </div>
    </ErrorBoundary>
  )
}
