import React, { useState, useEffect } from 'react'
import Icon from './Icon'
import { DEFAULT_REGEX_STR } from '../constants_local'

// Helper: Convert wildcard pattern to regex for highlighting
const wildcardToRegex = (pattern) => {
  let escaped = pattern.replace(/[.+^${}()|[\]\\]/g, '\\$&')
  escaped = escaped.replace(/\*/g, '\\S*').replace(/\?/g, '.')
  return escaped
}

// Helper: Check if term contains wildcards
const hasWildcard = (term) => /[*?]/.test(term)

// Helper: Parse proximity search to get type, terms, and distance
const parseProximitySearch = (input) => {
  if (!input || typeof input !== 'string') return null
  const trimmed = input.trim()
  
  // ONEAR/n pattern
  const onearMatch = trimmed.match(/^(.+?)\s+ONEAR\/(\d+)\s+(.+)$/i)
  if (onearMatch) return { type: 'onear', terms: [onearMatch[1].trim(), onearMatch[3].trim()], distance: parseInt(onearMatch[2], 10) }
  
  // NEAR/n pattern
  const nearMatch = trimmed.match(/^(.+?)\s+NEAR\/(\d+)\s+(.+)$/i)
  if (nearMatch) return { type: 'near', terms: [nearMatch[1].trim(), nearMatch[3].trim()], distance: parseInt(nearMatch[2], 10) }
  
  // ~n pattern (shorthand for NEAR)
  const tildeMatch = trimmed.match(/^(.+?)\s+~(\d+)\s+(.+)$/i)
  if (tildeMatch) return { type: 'near', terms: [tildeMatch[1].trim(), tildeMatch[3].trim()], distance: parseInt(tildeMatch[2], 10) }
  
  // AROUND(n) pattern
  const aroundMatch = trimmed.match(/^(.+?)\s+AROUND\((\d+)\)\s+(.+)$/i)
  if (aroundMatch) return { type: 'near', terms: [aroundMatch[1].trim(), aroundMatch[3].trim()], distance: parseInt(aroundMatch[2], 10) }
  
  // SENTENCE pattern
  const sentenceMatch = trimmed.match(/^(.+?)\s+(?:\/s|SENTENCE)\s+(.+)$/i)
  if (sentenceMatch) return { type: 'sentence', terms: [sentenceMatch[1].trim(), sentenceMatch[2].trim()] }
  
  // PARAGRAPH pattern
  const paragraphMatch = trimmed.match(/^(.+?)\s+(?:\/p|PARAGRAPH)\s+(.+)$/i)
  if (paragraphMatch) return { type: 'paragraph', terms: [paragraphMatch[1].trim(), paragraphMatch[2].trim()] }
  
  return null
}

// Helper: Find proximity matches with their actual positions in text
const findProximityMatches = (text, searchTerm) => {
  const proximity = parseProximitySearch(searchTerm)
  if (!proximity) return null
  
  const { type, terms, distance } = proximity
  const buildPattern = (t) => {
    if (hasWildcard(t)) return wildcardToRegex(t)
    return t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  }
  
  const pattern1 = buildPattern(terms[0])
  const pattern2 = buildPattern(terms[1])
  const regex1 = new RegExp(`\\b(${pattern1})\\b`, 'gi')
  const regex2 = new RegExp(`\\b(${pattern2})\\b`, 'gi')
  
  const matches = []
  
  if (type === 'near' || type === 'onear') {
    // Find all positions of both terms
    const pos1 = [], pos2 = []
    let m
    while ((m = regex1.exec(text)) !== null) pos1.push({ index: m.index, end: m.index + m[0].length, term: m[0] })
    while ((m = regex2.exec(text)) !== null) pos2.push({ index: m.index, end: m.index + m[0].length, term: m[0] })
    
    if (pos1.length === 0 || pos2.length === 0) return []
    
    // Count words between positions
    const wordPattern = /\S+/g
    const words = []
    while ((m = wordPattern.exec(text)) !== null) words.push({ start: m.index, end: m.index + m[0].length })
    
    const usedPairs = new Set()
    
    for (let i = 0; i < pos1.length; i++) {
      for (let j = 0; j < pos2.length; j++) {
        const p1 = pos1[i], p2 = pos2[j]
        // For ordered search (ONEAR), term1 must come before term2
        if (type === 'onear' && p1.index >= p2.index) continue
        
        const start = Math.min(p1.index, p2.index)
        const end = Math.max(p1.end, p2.end)
        const wordsBetween = words.filter(w => w.start > Math.min(p1.end, p2.end) && w.end < Math.max(p1.index, p2.index)).length
        
        if (wordsBetween <= distance) {
          // Create a unique key for this pair to avoid duplicates
          const pairKey = `${i}-${j}`
          if (!usedPairs.has(pairKey)) {
            usedPairs.add(pairKey)
            // Add ONE match per proximity pair, at the position of the first term
            matches.push({ 
              index: start, 
              term: `${p1.term}...${p2.term}`,
              // Store both terms for highlighting
              terms: [p1.term, p2.term]
            })
          }
        }
      }
    }
  } else if (type === 'sentence') {
    // Split by sentence boundaries
    const sentencePattern = /[^.!?]*[.!?]+/g
    let sentenceMatch
    while ((sentenceMatch = sentencePattern.exec(text)) !== null) {
      const sentence = sentenceMatch[0]
      const sentenceStart = sentenceMatch.index
      regex1.lastIndex = 0; regex2.lastIndex = 0
      
      const matches1 = [], matches2 = []
      let m
      while ((m = regex1.exec(sentence)) !== null) matches1.push({ index: sentenceStart + m.index, term: m[0] })
      regex1.lastIndex = 0
      while ((m = regex2.exec(sentence)) !== null) matches2.push({ index: sentenceStart + m.index, term: m[0] })
      
      if (matches1.length > 0 && matches2.length > 0) {
        // Add ONE match per sentence that contains both terms
        const firstMatch = matches1[0].index < matches2[0].index ? matches1[0] : matches2[0]
        matches.push({ 
          index: firstMatch.index, 
          term: `${matches1[0].term}...${matches2[0].term}`,
          terms: [matches1[0].term, matches2[0].term]
        })
      }
    }
  } else if (type === 'paragraph') {
    // Split by paragraph boundaries
    const paragraphs = text.split(/\n\s*\n|\r\n\s*\r\n/)
    let offset = 0
    for (const para of paragraphs) {
      regex1.lastIndex = 0; regex2.lastIndex = 0
      
      const matches1 = [], matches2 = []
      let m
      while ((m = regex1.exec(para)) !== null) matches1.push({ index: offset + m.index, term: m[0] })
      regex1.lastIndex = 0
      while ((m = regex2.exec(para)) !== null) matches2.push({ index: offset + m.index, term: m[0] })
      
      if (matches1.length > 0 && matches2.length > 0) {
        // Add ONE match per paragraph that contains both terms
        const firstMatch = matches1[0].index < matches2[0].index ? matches1[0] : matches2[0]
        matches.push({ 
          index: firstMatch.index, 
          term: `${matches1[0].term}...${matches2[0].term}`,
          terms: [matches1[0].term, matches2[0].term]
        })
      }
      offset += para.length + 2 // Account for paragraph separator
    }
  }
  
  // Sort by index
  matches.sort((a, b) => a.index - b.index)
  
  return matches
}

// Helper: Parse boolean/proximity search syntax to extract terms for highlighting
const parseBooleanSearchTerms = (input) => {
  if (!input || typeof input !== 'string') return null
  const trimmed = input.trim()
  
  // Check proximity searches first - return terms for highlighting purposes
  const proximity = parseProximitySearch(trimmed)
  if (proximity) return proximity.terms
  
  // Exact phrase in quotes - return as single term
  const phraseMatch = trimmed.match(/^"([^"]+)"$/)
  if (phraseMatch) return [phraseMatch[1]]
  
  // Single wildcard term (not part of boolean) - return as single term
  if (hasWildcard(trimmed) && !/\s(AND|OR|NOT|NEAR|ONEAR|AROUND|SENTENCE|PARAGRAPH)\s/i.test(trimmed)) {
    return [trimmed]
  }
  
  // Lookahead patterns like (?=.*\bterm1\b)(?=.*\bterm2\b).*
  if (trimmed.includes('(?=') && trimmed.includes('.*')) {
    const termMatches = [...trimmed.matchAll(/\(\?=\.\*(?:\\b)?(\w+)(?:\\b)?\)/g)]
    const terms = termMatches.map(m => m[1]).filter(Boolean)
    if (terms.length >= 2) return terms
  }
  
  // Extract NOT terms and remove them (we don't highlight excluded terms)
  let remaining = trimmed
  let hadNotOperator = false
  if (/\sNOT\s/i.test(remaining)) {
    remaining = remaining.split(/\s+NOT\s+/i)[0].trim()
    hadNotOperator = true
  }
  // Handle -term and !term exclusions
  if (/(?:^|\s)[-!]\w+/.test(remaining)) {
    remaining = remaining.replace(/(?:^|\s)[-!]\w+/g, ' ').trim()
    hadNotOperator = true
  }
  
  // "term1 OR term2"
  if (/\sOR\s/i.test(remaining)) {
    const terms = remaining.split(/\s+OR\s+/i).map(t => t.trim()).filter(Boolean)
    if (terms.length >= 1) return terms
  }
  
  // "term1 | term2"
  if (/\s*\|\s*/.test(remaining)) {
    const terms = remaining.split(/\s*\|\s*/).map(t => t.trim()).filter(Boolean)
    if (terms.length >= 1) return terms
  }
  
  // "term1 AND term2"
  if (/\sAND\s/i.test(remaining)) {
    const terms = remaining.split(/\s+AND\s+/i).map(t => t.trim()).filter(Boolean)
    if (terms.length >= 1) return terms
  }
  
  // "+term1 +term2"
  if (/^\+\S/.test(remaining) && remaining.includes(' +')) {
    const terms = remaining.split(/\s+/).filter(t => t.startsWith('+')).map(t => t.slice(1).trim()).filter(Boolean)
    if (terms.length >= 1) return terms
  }
  
  // "term1 & term2"
  if (/\s*&\s*/.test(remaining) && !remaining.includes('|')) {
    const terms = remaining.split(/\s*&\s*/).map(t => t.trim()).filter(Boolean)
    if (terms.length >= 1) return terms
  }
  
  // If we had a NOT operator and have a remaining term, return it for highlighting
  if (hadNotOperator && remaining) {
    return [remaining]
  }
  
  return null
}

// Build regex pattern from search term (handles boolean/proximity searches with wildcard support)
const buildSearchRegex = (searchTerm) => {
  if (!searchTerm) return new RegExp(DEFAULT_REGEX_STR, 'gi')
  
  // Check if searchTerm looks like a raw regex pattern (contains regex syntax like \b, \s, (?:, etc.)
  // If so, use it directly without parsing as boolean search
  const looksLikeRegex = /\\[bsdwBSDW]|\\s\+|\(\?[=!:]|\[\^?[^\]]+\]/.test(searchTerm)
  if (looksLikeRegex) {
    try {
      return new RegExp(`(${searchTerm})`, 'gi')
    } catch {
      // Fall through to other parsing methods if regex is invalid
    }
  }
  
  const booleanTerms = parseBooleanSearchTerms(searchTerm)
  if (booleanTerms) {
    // For boolean/proximity search, create a regex that matches any of the terms
    const patterns = booleanTerms.map(t => {
      if (hasWildcard(t)) return wildcardToRegex(t)
      return t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    })
    return new RegExp(`\\b(${patterns.join('|')})\\b`, 'gi')
  }
  
  // Check for standalone wildcard search (not boolean)
  if (hasWildcard(searchTerm)) {
    const pattern = wildcardToRegex(searchTerm)
    return new RegExp(`\\b(${pattern})\\b`, 'gi')
  }
  
  // Regular search - try to use as-is, fallback to escaped
  try {
    return new RegExp(`(${searchTerm})`, 'gi')
  } catch {
    const escaped = searchTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    return new RegExp(`(${escaped})`, 'gi')
  }
}

const SnippetRow = ({ id, fullText, term, terms, matchIndex, index, highlightFn }) => {
  const [expanded, setExpanded] = useState(false)
  const context = expanded ? 600 : 200
  const start = Math.max(0, matchIndex - context)
  const end = Math.min(fullText.length, matchIndex + (term?.length||0) + context)
  const snippetText = (start>0? '...' : '') + fullText.substring(start,end) + (end<fullText.length ? '...' : '')
  return (
    <div id={id} onClick={()=>setExpanded(!expanded)} className="bg-gray-50 p-4 rounded-lg border-l-4 border-yellow-400 shadow-sm transition-all duration-300 cursor-pointer hover:bg-blue-50 group">
      <div className="flex justify-between items-start mb-2"><p className="text-gray-400 text-xs font-sans font-bold uppercase tracking-wide group-hover:text-blue-500">Match #{index+1}</p><button className="text-xs text-blue-600 font-medium flex items-center gap-1 opacity-60 group-hover:opacity-100">{expanded ? 'Collapse' : <><Icon name="maximize" size={12} /> Expand Context</>}</button></div>
      <p className="text-gray-800 leading-relaxed font-serif text-sm">{highlightFn(snippetText, term, terms)}</p>
    </div>
  )
}

export default function SermonModal({ sermon, onClose, focusMatchIndex = 0 }){
  const [fullText, setFullText] = useState('')
  const [isLoading, setIsLoading] = useState(true)
  const [viewMode, setViewMode] = useState('snippets')
  const [mentions, setMentions] = useState([])
  const [videoUrl, setVideoUrl] = useState('')

  useEffect(()=>{
    setIsLoading(true)
    fetch(sermon.path).then(res => {
      if (!res.ok) return 'Transcript not available.'
      // Check content-type to avoid HTML fallback pages
      const contentType = res.headers.get('content-type') || ''
      if (contentType.includes('text/html')) {
        return 'Transcript not available. (File not found)'
      }
      return res.text()
    }).then(text=>{
      // Additional check: if content looks like HTML, it's probably a 404 fallback
      if (text.trim().startsWith('<!doctype') || text.trim().startsWith('<html') || text.trim().startsWith('<!DOCTYPE')) {
        setFullText('Transcript not available. The transcript file may be missing or corrupted.')
        setIsLoading(false)
        setViewMode('full')
        return
      }
      
      setFullText(text); setIsLoading(false)
      // Extract YouTube URL from transcript if available
      const ytMatch = text.match(/https?:\/\/(?:www\.)?(?:youtube\.com\/watch\?v=[A-Za-z0-9_\-]+|youtu\.be\/[A-Za-z0-9_\-]+)/i)
      if(ytMatch) setVideoUrl(ytMatch[0])
      
      // Check if this is a proximity search
      const proximityMatches = findProximityMatches(text, sermon.searchTerm)
      let found
      if (proximityMatches !== null) {
        // Use proximity-aware matches
        found = proximityMatches
      } else {
        // Regular search
        const regex = buildSearchRegex(sermon.searchTerm)
        found = []; let match; while((match = regex.exec(text)) !== null) { found.push({ index: match.index, term: match[0] }) }
      }
      
      setMentions(found);
      if(found.length===0) setViewMode('full')
      else {
        if(typeof focusMatchIndex === 'number'){
          setViewMode('snippets')
          setTimeout(()=>{
            const el = document.getElementById(`match-${focusMatchIndex}`)
            if(el) el.scrollIntoView({ behavior: 'smooth', block: 'center' })
          }, 150)
        }
      }
    }).catch(()=>{ setFullText('Error loading transcript.'); setIsLoading(false) })
  }, [sermon])

  const highlight = (text, term, terms) => {
    if(!text) return ''
    let splitter
    
    // If terms array is provided (from proximity match), use it directly
    if (terms && Array.isArray(terms) && terms.length > 0) {
      const escaped = terms.map(t => {
        if (hasWildcard(t)) return wildcardToRegex(t)
        return t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
      })
      splitter = new RegExp(`\\b(${escaped.join('|')})\\b`, 'gi')
    } else {
      // Build regex that handles boolean searches
      const booleanTerms = parseBooleanSearchTerms(term)
      if (booleanTerms) {
        const escaped = booleanTerms.map(t => {
          if (hasWildcard(t)) return wildcardToRegex(t)
          return t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
        })
        splitter = new RegExp(`\\b(${escaped.join('|')})\\b`, 'gi')
      } else {
        const t = term || DEFAULT_REGEX_STR
        try {
          splitter = new RegExp(`(${t})`, 'gi')
        } catch (e) {
          const escaped = t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
          splitter = new RegExp(`(${escaped})`, 'gi')
        }
      }
    }
    // split using a case-insensitive regex; captured groups become array entries at odd indices
    const parts = text.split(splitter)
    return parts.map((part, i) => i % 2 === 1 ? <span key={i} className="bg-yellow-200 text-yellow-900 px-0.5 rounded">{part}</span> : part)
  }

  const downloadText = () => { const a = document.createElement('a'); a.href = sermon.path; a.download = `${sermon.date} - ${sermon.title}.txt`; a.click(); }

  return (
    <div className="fixed inset-0 bg-black/60 backdrop-blur-sm z-50 flex items-center justify-center p-4" onClick={onClose}>
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-5xl h-[90vh] flex flex-col overflow-hidden" onClick={e=>e.stopPropagation()}>
        <div className="p-5 border-b bg-gray-50 flex justify-between items-start">
          <div className="flex-1 pr-4">
            <h2 className="text-xl font-bold text-gray-900 line-clamp-1">{sermon.title}</h2>
            <div className="flex flex-wrap gap-2 mt-2 text-sm text-gray-600"><span className="bg-white border px-2 py-0.5 rounded">{sermon.church}</span><span className="bg-white border px-2 py-0.5 rounded">{sermon.date}</span><span className="bg-green-100 text-green-800 border border-green-200 px-2 py-0.5 rounded font-bold">{mentions.length} Matches</span></div>
            {videoUrl && (
              <a href={videoUrl} target="_blank" rel="noopener noreferrer" className="inline-block mt-3 px-3 py-1 bg-red-600 text-white text-sm hover:bg-red-700 rounded font-medium">
                ▶ Watch on YouTube
              </a>
            )}
          </div>
          <div className="flex gap-2"><button onClick={downloadText} className="p-2 bg-blue-50 text-blue-600 hover:bg-blue-100 rounded-lg"><Icon name="download" /></button><button onClick={onClose} className="p-2 bg-red-50 text-red-600 hover:bg-red-100 rounded-lg"><Icon name="x" /></button></div>
        </div>
        <div className="px-6 py-3 border-b flex gap-4 bg-white shadow-sm z-10">
          <button onClick={()=>setViewMode('snippets')} className={`flex items-center gap-2 text-sm font-medium px-3 py-1.5 rounded-md ${viewMode==='snippets' ? 'bg-blue-100 text-blue-700' : 'text-gray-500'}`} disabled={mentions.length===0}><Icon name="eye" size={16} /> Context ({mentions.length})</button>
          <button onClick={()=>setViewMode('full')} className={`flex items-center gap-2 text-sm font-medium px-3 py-1.5 rounded-md ${viewMode==='full' ? 'bg-blue-100 text-blue-700' : 'text-gray-500'}`}><Icon name="alignLeft" size={16} /> Full Transcript</button>
        </div>
        <div className="flex-1 overflow-y-auto p-8 bg-white font-serif text-gray-800 leading-relaxed text-base">
          {isLoading ? <p className="text-center text-gray-400 italic">Loading content...</p> : viewMode==='snippets' ? (<div className="max-w-3xl mx-auto space-y-4">{mentions.map((m,i)=><SnippetRow key={i} id={`match-${i}`} fullText={fullText} term={m.term} terms={m.terms} matchIndex={m.index} index={i} highlightFn={highlight} />)}</div>) : <div className="max-w-3xl mx-auto whitespace-pre-wrap">{highlight(fullText, sermon.searchTerm)}</div>}
        </div>
      </div>
    </div>
  )
}
