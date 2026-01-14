import React, { useState, useEffect } from 'react'
import Icon from './Icon'
import { DEFAULT_REGEX_STR } from '../constants_local'

const TIMESTAMP_TOKEN_REGEX = /\[(\d{1,2}:\d{2}(?::\d{2})?)\]/g

const safeDecodeURIComponent = (value) => {
  if (typeof value !== 'string') return ''
  try {
    return decodeURIComponent(value)
  } catch {
    return value
  }
}

// IMPORTANT: Do NOT use encodeURIComponent() for transcript paths.
// Only encode spaces and '#' (hash breaks URLs by becoming a fragment).
const normalizeTranscriptPathForUrl = (maybeEncodedPath) => {
  const decoded = safeDecodeURIComponent(maybeEncodedPath)
  return decoded
    .split('/')
    .map((part) => part.replace(/ /g, '%20').replace(/#/g, '%23'))
    .join('/')
}

const buildCandidateUrls = (basePath, normalizedPath) => {
  const rel = (normalizedPath || '').replace(/^\/+/, '')
  const base = typeof basePath === 'string' && basePath.length ? basePath : '/'

  let baseUrl
  try {
    // Check if base is absolute (e.g. R2 URL)
    // eslint-disable-next-line no-new
    new URL(base)
    baseUrl = base
  } catch {
    // Relative path - resolve against current origin
    baseUrl = new URL(base, window.location.origin).toString()
  }

  const withBase = new URL(rel, baseUrl).toString()
  const fromRoot = new URL(`/${rel}`, window.location.origin).toString()

  // Try base-prefixed first, then root fallback
  return Array.from(new Set([withBase, fromRoot]))
}

const getTranscriptFetchCacheMode = () => {
  // In dev, Vite can previously serve index.html for missing/static edge cases (e.g. %23 paths),
  // and browsers may cache that HTML. Use no-store to avoid stale HTML masking real files.
  return import.meta.env.DEV ? 'no-store' : 'force-cache'
}

const timestampToSeconds = (ts) => {
  if (!ts) return null
  const parts = ts.split(':').map(p => parseInt(p, 10))
  if (parts.some(n => Number.isNaN(n))) return null
  if (parts.length === 2) {
    const [m, s] = parts
    return m * 60 + s
  }
  if (parts.length === 3) {
    const [h, m, s] = parts
    return h * 3600 + m * 60 + s
  }
  return null
}

const buildYouTubeUrlAtSeconds = (videoUrl, seconds) => {
  if (!videoUrl || typeof videoUrl !== 'string' || seconds == null) return null
  try {
    const url = new URL(videoUrl)
    url.searchParams.set('t', String(seconds))
    return url.toString()
  } catch {
    return null
  }
}

const renderTimestampLinks = (text, videoUrl) => {
  if (!text) return text
  if (!videoUrl) return text

  TIMESTAMP_TOKEN_REGEX.lastIndex = 0

  const parts = []
  let lastIndex = 0
  let match
  while ((match = TIMESTAMP_TOKEN_REGEX.exec(text)) !== null) {
    const full = match[0]
    const ts = match[1]
    const start = match.index
    const end = start + full.length

    if (start > lastIndex) parts.push(text.slice(lastIndex, start))

    const seconds = timestampToSeconds(ts)
    const href = buildYouTubeUrlAtSeconds(videoUrl, seconds)
    if (href) {
      parts.push(
        <a
          key={`${start}-${ts}`}
          href={href}
          target="_blank"
          rel="noopener noreferrer"
          className="text-blue-700 hover:text-blue-900 underline"
          onClick={(e) => e.stopPropagation()}
          title={`Open YouTube at ${ts}`}
        >
          {full}
        </a>
      )
    } else {
      parts.push(full)
    }
    lastIndex = end
  }

  if (lastIndex < text.length) parts.push(text.slice(lastIndex))
  return parts
}

const escapeRegex = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')

const TIMESTAMP_GAP_REGEX_PART = '(?:\\s|\\[[^\\]]+\\]\\s*)+'

const applyTimestampGaps = (pattern) => {
  if (!pattern) return pattern
  // Replace whitespace runs with a pattern that also allows [M:SS] tokens between words.
  return pattern.replace(/\\s\+/g, TIMESTAMP_GAP_REGEX_PART)
}

// Fast mapping from a match position in the plain transcript to an approximate
// position in the timestamped transcript. This avoids expensive full-text
// searches for every match when there are many results.
const approximateIndexInTimestamped = (plainText, matchIndex, timestampedText, fallbackNeedle) => {
  const plainTextLen = plainText ? plainText.length : 0
  if (!timestampedText || !plainTextLen || plainTextLen <= 0) return null
  const tsLen = timestampedText.length
  if (tsLen === 0) return null

  const ratio = Math.min(1, Math.max(0, matchIndex / plainTextLen))
  const base = Math.min(tsLen - 1, Math.max(0, Math.floor(ratio * tsLen)))

  // Local-window search around the approximate position to improve accuracy.
  // Use anchor phrases derived from the plain transcript and allow timestamps between words.
  const windowRadius = 8000
  const start = Math.max(0, base - windowRadius)
  const end = Math.min(tsLen, base + windowRadius)
  const hay = timestampedText.slice(start, end)
  const target = base - start

  const words = getWordsAroundIndex(plainText, matchIndex, 3, 6)
    .map(w => w.replace(/^'+|'+$/g, ''))
    .filter(Boolean)

  const findClosestByRegex = (re) => {
    if (!re) return null
    let best = null
    let guard = 0
    let m
    while ((m = re.exec(hay)) !== null && guard < 120) {
      const idx = m.index
      const dist = Math.abs(idx - target)
      if (!best || dist < best.dist) best = { idx, dist }
      if (dist === 0) break
      // Avoid infinite loops on zero-length matches
      if (m.index === re.lastIndex) re.lastIndex++
      guard++
    }
    return best ? (start + best.idx) : null
  }

  // 1) Best effort: search for a nearby multi-word anchor phrase.
  const joiner = TIMESTAMP_GAP_REGEX_PART
  const maxLen = Math.min(8, words.length)
  for (let len = maxLen; len >= 3; len--) {
    for (let offset = 0; offset <= words.length - len; offset++) {
      const subset = words.slice(offset, offset + len)
      const pattern = subset.map(w => `\\b${escapeRegex(w)}\\b`).join(joiner)
      const re = new RegExp(pattern, 'ig')
      const hit = findClosestByRegex(re)
      if (typeof hit === 'number') return hit
    }
  }

  // 2) Fallback: search for the matched term itself, allowing timestamps between words.
  const needle = (fallbackNeedle || '').toString().trim()
  if (needle) {
    let needlePattern = escapeRegex(needle)
    needlePattern = needlePattern.replace(/\s+/g, '\\s+')
    needlePattern = applyTimestampGaps(needlePattern)
    const re = new RegExp(needlePattern, 'ig')
    const hit = findClosestByRegex(re)
    if (typeof hit === 'number') return hit
  }

  return base
}

const firstTimestampTokenIndex = (text) => {
  if (!text) return null
  const idx = text.search(/\[(\d{1,2}:\d{2}(?::\d{2})?)\]/)
  return idx >= 0 ? idx : null
}

const buildLineSnippet = (text, index, linesBefore, linesAfter, maxChars) => {
  if (!text) return ''
  const len = text.length
  const at = Math.min(Math.max(0, index || 0), Math.max(0, len - 1))

  // Find the line containing `at`
  const lineStart = text.lastIndexOf('\n', at - 1) + 1
  const lineEnd = (() => {
    const e = text.indexOf('\n', at)
    return e === -1 ? len : e
  })()

  // Expand to surrounding lines
  let start = lineStart
  for (let i = 0; i < linesBefore; i++) {
    const prev = text.lastIndexOf('\n', Math.max(0, start - 2))
    if (prev === -1) { start = 0; break }
    start = prev + 1
  }

  let end = lineEnd
  for (let i = 0; i < linesAfter; i++) {
    const next = text.indexOf('\n', end + 1)
    if (next === -1) { end = len; break }
    end = next
  }

  // If the selected region is huge (very long lines), cap by chars around `at`.
  if (maxChars && (end - start) > maxChars) {
    const half = Math.floor(maxChars / 2)
    start = Math.max(0, at - half)
    end = Math.min(len, at + half)
  }

  return (start > 0 ? '...' : '') + text.slice(start, end) + (end < len ? '...' : '')
}

const getWordsAroundIndex = (text, idx, before = 3, after = 6) => {
  if (!text) return []
  const wordRe = /\b[\w']+\b/g
  const words = []
  let m
  while ((m = wordRe.exec(text)) !== null) {
    words.push({ word: m[0], index: m.index, end: m.index + m[0].length })
  }
  if (words.length === 0) return []

  let center = -1
  for (let i = 0; i < words.length; i++) {
    if (words[i].index <= idx && idx < words[i].end) {
      center = i
      break
    }
  }
  if (center === -1) {
    // fallback: closest word before idx
    for (let i = words.length - 1; i >= 0; i--) {
      if (words[i].index <= idx) {
        center = i
        break
      }
    }
  }
  if (center === -1) center = 0

  const start = Math.max(0, center - before)
  const end = Math.min(words.length, center + after + 1)
  return words.slice(start, end).map(w => w.word).filter(w => w.length >= 2)
}

const findApproximateIndexInTimestamped = (plainText, matchIndex, timestampedText, fallbackNeedle) => {
  if (!timestampedText) return null

  const words = getWordsAroundIndex(plainText, matchIndex, 3, 6)
    .map(w => w.replace(/^'+|'+$/g, ''))
    .filter(Boolean)

  const joiner = '(?:\\s|\\[[^\\]]+\\])+' // allow timestamps between words

  // Try to find a nearby anchor phrase (3..8 words) in the timestamped transcript.
  const maxLen = Math.min(8, words.length)
  for (let len = maxLen; len >= 3; len--) {
    for (let offset = 0; offset <= words.length - len; offset++) {
      const subset = words.slice(offset, offset + len)
      const pattern = `(?:\\[[^\\]]+\\]\\s*)?` + subset.map(w => `\\b${escapeRegex(w)}\\b`).join(joiner)
      const re = new RegExp(pattern, 'i')
      const idx = timestampedText.search(re)
      if (idx !== -1) return idx
    }
  }

  // Fallback: look for the matched term itself.
  if (fallbackNeedle) {
    const needle = String(fallbackNeedle).trim()
    if (needle) {
      const idx = timestampedText.toLowerCase().indexOf(needle.toLowerCase())
      if (idx !== -1) return idx
    }
  }

  return null
}

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
// Helper to strip surrounding quotes from terms
const stripQuotes = (t) => {
  const trimmed = t.trim()
  if ((trimmed.startsWith('"') && trimmed.endsWith('"')) || (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
    return trimmed.slice(1, -1)
  }
  return trimmed
}

const parseBooleanSearchTerms = (input) => {
  if (!input || typeof input !== 'string') return null
  const trimmed = input.trim()
  
  // Check proximity searches first - return terms for highlighting purposes
  const proximity = parseProximitySearch(trimmed)
  if (proximity) return proximity.terms.map(stripQuotes)
  
  // Exact phrase in quotes - return as single term
  const phraseMatch = trimmed.match(/^"([^"]+)"$/)
  if (phraseMatch) return [phraseMatch[1]]
  
  // Single wildcard term (not part of boolean) - return as single term
  if (hasWildcard(trimmed) && !/\s(AND|OR|NOT|NEAR|ONEAR|AROUND|SENTENCE|PARAGRAPH)\s/i.test(trimmed)) {
    return [stripQuotes(trimmed)]
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
    const terms = remaining.split(/\s+OR\s+/i).map(t => stripQuotes(t.trim())).filter(Boolean)
    if (terms.length >= 1) return terms
  }
  
  // "term1 | term2"
  if (/\s*\|\s*/.test(remaining)) {
    const terms = remaining.split(/\s*\|\s*/).map(t => stripQuotes(t.trim())).filter(Boolean)
    if (terms.length >= 1) return terms
  }
  
  // "term1 AND term2"
  if (/\sAND\s/i.test(remaining)) {
    const terms = remaining.split(/\s+AND\s+/i).map(t => stripQuotes(t.trim())).filter(Boolean)
    if (terms.length >= 1) return terms
  }
  
  // "+term1 +term2"
  if (/^\+\S/.test(remaining) && remaining.includes(' +')) {
    const terms = remaining.split(/\s+/).filter(t => t.startsWith('+')).map(t => stripQuotes(t.slice(1).trim())).filter(Boolean)
    if (terms.length >= 1) return terms
  }
  
  // "term1 & term2"
  if (/\s*&\s*/.test(remaining) && !remaining.includes('|')) {
    const terms = remaining.split(/\s*&\s*/).map(t => stripQuotes(t.trim())).filter(Boolean)
    if (terms.length >= 1) return terms
  }
  
  // If we had a NOT operator and have a remaining term, return it for highlighting
  if (hadNotOperator && remaining) {
    return [stripQuotes(remaining)]
  }
  
  return null
}

// Build regex pattern from search term (handles boolean/proximity searches with wildcard support)
const buildSearchRegex = (searchTerm, wholeWords = true) => {
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
      let escaped = t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
      // Handle apostrophes - match various apostrophe types or no apostrophe
      escaped = escaped.replace(/'/g, "['''']?")
      // Handle multi-word phrases with flexible whitespace
      escaped = escaped.replace(/\s+/g, '\\s+')
      return escaped
    })
    return new RegExp(`\\b(${patterns.join('|')})\\b`, 'gi')
  }
  
  // Check for standalone wildcard search (not boolean)
  if (hasWildcard(searchTerm)) {
    const pattern = wildcardToRegex(searchTerm)
    return new RegExp(`\\b(${pattern})\\b`, 'gi')
  }
  
  // Regular search - respect wholeWords setting
  try {
    if (wholeWords) {
      const escaped = searchTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
      return new RegExp(`\\b(${escaped})\\b`, 'gi')
    }
    return new RegExp(`(${searchTerm})`, 'gi')
  } catch {
    const escaped = searchTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    if (wholeWords) {
      return new RegExp(`\\b(${escaped})\\b`, 'gi')
    }
    return new RegExp(`(${escaped})`, 'gi')
  }
}

const SnippetRow = ({ id, fullText, plainTextForAnchor, timestampedTextForAnchor, term, terms, matchIndex, index, highlightFn, useLineSnippets = false }) => {
  const [expanded, setExpanded] = useState(false)

  let snippetText = ''
  if (useLineSnippets) {
    let effectiveText = fullText
    let effectiveIndex = matchIndex

    // When showing timestamped transcript, map the plain match to a nearby location in the timestamped text
    if (timestampedTextForAnchor && plainTextForAnchor) {
      const fallbackNeedle = (terms && Array.isArray(terms) && terms.length > 0) ? terms[0] : term
      const mapped = approximateIndexInTimestamped(plainTextForAnchor, matchIndex, timestampedTextForAnchor, fallbackNeedle)
      if (typeof mapped === 'number' && mapped >= 0) {
        effectiveText = timestampedTextForAnchor
        effectiveIndex = mapped
      }
    }

    // If we landed in the timestamped preamble/header, jump to the first real timestamp.
    if (effectiveText === timestampedTextForAnchor) {
      const firstTs = firstTimestampTokenIndex(effectiveText)
      if (typeof firstTs === 'number' && firstTs >= 0 && effectiveIndex < firstTs) {
        effectiveIndex = firstTs
      }
    }

    // Prefer line-based snippets so adjacent matches don't look identical.
    const linesBefore = expanded ? 5 : 2
    const linesAfter = expanded ? 6 : 3
    const maxChars = expanded ? 2400 : 1100
    snippetText = buildLineSnippet(effectiveText, effectiveIndex, linesBefore, linesAfter, maxChars)
  } else {
    const context = expanded ? 600 : 200
    const start = Math.max(0, matchIndex - context)
    const end = Math.min(fullText.length, matchIndex + (term?.length || 0) + context)
    snippetText = (start > 0 ? '...' : '') + fullText.substring(start, end) + (end < fullText.length ? '...' : '')
  }

  const renderedSnippet = React.useMemo(() => highlightFn(snippetText, term, terms), [snippetText, term, terms, highlightFn])

  return (
    <div id={id} onClick={()=>setExpanded(!expanded)} className="bg-gray-50 p-3 sm:p-4 rounded-lg border-l-4 border-yellow-400 shadow-sm transition-all duration-300 cursor-pointer hover:bg-blue-50 group overflow-hidden">
      <div className="flex justify-between items-start mb-1.5 sm:mb-2"><p className="text-gray-400 text-xs font-sans font-bold uppercase tracking-wide group-hover:text-blue-500">Match #{index+1}</p><button className="text-xs text-blue-600 font-medium flex items-center gap-1 opacity-60 group-hover:opacity-100">{expanded ? <><Icon name="chevronUp" size={12} /> Collapse</> : <><Icon name="chevronDown" size={12} /> Expand</>}</button></div>
      <p className="text-gray-800 leading-relaxed font-serif text-[13px] sm:text-sm break-words">{renderedSnippet}</p>
    </div>
  )
}

export default function SermonModal({ sermon, onClose, focusMatchIndex = 0, wholeWords = true, onSaveProgress, resumePosition, relatedSermons = [], onSelectRelated, requireConsent = false }){
  const [plainText, setPlainText] = useState('')
  const [timestampedText, setTimestampedText] = useState('')
  const [hasTimestamped, setHasTimestamped] = useState(false)
  const [checkingTimestamped, setCheckingTimestamped] = useState(false)
  const [showTimestamps, setShowTimestamps] = useState(false)
  const [loadingTimestamped, setLoadingTimestamped] = useState(false)

  const [isLoading, setIsLoading] = useState(true)
  const [viewMode, setViewMode] = useState('snippets')
  const [mentions, setMentions] = useState([])
  const [videoUrl, setVideoUrl] = useState('')
  const [hasConsented, setHasConsented] = useState(false)
  const [consentChecked, setConsentChecked] = useState(false)
  const [showDownloadConsent, setShowDownloadConsent] = useState(false)
  const [downloadConsentChecked, setDownloadConsentChecked] = useState(false)
  const contentRef = React.useRef(null)
  const touchStartYRef = React.useRef(0)
  const [fullHighlightPhase, setFullHighlightPhase] = React.useState(0)

  // Prevent the page behind the modal from scrolling/bouncing (mobile Safari overscroll).
  useEffect(() => {
    const scrollY = window.scrollY
    const prevBodyOverflow = document.body.style.overflow
    const prevBodyPosition = document.body.style.position
    const prevBodyTop = document.body.style.top
    const prevBodyWidth = document.body.style.width
    const prevHtmlOverflow = document.documentElement.style.overflow

    document.documentElement.style.overflow = 'hidden'
    document.body.style.overflow = 'hidden'
    document.body.style.position = 'fixed'
    document.body.style.top = `-${scrollY}px`
    document.body.style.width = '100%'

    return () => {
      document.documentElement.style.overflow = prevHtmlOverflow
      document.body.style.overflow = prevBodyOverflow
      document.body.style.position = prevBodyPosition
      document.body.style.top = prevBodyTop
      document.body.style.width = prevBodyWidth
      window.scrollTo(0, scrollY)
    }
  }, [])

  // iOS Safari can still "rubber-band" scroll fixed overlays.
  // Prevent overscroll past the top/bottom of the modal's scroll container,
  // prevent touch scrolling outside the scroll container, and block horizontal
  // pan gestures inside the scroller (to avoid sideways rubber-banding).
  useEffect(() => {
    const scroller = contentRef.current
    if (!scroller) return

    const isIos = typeof navigator !== 'undefined' && /iP(ad|hone|od)/.test(navigator.userAgent)
    if (!isIos) return

    const onTouchStart = (e) => {
      const t = e.touches && e.touches[0]
      if (!t) return
      touchStartYRef.current = t.clientY
      // stash X on the same ref object to avoid adding another ref
      touchStartYRef.currentX = t.clientX
    }

    const onTouchMove = (e) => {
      const t = e.touches && e.touches[0]
      if (!t) return

      // If touch is outside the scroller, block it.
      const target = e.target
      if (target && scroller && !scroller.contains(target)) {
        e.preventDefault()
        return
      }

      const currentY = t.clientY
      const currentX = t.clientX
      const startX = touchStartYRef.currentX || 0
      const deltaY = currentY - touchStartYRef.current
      const deltaX = currentX - startX

      // Block horizontal pan inside the transcript area.
      if (Math.abs(deltaX) > Math.abs(deltaY) + 6) {
        e.preventDefault()
        return
      }

      const scrollTop = scroller.scrollTop
      const maxScrollTop = scroller.scrollHeight - scroller.clientHeight

      const atTop = scrollTop <= 0
      const atBottom = scrollTop >= maxScrollTop - 1

      // deltaY > 0 means user is pulling content down (scrolling up)
      // deltaY < 0 means user is pushing content up (scrolling down)
      if ((atTop && deltaY > 0) || (atBottom && deltaY < 0)) {
        e.preventDefault()
      }
    }

    // Use { passive: false } so preventDefault works on iOS.
    scroller.addEventListener('touchstart', onTouchStart, { passive: true })
    scroller.addEventListener('touchmove', onTouchMove, { passive: false })
    document.addEventListener('touchmove', onTouchMove, { passive: false })

    return () => {
      scroller.removeEventListener('touchstart', onTouchStart)
      scroller.removeEventListener('touchmove', onTouchMove)
      document.removeEventListener('touchmove', onTouchMove)
    }
  }, [contentRef])

  // Track scroll position for reading progress
  useEffect(() => {
    const el = contentRef.current
    if (!el || !onSaveProgress) return
    
    let timeout
    const handleScroll = () => {
      clearTimeout(timeout)
      timeout = setTimeout(() => {
        const position = el.scrollTop / (el.scrollHeight - el.clientHeight)
        if (position > 0.01 && position < 0.99) {
          onSaveProgress(sermon.id, position)
        }
      }, 500)
    }
    el.addEventListener('scroll', handleScroll)
    return () => el.removeEventListener('scroll', handleScroll)
  }, [sermon.id, onSaveProgress])

  // Resume scroll position on mount
  useEffect(() => {
    // Only restore reading progress for the Full Transcript view.
    // Context/snippets should always start at the top.
    if (viewMode === 'full' && resumePosition && contentRef.current && !isLoading) {
      setTimeout(() => {
        const el = contentRef.current
        if (el) {
          el.scrollTop = resumePosition * (el.scrollHeight - el.clientHeight)
        }
      }, 100)
    }
  }, [resumePosition, isLoading, viewMode])

  // Always start Context (snippets) at the top.
  useEffect(() => {
    if (isLoading) return
    if (viewMode !== 'snippets') return
    const el = contentRef.current
    if (!el) return
    el.scrollTop = 0
  }, [viewMode, isLoading])

  useEffect(()=>{
    setIsLoading(true)
    setShowTimestamps(false)
    setHasTimestamped(false)
    setTimestampedText('')
    setCheckingTimestamped(false)
    setLoadingTimestamped(false)
    setVideoUrl(sermon.videoUrl || sermon.url || '')
    
    // Handle missing path
    if (!sermon.path) {
      setPlainText('Transcript not available. (No path configured)')
      setIsLoading(false)
      setViewMode('full')
      return
    }
    
    // Check for R2 data URL first
    const basePath = import.meta.env.VITE_DATA_URL || import.meta.env.BASE_URL || '/'
    const encodedPath = normalizeTranscriptPathForUrl(sermon.path)
    const plainCandidates = buildCandidateUrls(basePath, encodedPath)

    const decodedTimestamped = (() => {
      if (sermon.timestampedPath) return safeDecodeURIComponent(sermon.timestampedPath)
      const decodedPlain = safeDecodeURIComponent(sermon.path)
      if (decodedPlain.endsWith('.timestamped.txt')) return decodedPlain
      if (decodedPlain.endsWith('.txt')) return decodedPlain.slice(0, -4) + '.timestamped.txt'
      return decodedPlain + '.timestamped.txt'
    })()

    const encodedTimestamped = normalizeTranscriptPathForUrl(decodedTimestamped)
    const timestampedCandidates = buildCandidateUrls(basePath, encodedTimestamped)
    const timestampedFetchPath = timestampedCandidates[0]

    const fetchFirstAvailableText = async (urls) => {
      const cacheMode = getTranscriptFetchCacheMode()
      for (const url of urls) {
        try {
          const res = await fetch(url, { cache: cacheMode })
          if (!res.ok) continue
          const ct = (res.headers.get('content-type') || '').toLowerCase()
          if (ct.includes('text/html')) continue
          const text = await res.text()
          const trimmed = (text || '').trim()
          if (trimmed.startsWith('<!doctype') || trimmed.startsWith('<html') || trimmed.startsWith('<!DOCTYPE')) continue
          return text
        } catch {
          // try next
        }
      }
      return null
    }

    // Prefer metadata flag (no network probe). Fallback to probe only for older metadata.
    const hasMetaHasTimestamped = typeof sermon.hasTimestamped === 'boolean'
    if (hasMetaHasTimestamped) {
      const ok = Boolean(sermon.hasTimestamped)
      setHasTimestamped(ok)
      if (ok) {
        // Default ON when timestamped transcript exists
        setShowTimestamps(true)
        // Defer so the plain transcript can paint immediately.
        setTimeout(() => loadTimestamped(timestampedFetchPath), 0)
      }
    } else {
      setCheckingTimestamped(true)
      const probeTimestamped = async () => {
        try {
          const head = await fetch(timestampedFetchPath, { method: 'HEAD', cache: getTranscriptFetchCacheMode() })
          if (!head.ok) return false
          const contentType = head.headers.get('content-type') || ''
          if (contentType.includes('text/html')) return false
          return true
        } catch {
          // Fallback: small Range request (some hosts may not support HEAD reliably)
          try {
            const r = await fetch(timestampedFetchPath, { headers: { Range: 'bytes=0-2047' }, cache: getTranscriptFetchCacheMode() })
            if (!r.ok) return false
            const contentType = r.headers.get('content-type') || ''
            if (contentType.includes('text/html')) return false
            const t = await r.text()
            const trimmed = (t || '').trim()
            if (trimmed.startsWith('<!doctype') || trimmed.startsWith('<html') || trimmed.startsWith('<!DOCTYPE')) return false
            const hasAnyTimestampToken = /\[(\d{1,2}:\d{2}(?::\d{2})?)\]/.test(trimmed)
            return trimmed.includes('TIMESTAMPED TRANSCRIPT') || hasAnyTimestampToken
          } catch {
            return false
          }
        }
      }

      probeTimestamped().then(ok => {
        setHasTimestamped(Boolean(ok))
        setCheckingTimestamped(false)
        if (ok) {
          setShowTimestamps(true)
          setTimeout(() => loadTimestamped(timestampedFetchPath), 0)
        }
      }).catch(() => {
        setHasTimestamped(false)
        setCheckingTimestamped(false)
      })
    }

    ;(async () => {
      const text = await fetchFirstAvailableText(plainCandidates)
      if (!text) {
        setPlainText('Transcript not available. (File not found)')
        setIsLoading(false)
        setViewMode('full')
        return
      }

      setPlainText(text)
      setIsLoading(false)
      // Extract YouTube URL from transcript if available
      const ytMatch = text.match(/https?:\/\/(?:www\.)?(?:youtube\.com\/watch\?v=[A-Za-z0-9_\-]+|youtu\.be\/[A-Za-z0-9_\-]+)/i)

      if (ytMatch) setVideoUrl(ytMatch[0])
      else setVideoUrl(sermon.videoUrl || sermon.url || '')

    })().catch(()=>{ setPlainText('Error loading transcript.'); setIsLoading(false) })
  }, [sermon, wholeWords])

  const displayText = showTimestamps && timestampedText ? timestampedText : plainText

  const highlightTermsForFullTranscript = React.useMemo(() => {
    if (!mentions || mentions.length === 0) return null
    const set = new Set()
    for (const m of mentions) {
      if (m && Array.isArray(m.terms) && m.terms.length > 0) {
        for (const t of m.terms) {
          if (t) set.add(String(t))
        }
      } else if (m && m.term) {
        set.add(String(m.term))
      }
    }
    const arr = Array.from(set)
    // Safety cap: avoid creating an excessively large regex.
    return arr.length > 60 ? arr.slice(0, 60) : arr
  }, [mentions])

  // Mentions are ALWAYS computed from the plain transcript so counts/ordering stay stable.
  useEffect(() => {
    if (!plainText) return

    const proximityMatches = findProximityMatches(plainText, sermon.searchTerm)
    let found
    if (proximityMatches !== null) {
      found = proximityMatches
    } else {
      const regex = buildSearchRegex(sermon.searchTerm, wholeWords)
      found = []
      let match
      while ((match = regex.exec(plainText)) !== null) {
        found.push({ index: match.index, term: match[0] })
      }
    }

    setMentions(found)

    if (found.length === 0) {
      if (requireConsent && !hasConsented) setViewMode('consent')
      else setViewMode('full')
      return
    }

    if (typeof focusMatchIndex === 'number') {
      setViewMode('snippets')
      setTimeout(() => {
        const el = document.getElementById(`match-${focusMatchIndex}`)
        if (el) el.scrollIntoView({ behavior: 'smooth', block: 'center' })
      }, 150)
    }
  }, [plainText, sermon.searchTerm, wholeWords])

  const highlight = (text, term, terms) => {
    if(!text) return ''
    let splitter
    
    // If terms array is provided (from proximity match), use it directly
    if (terms && Array.isArray(terms) && terms.length > 0) {
      const escaped = terms.map(t => {
        if (hasWildcard(t)) return wildcardToRegex(t)
        let p = t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
        p = p.replace(/\s+/g, '\\s+')
        if (showTimestamps) p = applyTimestampGaps(p)
        return p
      })
      splitter = new RegExp(`\\b(${escaped.join('|')})\\b`, 'gi')
    } else {
      // Build regex that handles boolean searches
      const booleanTerms = parseBooleanSearchTerms(term)
      if (booleanTerms) {
        const escaped = booleanTerms.map(t => {
          if (hasWildcard(t)) return wildcardToRegex(t)
          let p = t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
          p = p.replace(/\s+/g, '\\s+')
          if (showTimestamps) p = applyTimestampGaps(p)
          return p
        })
        splitter = new RegExp(`\\b(${escaped.join('|')})\\b`, 'gi')
      } else {
        const t = term || DEFAULT_REGEX_STR
        try {
          // For regular terms, respect the wholeWords setting
          if (wholeWords && !t.includes('\\b')) {
            let escaped = t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
            escaped = escaped.replace(/\s+/g, '\\s+')
            if (showTimestamps) escaped = applyTimestampGaps(escaped)
            splitter = new RegExp(`\\b(${escaped})\\b`, 'gi')
          } else {
            // If the user provided a regex, keep it; but in timestamp mode
            // allow timestamps between literal whitespace where possible.
            let pattern = `(${t})`
            if (showTimestamps) pattern = applyTimestampGaps(pattern)
            splitter = new RegExp(pattern, 'gi')
          }
        } catch (e) {
          let escaped = t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
          escaped = escaped.replace(/\s+/g, '\\s+')
          if (showTimestamps) escaped = applyTimestampGaps(escaped)
          if (wholeWords) {
            splitter = new RegExp(`\\b(${escaped})\\b`, 'gi')
          } else {
            splitter = new RegExp(`(${escaped})`, 'gi')
          }
        }
      }
    }
    // split using a case-insensitive regex; captured groups become array entries at odd indices
    const parts = text.split(splitter)
    return parts.flatMap((part, i) => {
      if (i % 2 === 1) {
        const rendered = renderTimestampLinks(part, showTimestamps ? videoUrl : null)
        return <span key={i} className="bg-yellow-200 text-yellow-900 px-0.5 rounded">{rendered}</span>
      }
      const rendered = renderTimestampLinks(part, showTimestamps ? videoUrl : null)
      return Array.isArray(rendered) ? rendered : [rendered]
    })
  }

  // Rendering the full highlighted transcript can be expensive on mobile.
  // To avoid a one-time blank/frozen screen on first open, render a lightweight
  // plain transcript first, then swap to the highlighted version after paint.
  useEffect(() => {
    if (viewMode !== 'full') return
    setFullHighlightPhase(0)
    const id = window.requestAnimationFrame(() => setFullHighlightPhase(1))
    return () => window.cancelAnimationFrame(id)
  }, [viewMode, displayText, showTimestamps, videoUrl, highlightTermsForFullTranscript, wholeWords, sermon.searchTerm])

  const fullTranscriptContent = React.useMemo(() => {
    if (viewMode !== 'full') return null

    if (fullHighlightPhase === 0) {
      // Fast path: show plain text (with timestamp links when enabled).
      return renderTimestampLinks(displayText, showTimestamps ? videoUrl : null)
    }

    // Highlight using the actual matched terms so the Full Transcript view stays
    // consistent with the Search Results list (and works for complex queries).
    return highlight(displayText, sermon.searchTerm, highlightTermsForFullTranscript)
  }, [viewMode, fullHighlightPhase, displayText, sermon.searchTerm, highlightTermsForFullTranscript, showTimestamps, videoUrl, wholeWords])

  const loadTimestamped = async (prefetchedPath = null) => {
    if (!sermon.path) return
    if (timestampedText) return

    setLoadingTimestamped(true)

    const basePath = import.meta.env.VITE_DATA_URL || import.meta.env.BASE_URL || '/'
    const candidates = prefetchedPath
      ? Array.from(new Set([prefetchedPath, ...buildCandidateUrls(basePath, prefetchedPath)]))
      : (() => {
          const decoded = safeDecodeURIComponent(sermon.timestampedPath || sermon.path)
          const decodedTimestamped = sermon.timestampedPath
            ? decoded
            : decoded.endsWith('.timestamped.txt')
              ? decoded
              : decoded.endsWith('.txt')
                ? decoded.slice(0, -4) + '.timestamped.txt'
                : decoded + '.timestamped.txt'
          const normalized = normalizeTranscriptPathForUrl(decodedTimestamped)
          return buildCandidateUrls(basePath, normalized)
        })()

    try {
      let loaded = null
      for (const url of candidates) {
        const res = await fetch(url, { cache: getTranscriptFetchCacheMode() })
        if (!res.ok) continue
        const contentType = (res.headers.get('content-type') || '').toLowerCase()
        if (contentType.includes('text/html')) continue
        const text = await res.text()
        const trimmed = (text || '').trim()
        if (trimmed.startsWith('<!doctype') || trimmed.startsWith('<html') || trimmed.startsWith('<!DOCTYPE')) continue
        loaded = text
        break
      }
      if (!loaded) throw new Error('not ok')
      setTimestampedText(loaded)
    } catch {
      setHasTimestamped(false)
      setShowTimestamps(false)
    } finally {
      setLoadingTimestamped(false)
    }
  }

  const handleDownloadClick = () => {
    if (requireConsent) {
      // Always show download consent when requireConsent is enabled
      setShowDownloadConsent(true)
    } else {
      downloadText()
    }
  }

  const downloadText = async () => { 
    // Decode then selectively encode only problematic chars (spaces, #)
    const basePath = import.meta.env.VITE_DATA_URL || import.meta.env.BASE_URL || '/'
    
    let downloadUrl
    let filename

    // Logic to choose between timestamped vs plain
    // Duplicate the path construction logic from the effect above
    if (showTimestamps && hasTimestamped) {
      const decodedTimestamped = (() => {
        if (sermon.timestampedPath) return safeDecodeURIComponent(sermon.timestampedPath)
        const decodedPlain = safeDecodeURIComponent(sermon.path)
        if (decodedPlain.endsWith('.timestamped.txt')) return decodedPlain
        if (decodedPlain.endsWith('.txt')) return decodedPlain.slice(0, -4) + '.timestamped.txt'
        return decodedPlain + '.timestamped.txt'
      })()

      const encodedTimestamped = normalizeTranscriptPathForUrl(decodedTimestamped)
      downloadUrl = buildCandidateUrls(basePath, encodedTimestamped)[0]
      filename = `${sermon.date} - ${sermon.title}.timestamped.txt`
    } else {
      const encodedPath = normalizeTranscriptPathForUrl(sermon.path)
      downloadUrl = buildCandidateUrls(basePath, encodedPath)[0]
      filename = `${sermon.date} - ${sermon.title}.txt`
    }

    try {
      // Fetch as blob to force download (avoid opening in tab)
      const resp = await fetch(downloadUrl)
      if (!resp.ok) throw new Error(`Download failed: ${resp.status}`)
      const blob = await resp.blob()
      const url = URL.createObjectURL(blob)
      
      const a = document.createElement('a')
      a.href = url
      a.download = filename
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      URL.revokeObjectURL(url)
    } catch (err) {
      console.error('Download fetch failed, falling back to direct link', err)
      const a = document.createElement('a')
      a.href = downloadUrl
      a.download = filename
      a.target = '_blank'
      a.rel = 'noopener noreferrer'
      a.click()
    }
    
    setShowDownloadConsent(false)
  }

  // Handle clicking Full Transcript tab with consent requirement
  const handleFullTranscriptClick = () => {
    if (requireConsent && !hasConsented) {
      // Show consent prompt instead of switching view
      setViewMode('consent')
    } else {
      setViewMode('full')
    }
  }

  return (
    <div className="fixed inset-0 bg-black/60 backdrop-blur-sm z-50 flex items-start sm:items-center justify-center p-2 sm:p-4 overscroll-none overflow-hidden" onClick={onClose}>
      {/* Download consent modal */}
      {showDownloadConsent && (
        <div className="fixed inset-0 bg-black/40 z-[60] flex items-center justify-center p-4" onClick={(e) => { e.stopPropagation(); setShowDownloadConsent(false); setDownloadConsentChecked(false); }}>
          <div className="bg-white rounded-xl shadow-2xl max-w-md w-full p-6" onClick={e => e.stopPropagation()}>
            <h3 className="text-lg font-bold text-gray-900 mb-4">Download Transcript</h3>
            <label className="flex items-start gap-3 cursor-pointer mb-3">
              <input 
                type="checkbox" 
                checked={downloadConsentChecked}
                onChange={(e) => setDownloadConsentChecked(e.target.checked)}
                className="mt-1 w-4 h-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
              />
              <span className="text-sm text-gray-700">
                I am downloading this transcript for research or verification.
              </span>
            </label>
            <p className="text-xs text-gray-500 mb-2 ml-7">
              Downloading is provided for research and verification, not redistribution. Please use the source video for the full viewing experience.
            </p>
            {videoUrl && (
              <a href={videoUrl} target="_blank" rel="noopener noreferrer" className="text-xs text-red-600 hover:text-red-700 mb-4 ml-7 flex items-center gap-1 font-medium">
                ▶ {videoUrl}
              </a>
            )}
            <div className="flex gap-3 justify-end mt-4">
              <button 
                onClick={() => { setShowDownloadConsent(false); setDownloadConsentChecked(false); }}
                className="px-4 py-2 text-sm text-gray-600 hover:bg-gray-100 rounded-lg"
              >
                Cancel
              </button>
              <button 
                onClick={() => { if (downloadConsentChecked) { setHasConsented(true); downloadText(); } }}
                disabled={!downloadConsentChecked}
                className={`px-4 py-2 text-sm font-medium rounded-lg ${downloadConsentChecked ? 'bg-blue-600 text-white hover:bg-blue-700' : 'bg-gray-200 text-gray-400 cursor-not-allowed'}`}
              >
                Download
              </button>
            </div>
          </div>
        </div>
      )}
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-5xl h-[95dvh] sm:h-[90vh] flex flex-col overflow-hidden touch-pan-y" onClick={e=>e.stopPropagation()}>
        <div className="p-3 sm:p-5 border-b bg-gray-50 flex gap-3 items-start">
          <div className="min-w-0 flex-1 pr-1 sm:pr-4">
            <div className="flex items-start gap-2 min-w-0">
              <h2 className="min-w-0 flex-1 text-base sm:text-xl font-bold text-gray-900 truncate">{sermon.title}</h2>
            </div>
            <div className="mt-2 flex items-center gap-2">
              <div className="min-w-0 flex-1 flex flex-nowrap sm:flex-wrap gap-2 text-xs sm:text-sm text-gray-600 overflow-x-auto sm:overflow-visible">
                <span className="bg-white border px-1.5 sm:px-2 py-0.5 rounded whitespace-nowrap">{sermon.church || sermon.venue}</span>
                <span className="bg-white border px-1.5 sm:px-2 py-0.5 rounded whitespace-nowrap">{sermon.date}</span>
                {sermon.location && (
                  <span className="bg-teal-50 text-teal-700 border border-teal-200 px-1.5 sm:px-2 py-0.5 rounded whitespace-nowrap">{sermon.location}</span>
                )}
                {sermon.speaker && sermon.speaker !== 'Unknown Speaker' && (
                  <span className="bg-purple-50 text-purple-700 border border-purple-200 px-1.5 sm:px-2 py-0.5 rounded whitespace-nowrap">{sermon.speaker}</span>
                )}
                {sermon.type && sermon.type !== 'Service' && (
                  <span className="bg-amber-50 text-amber-700 border border-amber-200 px-1.5 sm:px-2 py-0.5 rounded whitespace-nowrap">{sermon.type}</span>
                )}
                {sermon.language && sermon.language !== 'Unknown' && (
                  <span className="bg-gray-100 text-gray-700 border border-gray-200 px-1.5 sm:px-2 py-0.5 rounded whitespace-nowrap">{sermon.language}</span>
                )}
                {(sermon.durationMinutes > 0 || sermon.durationHrs > 0) && (
                  <span className="bg-blue-50 text-blue-700 border border-blue-200 px-1.5 sm:px-2 py-0.5 rounded whitespace-nowrap">
                    {sermon.durationMinutes > 0
                      ? (sermon.durationMinutes >= 60 
                          ? `${(sermon.durationMinutes / 60).toFixed(1)} hours` 
                          : `${sermon.durationMinutes} min`)
                      : (sermon.videoUrl || sermon.url) 
                        ? (sermon.durationHrs >= 1 ? `${sermon.durationHrs.toFixed(1)} hrs video` : `${Math.round(sermon.durationHrs * 60)} min video`)
                        : (sermon.durationHrs >= 1 ? `~${sermon.durationHrs.toFixed(1)} hours` : `~${Math.round(sermon.durationHrs * 60)} min`)
                    }
                  </span>
                )}
                {sermon.wordCount > 0 && (
                  <span className="bg-indigo-50 text-indigo-700 border border-indigo-200 px-1.5 sm:px-2 py-0.5 rounded whitespace-nowrap">
                    {sermon.wordCount.toLocaleString()} words
                  </span>
                )}
                <span className="bg-green-100 text-green-800 border border-green-200 px-1.5 sm:px-2 py-0.5 rounded font-bold whitespace-nowrap">{mentions.length} Matches</span>
              </div>
              {videoUrl && (
                <a href={videoUrl} target="_blank" rel="noopener noreferrer" className="hidden sm:inline-block shrink-0 px-3 py-1 bg-red-600 text-white text-xs sm:text-sm hover:bg-red-700 rounded font-medium whitespace-nowrap">
                  ▶ Watch on YouTube
                </a>
              )}
            </div>
          </div>
          <div className="flex gap-2 shrink-0">
            {videoUrl && (
              <a
                href={videoUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="sm:hidden p-1.5 bg-red-50 text-red-600 hover:bg-red-100 rounded-lg font-bold"
                onClick={(e) => e.stopPropagation()}
                title="Watch on YouTube"
              >
                ▶
              </a>
            )}
            <button onClick={handleDownloadClick} className="p-1.5 sm:p-2 bg-blue-50 text-blue-600 hover:bg-blue-100 rounded-lg"><Icon name="download" /></button>
            <button onClick={onClose} className="p-1.5 sm:p-2 bg-red-50 text-red-600 hover:bg-red-100 rounded-lg"><Icon name="x" /></button>
          </div>
        </div>
        <div className="px-3 sm:px-6 py-2 sm:py-3 border-b flex flex-wrap items-center gap-2 sm:gap-4 bg-white shadow-sm z-10">
          <button onClick={()=>setViewMode('snippets')} className={`flex items-center gap-2 text-xs sm:text-sm font-medium px-2 sm:px-3 py-1 sm:py-1.5 rounded-md ${viewMode==='snippets' ? 'bg-blue-100 text-blue-700' : 'text-gray-500'}`} disabled={mentions.length===0}><Icon name="eye" size={16} /> Search Results ({mentions.length})</button>
          <button onClick={handleFullTranscriptClick} className={`flex items-center gap-2 text-xs sm:text-sm font-medium px-2 sm:px-3 py-1 sm:py-1.5 rounded-md ${viewMode==='full' ? 'bg-blue-100 text-blue-700' : 'text-gray-500'}`}><Icon name="alignLeft" size={16} /> Full Transcript</button>
          <div className="flex items-center gap-2 sm:gap-3 sm:ml-auto">
            <label
              className={`flex items-center gap-2 text-xs sm:text-sm font-medium px-2 sm:px-3 py-1 sm:py-1.5 rounded-md select-none ${hasTimestamped && showTimestamps ? 'bg-blue-100 text-blue-700' : 'text-gray-500'} ${(checkingTimestamped || loadingTimestamped) ? 'opacity-60 cursor-wait' : hasTimestamped ? 'cursor-pointer' : 'opacity-50 cursor-not-allowed'}`}
              title={hasTimestamped ? 'Toggle timestamped transcript' : 'Timestamped transcript not available'}
            >
              <input
                type="checkbox"
                checked={hasTimestamped ? showTimestamps : false}
                disabled={!hasTimestamped || loadingTimestamped || checkingTimestamped}
                onChange={async (e) => {
                  const next = e.target.checked
                  if (next) {
                    setShowTimestamps(true)
                    await loadTimestamped()
                  } else {
                    setShowTimestamps(false)
                  }
                }}
                className="sr-only peer"
              />
              <div className={`relative w-10 h-6 rounded-full ${hasTimestamped ? 'bg-gray-200 peer-checked:bg-blue-600 peer-focus:ring-blue-200' : 'bg-gray-200'} peer-focus:outline-none peer-focus:ring-4 after:content-[''] after:absolute after:top-0.5 after:left-0.5 after:bg-white after:border after:border-gray-300 after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:after:translate-x-4`} />
              <span>
                {checkingTimestamped ? 'Checking…' : loadingTimestamped ? 'Loading…' : 'Timestamps'}
              </span>
            </label>

            {hasTimestamped ? (
              <span className="hidden sm:inline text-xs text-gray-500">
                Click a timestamp to watch the source video at that moment.
              </span>
            ) : (
              <span className="hidden sm:inline text-xs text-gray-400">
                Timestamps not available for this transcript.
              </span>
            )}
          </div>
          {relatedSermons.length > 0 && (
            <button onClick={()=>setViewMode('related')} className={`flex items-center gap-2 text-xs sm:text-sm font-medium px-2 sm:px-3 py-1 sm:py-1.5 rounded-md ${viewMode==='related' ? 'bg-blue-100 text-blue-700' : 'text-gray-500'}`}><Icon name="share" size={16} /> Related ({relatedSermons.length})</button>
          )}
        </div>
        <div
          ref={contentRef}
          className="flex-1 overflow-y-auto overflow-x-hidden p-3 sm:p-8 bg-white font-serif text-gray-800 leading-relaxed text-sm sm:text-base overscroll-none touch-pan-y"
          style={{ WebkitOverflowScrolling: 'touch' }}
        >
          {isLoading ? <p className="text-center text-gray-400 italic">Loading content...</p> : 
            viewMode==='consent' ? (
              <div className="max-w-md mx-auto mt-12">
                <div className="bg-amber-50 border border-amber-200 rounded-xl p-6">
                  <h3 className="text-lg font-bold text-amber-800 mb-4 flex items-center gap-2">
                    <Icon name="alertCircle" size={20} />
                    View Full Transcript
                  </h3>
                  <label className="flex items-start gap-3 cursor-pointer mb-3">
                    <input 
                      type="checkbox" 
                      checked={consentChecked}
                      onChange={(e) => setConsentChecked(e.target.checked)}
                      className="mt-1 w-4 h-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                    />
                    <span className="text-sm text-gray-700">
                      I am accessing this transcript for research or verification.
                    </span>
                  </label>
                  <p className="text-xs text-gray-500 mb-2 ml-7">
                    Please use the source video for the full viewing experience.
                  </p>
                  {videoUrl && (
                    <a href={videoUrl} target="_blank" rel="noopener noreferrer" className="text-xs text-red-600 hover:text-red-700 mb-5 ml-7 flex items-center gap-1 font-medium">
                      ▶ {videoUrl}
                    </a>
                  )}
                  <div className="flex gap-3 mt-4">
                    <button 
                      onClick={() => setViewMode('snippets')}
                      className="flex-1 px-4 py-2 text-sm text-gray-600 hover:bg-gray-100 rounded-lg border"
                    >
                      Back to Context
                    </button>
                    <button 
                      onClick={() => { if (consentChecked) { setHasConsented(true); setViewMode('full'); } }}
                      disabled={!consentChecked}
                      className={`flex-1 px-4 py-2 text-sm font-medium rounded-lg ${consentChecked ? 'bg-blue-600 text-white hover:bg-blue-700' : 'bg-gray-200 text-gray-400 cursor-not-allowed'}`}
                    >
                      View Full Transcript
                    </button>
                  </div>
                </div>
              </div>
            ) :
            viewMode==='snippets' ? (
              <div className="max-w-3xl mx-auto max-w-full space-y-3 sm:space-y-4">
                {mentions.map((m,i)=>(
                  <SnippetRow
                    key={i}
                    id={`match-${i}`}
                    fullText={plainText}
                    plainTextForAnchor={plainText}
                    timestampedTextForAnchor={showTimestamps ? timestampedText : ''}
                    term={m.term}
                    terms={m.terms}
                    matchIndex={m.index}
                    index={i}
                    highlightFn={highlight}
                    useLineSnippets={showTimestamps}
                  />
                ))}
              </div>
            ) : 
            viewMode==='related' && relatedSermons.length > 0 ? (
              <div className="max-w-3xl mx-auto">
                <h3 className="text-lg font-bold text-gray-800 mb-4">Related Sermons</h3>
                <p className="text-sm text-gray-500 mb-4">Based on similar topics, same venue, or date proximity</p>
                <div className="space-y-2">
                  {relatedSermons.map(s => (
                    <button
                      key={s.id}
                      onClick={() => onSelectRelated && onSelectRelated(s)}
                      className="w-full text-left p-4 bg-gray-50 hover:bg-blue-50 rounded-lg border transition flex justify-between items-center group"
                    >
                      <div className="min-w-0 flex-1">
                        <div className="font-bold text-gray-800 truncate">{s.title}</div>
                        <div className="text-sm text-gray-500 flex flex-wrap gap-2 mt-1">
                          <span>{s.date}</span>
                          {s.venue && <span>• {s.venue}</span>}
                          {s.topics && s.topics.length > 0 && (
                            <span className="text-blue-600">• {s.topics.slice(0, 2).join(', ')}</span>
                          )}
                        </div>
                      </div>
                      <Icon name="chevronRight" size={16} className="text-gray-400 group-hover:text-blue-600 flex-shrink-0 ml-4" />
                    </button>
                  ))}
                </div>
              </div>
            ) :
            <div className="max-w-3xl mx-auto max-w-full whitespace-pre-wrap break-words">{fullTranscriptContent}</div>
          }
        </div>
      </div>
    </div>
  )
}
