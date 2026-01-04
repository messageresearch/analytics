// Search Worker - runs text search in background thread
// Communicates with main thread via postMessage

// Helper: Convert wildcard pattern to regex
// * = any characters, ? = single character
function wildcardToRegex(pattern) {
  // Escape regex special chars except * and ?
  let escaped = pattern.replace(/[.+^${}()|[\]\\]/g, '\\$&')
  // Convert wildcards: * -> .* and ? -> .
  escaped = escaped.replace(/\*/g, '\\S*').replace(/\?/g, '.')
  return escaped
}

// Helper: Check if term contains wildcards
function hasWildcard(term) {
  return /[*?]/.test(term)
}

// Helper: Parse proximity/boolean search syntax
// Supports: AND, OR, NOT, NEAR/n, AROUND(n), ONEAR/n, /s (sentence), /p (paragraph), "exact phrase", wildcards, parentheses
function parseBooleanSearch(rawInput) {
  if (!rawInput || typeof rawInput !== 'string') return null
  const trimmed = rawInput.trim()
  
  // Check for ONEAR/n pattern (ordered proximity): "term1 ONEAR/5 term2"
  const onearMatch = trimmed.match(/^(.+?)\s+ONEAR\/(\d+)\s+(.+)$/i)
  if (onearMatch) {
    const term1 = onearMatch[1].trim()
    const distance = parseInt(onearMatch[2], 10)
    const term2 = onearMatch[3].trim()
    return { type: 'onear', terms: [term1, term2], distance, ordered: true, excluded: [] }
  }
  
  // Check for NEAR/n or AROUND(n) pattern: "term1 NEAR/5 term2" or "term1 ~5 term2" or "term1 AROUND(5) term2"
  const nearMatch = trimmed.match(/^(.+?)\s+(?:NEAR\/(\d+)|~(\d+)|AROUND\((\d+)\))\s+(.+)$/i)
  if (nearMatch) {
    const term1 = nearMatch[1].trim()
    const distance = parseInt(nearMatch[2] || nearMatch[3] || nearMatch[4], 10)
    const term2 = nearMatch[5].trim()
    return { type: 'near', terms: [term1, term2], distance, excluded: [] }
  }
  
  // Check for SENTENCE pattern: "term1 /s term2" or "term1 SENTENCE term2"
  const sentenceMatch = trimmed.match(/^(.+?)\s+(?:\/s|SENTENCE)\s+(.+)$/i)
  if (sentenceMatch) {
    const term1 = sentenceMatch[1].trim()
    const term2 = sentenceMatch[2].trim()
    return { type: 'sentence', terms: [term1, term2], excluded: [] }
  }
  
  // Check for PARAGRAPH pattern: "term1 /p term2" or "term1 PARAGRAPH term2"
  const paragraphMatch = trimmed.match(/^(.+?)\s+(?:\/p|PARAGRAPH)\s+(.+)$/i)
  if (paragraphMatch) {
    const term1 = paragraphMatch[1].trim()
    const term2 = paragraphMatch[2].trim()
    return { type: 'paragraph', terms: [term1, term2], excluded: [] }
  }
  
  // Check for exact phrase in quotes: "exact phrase"
  const phraseMatch = trimmed.match(/^"([^"]+)"$/)
  if (phraseMatch) {
    return { type: 'phrase', phrase: phraseMatch[1], excluded: [] }
  }
  
  // Pattern 0: Lookahead-based AND patterns like (?=.*\bterm1\b)(?=.*\bterm2\b).*
  if (trimmed.includes('(?=') && trimmed.includes('.*')) {
    const termMatches = [...trimmed.matchAll(/\(\?=\.\*(?:\\b)?(\w+)(?:\\b)?\)/g)]
    const terms = termMatches.map(m => m[1]).filter(Boolean)
    if (terms.length >= 2) return { type: 'and', required: terms, excluded: [] }
  }
  
  // Check for NOT patterns first (can be combined with AND/OR)
  let excluded = []
  let remaining = trimmed
  
  // Extract NOT terms: "term1 NOT term2 NOT term3"
  if (/\sNOT\s/i.test(remaining)) {
    const parts = remaining.split(/\s+NOT\s+/i)
    remaining = parts[0].trim()
    excluded = parts.slice(1).map(t => t.trim()).filter(Boolean)
  }
  
  // Extract -term or !term (at word boundaries)
  const negativeTerms = [...remaining.matchAll(/(?:^|\s)[-!](\w+)/g)]
  if (negativeTerms.length > 0) {
    excluded = [...excluded, ...negativeTerms.map(m => m[1])]
    remaining = remaining.replace(/(?:^|\s)[-!]\w+/g, ' ').trim()
  }
  
  // Check for OR patterns: "term1 OR term2", "term1 | term2"
  if (/\sOR\s/i.test(remaining)) {
    const terms = remaining.split(/\s+OR\s+/i).map(t => t.trim()).filter(Boolean)
    if (terms.length >= 2 || excluded.length > 0) {
      return { type: 'or', required: terms, excluded }
    }
  }
  
  if (/\s*\|\s*/.test(remaining)) {
    const terms = remaining.split(/\s*\|\s*/).map(t => t.trim()).filter(Boolean)
    if (terms.length >= 2 || excluded.length > 0) {
      return { type: 'or', required: terms, excluded }
    }
  }
  
  // Check for AND patterns: "term1 AND term2", "+term1 +term2", "term1 & term2"
  if (/\sAND\s/i.test(remaining)) {
    const terms = remaining.split(/\s+AND\s+/i).map(t => t.trim()).filter(Boolean)
    if (terms.length >= 2 || excluded.length > 0) {
      return { type: 'and', required: terms, excluded }
    }
  }
  
  // "+term1 +term2" (all terms must have +)
  if (/^\+\S/.test(remaining) && remaining.includes(' +')) {
    const terms = remaining.split(/\s+/).filter(t => t.startsWith('+')).map(t => t.slice(1).trim()).filter(Boolean)
    if (terms.length >= 2 || excluded.length > 0) {
      return { type: 'and', required: terms, excluded }
    }
  }
  
  // "term1 & term2"
  if (/\s*&\s*/.test(remaining) && !remaining.includes('|')) {
    const terms = remaining.split(/\s*&\s*/).map(t => t.trim()).filter(Boolean)
    if (terms.length >= 2 || excluded.length > 0) {
      return { type: 'and', required: terms, excluded }
    }
  }
  
  // If we only have excluded terms with a single search term
  if (excluded.length > 0 && remaining.trim()) {
    return { type: 'and', required: [remaining.trim()], excluded }
  }
  
  return null
}

// Helper: Build regex pattern for a term (with wildcard support)
function buildTermPattern(term, wholeWords = true) {
  if (hasWildcard(term)) {
    const pattern = wildcardToRegex(term)
    return wholeWords ? `\\b${pattern}\\b` : pattern
  }
  const escapeRe = (s) => String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  return wholeWords ? `\\b${escapeRe(term)}\\b` : escapeRe(term)
}

// Helper: Find proximity matches (NEAR/n or ONEAR/n for ordered)
function findNearMatches(text, term1, term2, maxDistance, wholeWords = true, ordered = false) {
  const pattern1 = buildTermPattern(term1, wholeWords)
  const pattern2 = buildTermPattern(term2, wholeWords)
  const regex1 = new RegExp(pattern1, 'gi')
  const regex2 = new RegExp(pattern2, 'gi')
  
  // Find all positions of both terms
  const positions1 = []
  const positions2 = []
  let match
  while ((match = regex1.exec(text)) !== null) positions1.push({ index: match.index, term: match[0] })
  while ((match = regex2.exec(text)) !== null) positions2.push({ index: match.index, term: match[0] })
  
  if (positions1.length === 0 || positions2.length === 0) return []
  
  // Find pairs within distance (count words between)
  const matches = []
  const wordPattern = /\S+/g
  const words = []
  while ((match = wordPattern.exec(text)) !== null) words.push({ start: match.index, end: match.index + match[0].length })
  
  for (const p1 of positions1) {
    for (const p2 of positions2) {
      // For ordered search, term1 must come before term2
      if (ordered && p1.index >= p2.index) continue
      
      // Count words between the two positions
      const start = Math.min(p1.index, p2.index)
      const end = Math.max(p1.index, p2.index)
      const wordsBetween = words.filter(w => w.start > start && w.end < end).length
      
      if (wordsBetween <= maxDistance) {
        matches.push({ index: Math.min(p1.index, p2.index), term: p1.term, term2: p2.term, distance: wordsBetween })
      }
    }
  }
  
  // Deduplicate by position
  const seen = new Set()
  return matches.filter(m => {
    const key = m.index
    if (seen.has(key)) return false
    seen.add(key)
    return true
  })
}

// Helper: Find sentence matches
function findSentenceMatches(text, term1, term2, wholeWords = true) {
  // Split into sentences (rough approximation)
  const sentences = text.split(/[.!?]+\s+/)
  const pattern1 = buildTermPattern(term1, wholeWords)
  const pattern2 = buildTermPattern(term2, wholeWords)
  const regex1 = new RegExp(pattern1, 'gi')
  const regex2 = new RegExp(pattern2, 'gi')
  
  const matches = []
  let offset = 0
  
  for (const sentence of sentences) {
    regex1.lastIndex = 0
    regex2.lastIndex = 0
    const has1 = regex1.test(sentence)
    regex1.lastIndex = 0
    regex2.lastIndex = 0
    const has2 = regex2.test(sentence)
    
    if (has1 && has2) {
      // Find first match of either term for position
      regex1.lastIndex = 0
      const m1 = regex1.exec(sentence)
      if (m1) matches.push({ index: offset + m1.index, term: m1[0] })
    }
    offset += sentence.length + 2 // account for sentence delimiter
  }
  
  return matches
}

// Helper: Find paragraph matches
function findParagraphMatches(text, term1, term2, wholeWords = true) {
  // Split into paragraphs (double newline or significant whitespace)
  const paragraphs = text.split(/\n\s*\n|\r\n\s*\r\n/)
  const pattern1 = buildTermPattern(term1, wholeWords)
  const pattern2 = buildTermPattern(term2, wholeWords)
  const regex1 = new RegExp(pattern1, 'gi')
  const regex2 = new RegExp(pattern2, 'gi')
  
  const matches = []
  let offset = 0
  
  for (const para of paragraphs) {
    regex1.lastIndex = 0
    regex2.lastIndex = 0
    const has1 = regex1.test(para)
    regex1.lastIndex = 0
    regex2.lastIndex = 0
    const has2 = regex2.test(para)
    
    if (has1 && has2) {
      regex1.lastIndex = 0
      const m1 = regex1.exec(para)
      if (m1) matches.push({ index: offset + m1.index, term: m1[0] })
    }
    offset += para.length + 2
  }
  
  return matches
}

// Helper: Build regex for a single term (with word boundary and wildcard support)
function buildTermRegex(term, wholeWords = true) {
  // Check for wildcards first
  if (hasWildcard(term)) {
    const pattern = wildcardToRegex(term)
    return wholeWords ? new RegExp(`\\b(${pattern})\\b`, 'gi') : new RegExp(`(${pattern})`, 'gi')
  }
  
  const escapeRe = (s) => String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  const isRegexLike = (s) => /[\\\(\)\[\]\|\^\$\.\*\+\?]/.test(s)
  const pattern = isRegexLike(term) ? term : escapeRe(term)
  
  if (wholeWords) {
    return new RegExp(`\\b(${pattern})\\b`, 'gi')
  } else {
    return new RegExp(`(${pattern})`, 'gi')
  }
}

// IndexedDB functions (duplicated here since workers can't import from main bundle)
const DB_NAME = 'sermon-search-cache'
const DB_VERSION = 1
const STORE_NAME = 'text_chunks'

function openDB() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION)
    request.onerror = () => reject(request.error)
    request.onsuccess = () => resolve(request.result)
    request.onupgradeneeded = (event) => {
      const db = event.target.result
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME, { keyPath: 'chunkIndex' })
      }
      if (!db.objectStoreNames.contains('cache_meta')) {
        db.createObjectStore('cache_meta', { keyPath: 'key' })
      }
    }
  })
}

async function getCachedChunks(indices) {
  try {
    const db = await openDB()
    return new Promise((resolve) => {
      const tx = db.transaction(STORE_NAME, 'readonly')
      const store = tx.objectStore(STORE_NAME)
      const results = new Map()
      let pending = indices.length
      if (pending === 0) { resolve(results); return }
      
      indices.forEach(idx => {
        const req = store.get(idx)
        req.onsuccess = () => {
          if (req.result?.data) results.set(idx, req.result.data)
          if (--pending === 0) resolve(results)
        }
        req.onerror = () => { if (--pending === 0) resolve(results) }
      })
    })
  } catch (e) { return new Map() }
}

async function cacheChunks(chunksMap) {
  try {
    const db = await openDB()
    return new Promise((resolve) => {
      const tx = db.transaction(STORE_NAME, 'readwrite')
      const store = tx.objectStore(STORE_NAME)
      const now = Date.now()
      for (const [idx, data] of chunksMap.entries()) {
        store.put({ chunkIndex: idx, data, cachedAt: now })
      }
      tx.oncomplete = () => resolve(true)
      tx.onerror = () => resolve(false)
    })
  } catch (e) { return false }
}

// Main search function
async function runSearch(params) {
  const { term, variations, rawRegex, options, totalChunks, apiPrefix } = params
  const { wholeWords = true } = options || {}
  
  // Combine all inputs for boolean search detection
  // Check primary term first, then rawRegex
  const searchInput = (term && parseBooleanSearch(term)) ? term : (rawRegex || term || '')
  
  // Check for boolean/proximity search syntax
  const booleanSearch = parseBooleanSearch(searchInput)
  
  // Build regex (or boolean search regexes)
  let regex
  let requiredRegexes = null
  let excludedRegexes = null
  let searchType = null
  let proximityParams = null // For NEAR, SENTENCE, PARAGRAPH
  const termCounts = Object.create(null)
  
  if (booleanSearch) {
    searchType = booleanSearch.type
    try {
      // Handle proximity searches
      if (searchType === 'near' || searchType === 'onear' || searchType === 'sentence' || searchType === 'paragraph') {
        proximityParams = booleanSearch
        // Build regex for highlighting both terms (with wildcard support)
        const terms = booleanSearch.terms || []
        const patterns = terms.map(t => {
          if (hasWildcard(t)) return wildcardToRegex(t)
          return String(t).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
        })
        const combinedPattern = patterns.join('|')
        regex = wholeWords 
          ? new RegExp(`\\b(${combinedPattern})\\b`, 'gi')
          : new RegExp(`(${combinedPattern})`, 'gi')
        
        let msg = ''
        if (searchType === 'near') {
          msg = `NEAR/${booleanSearch.distance} search: "${terms[0]}" within ${booleanSearch.distance} words of "${terms[1]}"`
        } else if (searchType === 'onear') {
          msg = `ONEAR/${booleanSearch.distance} search: "${terms[0]}" within ${booleanSearch.distance} words BEFORE "${terms[1]}" (ordered)`
        } else if (searchType === 'sentence') {
          msg = `SENTENCE search: "${terms[0]}" and "${terms[1]}" in same sentence`
        } else if (searchType === 'paragraph') {
          msg = `PARAGRAPH search: "${terms[0]}" and "${terms[1]}" in same paragraph`
        }
        
        self.postMessage({ type: 'progress', message: msg + '...', percent: 0, proximityTerms: terms })
      }
      // Handle phrase search
      else if (searchType === 'phrase') {
        const phrase = booleanSearch.phrase
        const escapeRe = (s) => String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
        regex = new RegExp(`(${escapeRe(phrase)})`, 'gi')
        self.postMessage({ type: 'progress', message: `Exact phrase: "${phrase}"...`, percent: 0 })
      }
      // Handle AND/OR/NOT
      else {
        if (booleanSearch.required && booleanSearch.required.length > 0) {
          requiredRegexes = booleanSearch.required.map(t => buildTermRegex(t, wholeWords))
        }
        
        if (booleanSearch.excluded && booleanSearch.excluded.length > 0) {
          excludedRegexes = booleanSearch.excluded.map(t => buildTermRegex(t, wholeWords))
        }
        
        // Build combined regex for counting
        const requiredTerms = booleanSearch.required || []
        if (requiredTerms.length > 0) {
          const combinedPattern = requiredTerms.map(t => {
            const isRegexLike = (s) => /[\\\(\)\[\]\|\^\$\.\*\+\?]/.test(s)
            const escapeRe = (s) => String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
            return isRegexLike(t) ? t : escapeRe(t)
          }).join('|')
          
          regex = wholeWords 
            ? new RegExp(`\\b(${combinedPattern})\\b`, 'gi')
            : new RegExp(`(${combinedPattern})`, 'gi')
        }
        
        let msg = ''
        if (searchType === 'and') {
          msg = `AND search: finding texts with ALL ${requiredTerms.length} terms`
        } else if (searchType === 'or') {
          msg = `OR search: finding texts with ANY of ${requiredTerms.length} terms`
        }
        if (booleanSearch.excluded && booleanSearch.excluded.length > 0) {
          msg += ` (excluding ${booleanSearch.excluded.length} terms)`
        }
        
        self.postMessage({ 
          type: 'progress', 
          message: msg + '...',
          percent: 0,
          booleanTerms: { required: booleanSearch.required, excluded: booleanSearch.excluded, type: searchType }
        })
      }
    } catch (e) {
      self.postMessage({ type: 'error', message: 'Invalid search terms: ' + e.message })
      return
    }
  } else if (rawRegex && String(rawRegex).trim()) {
    try {
      regex = new RegExp(rawRegex, 'gi')
    } catch (e) {
      self.postMessage({ type: 'error', message: 'Invalid regex: ' + e.message })
      return
    }
  } else {
    const vars = Array.isArray(variations) 
      ? variations.map(v => String(v).trim()).filter(Boolean) 
      : String(variations || '').split(',').map(v => v.trim()).filter(Boolean)
    const isRegexLike = (s) => /[\\\(\)\[\]\|\^\$\.\*\+\?]/.test(s)
    const escapeRe = (s) => String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    const patterns = [term, ...vars].filter(Boolean).map(t => isRegexLike(t) ? t : escapeRe(t))
    
    if (patterns.length > 0) {
      if (wholeWords) {
        regex = new RegExp(`\\b(${patterns.join('|')})\\b`, 'gi')
      } else {
        regex = new RegExp(`(${patterns.join('|')})`, 'gi')
      }
    }
  }
  
  const counts = Object.create(null)
  const BATCH = 50
  
  // Helper function to check conditions and count matches
  const processItem = (item) => {
    if (!item.text) return
    
    // Check excluded terms first (NOT)
    if (excludedRegexes) {
      const hasExcluded = excludedRegexes.some(r => {
        r.lastIndex = 0
        return r.test(item.text)
      })
      if (hasExcluded) return
    }
    
    // Handle proximity searches
    if (proximityParams) {
      const terms = proximityParams.terms || []
      let matches = []
      
      if (searchType === 'near') {
        matches = findNearMatches(item.text, terms[0], terms[1], proximityParams.distance, wholeWords, false)
      } else if (searchType === 'onear') {
        matches = findNearMatches(item.text, terms[0], terms[1], proximityParams.distance, wholeWords, true)
      } else if (searchType === 'sentence') {
        matches = findSentenceMatches(item.text, terms[0], terms[1], wholeWords)
      } else if (searchType === 'paragraph') {
        matches = findParagraphMatches(item.text, terms[0], terms[1], wholeWords)
      
      if (matches.length > 0) {
        counts[item.id] = (counts[item.id] || 0) + matches.length
        for (const m of matches) {
          const key = m.term.toLowerCase()
          termCounts[key] = (termCounts[key] || 0) + 1
        }
      }
      return
    }
    
    // Check required terms based on search type
    if (requiredRegexes && requiredRegexes.length > 0) {
      if (searchType === 'and') {
        const allPresent = requiredRegexes.every(r => {
          r.lastIndex = 0
          return r.test(item.text)
        })
        if (!allPresent) return
      } else if (searchType === 'or') {
        const anyPresent = requiredRegexes.some(r => {
          r.lastIndex = 0
          return r.test(item.text)
        })
        if (!anyPresent) return
      }
    }
    
    // Count matches using the combined regex
    if (!regex) return
    try {
      const matchList = item.text.match(regex)
      if (matchList && matchList.length > 0) {
        counts[item.id] = (counts[item.id] || 0) + matchList.length
        for (const m of matchList) {
          const key = m.toLowerCase()
          termCounts[key] = (termCounts[key] || 0) + 1
        }
      }
    } catch (e) {}
  }
  
  // Check which chunks are cached
  const allIndices = Array.from({ length: totalChunks }, (_, i) => i)
  
  self.postMessage({ type: 'progress', message: 'Checking cache...', percent: 0 })
  
  const cachedData = await getCachedChunks(allIndices)
  const cachedCount = cachedData.size
  const uncachedIndices = allIndices.filter(i => !cachedData.has(i))
  
  self.postMessage({ 
    type: 'progress', 
    message: `Found ${cachedCount}/${totalChunks} chunks in cache`,
    percent: 0,
    cached: cachedCount,
    total: totalChunks
  })
  
  // Process cached chunks first (fast!)
  let processedCount = 0
  for (const [idx, chunk] of cachedData.entries()) {
    for (const item of chunk) {
      processItem(item)
    }
    processedCount++
    if (processedCount % 20 === 0) {
      self.postMessage({ 
        type: 'progress', 
        message: `Searching cached: ${processedCount}/${cachedCount}`,
        percent: Math.round((processedCount / totalChunks) * 100)
      })
    }
  }
  
  // Fetch and process uncached chunks
  if (uncachedIndices.length > 0) {
    self.postMessage({ 
      type: 'progress', 
      message: `Downloading ${uncachedIndices.length} uncached chunks...`,
      percent: Math.round((cachedCount / totalChunks) * 100)
    })
    
    const newlyCached = new Map()
    
    for (let i = 0; i < uncachedIndices.length; i += BATCH) {
      const batchIndices = uncachedIndices.slice(i, i + BATCH)
      
      // Fetch batch in parallel
      const fetchPromises = batchIndices.map(idx =>
        fetch(`${apiPrefix}text_chunk_${idx}.json`)
          .then(r => r.ok ? r.json() : [])
          .then(data => ({ idx, data }))
          .catch(() => ({ idx, data: [] }))
      )
      
      const results = await Promise.all(fetchPromises)
      
      // Process and cache results
      for (const { idx, data } of results) {
        if (data.length > 0) {
          newlyCached.set(idx, data)
        }
        
        for (const item of data) {
          processItem(item)
        }
        processedCount++
      }
      
      const percent = Math.round((processedCount / totalChunks) * 100)
      self.postMessage({ 
        type: 'progress', 
        message: `Scanning: ${percent}% (${processedCount}/${totalChunks})`,
        percent
      })
    }
    
    // Cache newly fetched chunks in background
    if (newlyCached.size > 0) {
      cacheChunks(newlyCached).then(() => {
        self.postMessage({ type: 'cached', count: newlyCached.size })
      })
    }
  }
  
  // Build results
  const sortedTerms = Object.entries(termCounts)
    .map(([t, c]) => ({ term: t, count: c }))
    .sort((a, b) => b.count - a.count)
  
  self.postMessage({
    type: 'complete',
    counts: Object.entries(counts),
    matchedTerms: sortedTerms,
    regexSource: regex.source
  })
}

// Handle messages from main thread
self.onmessage = async (event) => {
  const { type, ...params } = event.data
  
  if (type === 'search') {
    await runSearch(params)
  } else if (type === 'clearCache') {
    try {
      const db = await openDB()
      const tx = db.transaction([STORE_NAME, 'cache_meta'], 'readwrite')
      tx.objectStore(STORE_NAME).clear()
      tx.objectStore('cache_meta').clear()
      tx.oncomplete = () => self.postMessage({ type: 'cacheCleared' })
    } catch (e) {
      self.postMessage({ type: 'cacheCleared' })
    }
  }
}
