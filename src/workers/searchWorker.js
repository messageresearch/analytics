// Search Worker - runs text search in background thread
// Communicates with main thread via postMessage

// IndexedDB functions (duplicated here since workers can't import from main bundle)
const DB_NAME = 'sermon-search-cache'
const DB_VERSION = 2  // Bumped to force cache refresh after ID type fix (string → int)
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
  
  // Build regex
  let regex
  const termCounts = Object.create(null)
  
  if (rawRegex && String(rawRegex).trim()) {
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
    const patterns = [term, ...vars].map(t => isRegexLike(t) ? t : escapeRe(t))
    
    if (wholeWords) {
      regex = new RegExp(`\\b(${patterns.join('|')})\\b`, 'gi')
    } else {
      regex = new RegExp(`(${patterns.join('|')})`, 'gi')
    }
  }
  
  const counts = Object.create(null)
  const BATCH = 50 // Smaller batches for more frequent progress updates
  
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
      try {
        const matchList = item.text ? item.text.match(regex) : null
        if (matchList && matchList.length > 0) {
          counts[item.id] = (counts[item.id] || 0) + matchList.length
          for (const m of matchList) {
            const key = m.toLowerCase()
            termCounts[key] = (termCounts[key] || 0) + 1
          }
        }
      } catch (e) {}
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
          try {
            const matchList = item.text ? item.text.match(regex) : null
            if (matchList && matchList.length > 0) {
              counts[item.id] = (counts[item.id] || 0) + matchList.length
              for (const m of matchList) {
                const key = m.toLowerCase()
                termCounts[key] = (termCounts[key] || 0) + 1
              }
            }
          } catch (e) {}
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
