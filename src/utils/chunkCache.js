// IndexedDB cache for text chunks - enables instant repeat searches

const DB_NAME = 'sermon-search-cache'
const DB_VERSION = 2  // Bumped to fix version conflict
const STORE_NAME = 'text_chunks'
const META_STORE = 'cache_meta'

let dbPromise = null

function openDB() {
  if (dbPromise) return dbPromise
  
  dbPromise = new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION)
    
    request.onerror = (e) => {
      // If version error, delete and retry
      if (e.target.error?.name === 'VersionError') {
        console.log('IndexedDB version conflict, resetting cache...')
        dbPromise = null
        indexedDB.deleteDatabase(DB_NAME).onsuccess = () => {
          openDB().then(resolve).catch(reject)
        }
      } else {
        reject(request.error)
      }
    }
    request.onsuccess = () => resolve(request.result)
    
    request.onupgradeneeded = (event) => {
      const db = event.target.result
      
      // Store for text chunks: key = chunk index, value = array of {id, text}
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME, { keyPath: 'chunkIndex' })
      }
      
      // Store for metadata (cache version, timestamp, etc.)
      if (!db.objectStoreNames.contains(META_STORE)) {
        db.createObjectStore(META_STORE, { keyPath: 'key' })
      }
    }
  })
  
  return dbPromise
}

// Get cached chunk by index
export async function getCachedChunk(chunkIndex) {
  try {
    const db = await openDB()
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_NAME, 'readonly')
      const store = tx.objectStore(STORE_NAME)
      const request = store.get(chunkIndex)
      request.onsuccess = () => resolve(request.result?.data || null)
      request.onerror = () => resolve(null)
    })
  } catch (e) {
    console.warn('IndexedDB getCachedChunk error:', e)
    return null
  }
}

// Get multiple cached chunks at once
export async function getCachedChunks(chunkIndices) {
  try {
    const db = await openDB()
    return new Promise((resolve) => {
      const tx = db.transaction(STORE_NAME, 'readonly')
      const store = tx.objectStore(STORE_NAME)
      const results = new Map()
      let pending = chunkIndices.length
      
      if (pending === 0) {
        resolve(results)
        return
      }
      
      chunkIndices.forEach(idx => {
        const request = store.get(idx)
        request.onsuccess = () => {
          if (request.result?.data) {
            results.set(idx, request.result.data)
          }
          pending--
          if (pending === 0) resolve(results)
        }
        request.onerror = () => {
          pending--
          if (pending === 0) resolve(results)
        }
      })
    })
  } catch (e) {
    console.warn('IndexedDB getCachedChunks error:', e)
    return new Map()
  }
}

// Cache a chunk
export async function cacheChunk(chunkIndex, data) {
  try {
    const db = await openDB()
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_NAME, 'readwrite')
      const store = tx.objectStore(STORE_NAME)
      store.put({ chunkIndex, data, cachedAt: Date.now() })
      tx.oncomplete = () => resolve(true)
      tx.onerror = () => resolve(false)
    })
  } catch (e) {
    console.warn('IndexedDB cacheChunk error:', e)
    return false
  }
}

// Cache multiple chunks at once
export async function cacheChunks(chunksMap) {
  try {
    const db = await openDB()
    return new Promise((resolve) => {
      const tx = db.transaction(STORE_NAME, 'readwrite')
      const store = tx.objectStore(STORE_NAME)
      const now = Date.now()
      
      for (const [chunkIndex, data] of chunksMap.entries()) {
        store.put({ chunkIndex, data, cachedAt: now })
      }
      
      tx.oncomplete = () => resolve(true)
      tx.onerror = () => resolve(false)
    })
  } catch (e) {
    console.warn('IndexedDB cacheChunks error:', e)
    return false
  }
}

// Get cache metadata
export async function getCacheMeta(key) {
  try {
    const db = await openDB()
    return new Promise((resolve) => {
      const tx = db.transaction(META_STORE, 'readonly')
      const store = tx.objectStore(META_STORE)
      const request = store.get(key)
      request.onsuccess = () => resolve(request.result?.value || null)
      request.onerror = () => resolve(null)
    })
  } catch (e) {
    return null
  }
}

// Set cache metadata
export async function setCacheMeta(key, value) {
  try {
    const db = await openDB()
    return new Promise((resolve) => {
      const tx = db.transaction(META_STORE, 'readwrite')
      const store = tx.objectStore(META_STORE)
      store.put({ key, value, updatedAt: Date.now() })
      tx.oncomplete = () => resolve(true)
      tx.onerror = () => resolve(false)
    })
  } catch (e) {
    return false
  }
}

// Check how many chunks are cached
export async function getCacheStats() {
  try {
    const db = await openDB()
    return new Promise((resolve) => {
      const tx = db.transaction(STORE_NAME, 'readonly')
      const store = tx.objectStore(STORE_NAME)
      const countRequest = store.count()
      countRequest.onsuccess = () => resolve({ cachedChunks: countRequest.result })
      countRequest.onerror = () => resolve({ cachedChunks: 0 })
    })
  } catch (e) {
    return { cachedChunks: 0 }
  }
}

// Clear entire cache
export async function clearCache() {
  try {
    const db = await openDB()
    return new Promise((resolve) => {
      const tx = db.transaction([STORE_NAME, META_STORE], 'readwrite')
      tx.objectStore(STORE_NAME).clear()
      tx.objectStore(META_STORE).clear()
      tx.oncomplete = () => resolve(true)
      tx.onerror = () => resolve(false)
    })
  } catch (e) {
    return false
  }
}

// Check if cache is valid for given data version
export async function isCacheValid(dataVersion) {
  const cachedVersion = await getCacheMeta('dataVersion')
  return cachedVersion === dataVersion
}

// Mark cache as valid for given data version
export async function setCacheVersion(dataVersion) {
  return setCacheMeta('dataVersion', dataVersion)
}
