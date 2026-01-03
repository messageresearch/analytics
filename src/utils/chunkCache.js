// IndexedDB-based cache for sermon transcript chunks
// Provides faster searches on subsequent visits by storing downloaded chunks locally

const DB_NAME = 'wmbmentions-chunk-cache'
const DB_VERSION = 1
const STORE_NAME = 'chunks'
const META_STORE = 'meta'

// Open or create the IndexedDB database
function openDB() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION)
    
    request.onerror = () => reject(request.error)
    request.onsuccess = () => resolve(request.result)
    
    request.onupgradeneeded = (event) => {
      const db = event.target.result
      
      // Store for cached chunks, keyed by chunk index
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME, { keyPath: 'idx' })
      }
      
      // Store for metadata (version tracking)
      if (!db.objectStoreNames.contains(META_STORE)) {
        db.createObjectStore(META_STORE, { keyPath: 'key' })
      }
    }
  })
}

// Check if the cache is valid for the current data version
export async function isCacheValid(dataVersion) {
  try {
    const db = await openDB()
    return new Promise((resolve) => {
      const tx = db.transaction(META_STORE, 'readonly')
      const store = tx.objectStore(META_STORE)
      const request = store.get('version')
      
      request.onsuccess = () => {
        const result = request.result
        resolve(result && result.value === dataVersion)
      }
      request.onerror = () => resolve(false)
    })
  } catch (e) {
    console.warn('Cache check failed:', e)
    return false
  }
}

// Set the cache version
export async function setCacheVersion(dataVersion) {
  try {
    const db = await openDB()
    return new Promise((resolve, reject) => {
      const tx = db.transaction(META_STORE, 'readwrite')
      const store = tx.objectStore(META_STORE)
      const request = store.put({ key: 'version', value: dataVersion })
      
      request.onsuccess = () => resolve()
      request.onerror = () => reject(request.error)
    })
  } catch (e) {
    console.warn('Set cache version failed:', e)
  }
}

// Clear all cached chunks
export async function clearCache() {
  try {
    const db = await openDB()
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_NAME, 'readwrite')
      const store = tx.objectStore(STORE_NAME)
      const request = store.clear()
      
      request.onsuccess = () => resolve()
      request.onerror = () => reject(request.error)
    })
  } catch (e) {
    console.warn('Clear cache failed:', e)
  }
}

// Get cached chunks by their indices
// Returns a Map of index -> chunk data
export async function getCachedChunks(indices) {
  const result = new Map()
  
  try {
    const db = await openDB()
    
    return new Promise((resolve) => {
      const tx = db.transaction(STORE_NAME, 'readonly')
      const store = tx.objectStore(STORE_NAME)
      
      let pending = indices.length
      if (pending === 0) {
        resolve(result)
        return
      }
      
      for (const idx of indices) {
        const request = store.get(idx)
        request.onsuccess = () => {
          if (request.result && request.result.data) {
            result.set(idx, request.result.data)
          }
          pending--
          if (pending === 0) resolve(result)
        }
        request.onerror = () => {
          pending--
          if (pending === 0) resolve(result)
        }
      }
    })
  } catch (e) {
    console.warn('Get cached chunks failed:', e)
    return result
  }
}

// Cache multiple chunks at once
// Takes a Map of index -> chunk data
export async function cacheChunks(chunksMap) {
  try {
    const db = await openDB()
    
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_NAME, 'readwrite')
      const store = tx.objectStore(STORE_NAME)
      
      for (const [idx, data] of chunksMap.entries()) {
        store.put({ idx, data })
      }
      
      tx.oncomplete = () => resolve()
      tx.onerror = () => reject(tx.error)
    })
  } catch (e) {
    console.warn('Cache chunks failed:', e)
  }
}

// Get cache statistics
export async function getCacheStats() {
  try {
    const db = await openDB()
    
    return new Promise((resolve) => {
      const tx = db.transaction([STORE_NAME, META_STORE], 'readonly')
      const chunkStore = tx.objectStore(STORE_NAME)
      const metaStore = tx.objectStore(META_STORE)
      
      const stats = { count: 0, version: null }
      
      const countRequest = chunkStore.count()
      countRequest.onsuccess = () => {
        stats.count = countRequest.result
      }
      
      const versionRequest = metaStore.get('version')
      versionRequest.onsuccess = () => {
        if (versionRequest.result) {
          stats.version = versionRequest.result.value
        }
      }
      
      tx.oncomplete = () => resolve(stats)
      tx.onerror = () => resolve(stats)
    })
  } catch (e) {
    console.warn('Get cache stats failed:', e)
    return { count: 0, version: null }
  }
}
