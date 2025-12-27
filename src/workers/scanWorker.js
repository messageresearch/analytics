// Web Worker for scanning text chunks for term matches with IndexedDB caching.
let cancelled = false;

// Simple IndexedDB wrapper for caching chunk JSON
function openDB(){
  return new Promise((resolve, reject)=>{
    const req = indexedDB.open('wmbmentions-chunks', 1)
    req.onupgradeneeded = () => { const db = req.result; if(!db.objectStoreNames.contains('chunks')) db.createObjectStore('chunks', { keyPath: 'key' }) }
    req.onsuccess = () => resolve(req.result)
    req.onerror = () => reject(req.error)
  })
}

async function getChunkFromCache(db, key){
  return new Promise((resolve)=>{
    try{
      const tx = db.transaction('chunks','readonly'); const store = tx.objectStore('chunks'); const r = store.get(key)
      r.onsuccess = ()=> resolve(r.result ? r.result.data : null)
      r.onerror = ()=> resolve(null)
    }catch(e){ resolve(null) }
  })
}

async function putChunkInCache(db, key, data){
  return new Promise((resolve)=>{
    try{
      const tx = db.transaction('chunks','readwrite'); const store = tx.objectStore('chunks'); const r = store.put({ key, data })
      r.onsuccess = ()=> resolve(true)
      r.onerror = ()=> resolve(false)
    }catch(e){ resolve(false) }
  })
}

self.addEventListener('message', async (ev) => {
  const msg = ev.data || {}
  try{
    self.postMessage({ type: 'log', message: 'worker-received-message', data: msg })
  }catch(e){}
  if(msg.type === 'ping'){
    try{ self.postMessage({ type: 'pong' }) }catch(e){}
    return
  }
  if (msg.type === 'cancel') { cancelled = true; return }
  if (msg.type !== 'analyze') return
  cancelled = false
  const { term, variations, apiPrefix = 'site_api/', totalChunks = 0, chunkBatch = 25 } = msg
  try {
    self.postMessage({ type: 'started', totalChunks, chunkBatch })
    const db = await openDB().catch((e)=>{ self.postMessage({ type: 'log', message: 'indexeddb-open-failed', error: String(e) }); return null })
    const vars = Array.isArray(variations) ? variations.map(v=>(''+v).trim()).filter(Boolean) : (''+ (variations || '')).split(',').map(v=>v.trim()).filter(Boolean)
    const allTerms = [term, ...vars].map(t=>t.replace(/[.*+?^${}()|[\\]\\]/g,'\\\\$&'))
    const regex = new RegExp(`(${allTerms.join('|')})`, 'gi')
    const counts = Object.create(null)
    let processedChunks = 0
    for (let i = 0; i < totalChunks; i += chunkBatch) {
      if (cancelled) break
      const promises = []
      for (let j = i; j < Math.min(i + chunkBatch, totalChunks); j++) {
        const key = `${apiPrefix}|${j}`
        if (db) {
          promises.push(getChunkFromCache(db, key).then(cached => cached || fetch(`${apiPrefix}text_chunk_${j}.json`).then(r => r.ok ? r.json() : []).then(data => { if(db && data) putChunkInCache(db, key, data); return data }).catch(()=>[])))
        } else {
          promises.push(fetch(`${apiPrefix}text_chunk_${j}.json`).then(r => r.ok ? r.json() : []).catch(()=>[]))
        }
      }
      let results = []
      try{
        results = await Promise.all(promises)
      }catch(e){
        self.postMessage({ type: 'log', message: 'chunk-fetch-batch-failed', error: String(e), batchStart: i })
        results = []
      }
      for (const chunk of results) {
        if (cancelled) break
        for (const item of chunk) {
          try {
            const matches = (item.text && item.text.match(regex)) ? item.text.match(regex).length : 0
            if (matches > 0) counts[item.id] = (counts[item.id] || 0) + matches
          } catch(e) { /* ignore bad items */ }
        }
      }
      processedChunks += results.length
      self.postMessage({ type: 'progress', processed: processedChunks, total: totalChunks, percent: Math.round((processedChunks/totalChunks)*100) })
    }
    if (cancelled) {
      self.postMessage({ type: 'cancelled' })
      return
    }
    // Send back as array of entries to avoid Map/complex transfer issues
    const entries = Object.entries(counts)
    self.postMessage({ type: 'result', counts: entries })
  } catch (err) {
    self.postMessage({ type: 'error', message: String(err) })
  }
})
