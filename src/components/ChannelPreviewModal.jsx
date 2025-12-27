import React, { useEffect, useState } from 'react'
import Icon from './Icon'

export default function ChannelPreviewModal({ channel, apiPrefix = 'site_api/', onClose }){
  const [content, setContent] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  useEffect(()=>{
    if(!channel) return
    const fetchContent = async ()=>{
      setLoading(true); setError(null)
      const tries = []
      if(channel.filename) tries.push(apiPrefix + channel.filename)
      if(channel.filename) tries.push('/' + channel.filename)
      if(channel.filename) tries.push(channel.filename)
      // last-resort: try fetching the channel URL (won't return transcript, but we attempt anyway)
      if(channel.url) tries.push(channel.url)
      let got = null
      const looksLikeHtml = (t) => {
        if(!t || typeof t !== 'string') return false
        const s = t.trim().toLowerCase()
        if(s.startsWith('<!doctype') || s.startsWith('<html')) return true
        if(s.includes('<script type="module" src="/@vite/client"') || s.includes('injectintoglobalhook')) return true
        return false
      }

      for(const p of tries){
        try{
          const r = await fetch(p)
          if(r && r.ok){
            const text = await r.text()
            // If the server returned the SPA index (HTML) because the file wasn't present,
            // treat that as a miss and continue trying other paths instead of showing HTML.
            if(looksLikeHtml(text)){
              // continue to next candidate
              continue
            }
            got = { path: p, text }
            break
          }
        }catch(e){}
      }
      if(got){ setContent(got); setLoading(false) }
      else {
        // Final attempt: fetch first candidate but show a clearer message if it's HTML
        if(tries.length){
          try{
            const r = await fetch(tries[0])
            if(r && r.ok){
              const text = await r.text()
              if(looksLikeHtml(text)) setError('Preview not available (server returned HTML index)')
              else setContent({ path: tries[0], text })
            } else setError('Preview not available')
          }catch(e){ setError('Preview not available') }
        } else {
          setError('Preview not available')
        }
        setLoading(false)
      }
    }
    fetchContent()
  }, [channel])

  if(!channel) return null
  return (
    <div className="fixed inset-0 z-60 flex items-center justify-center bg-black/60 p-4" onClick={onClose}>
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-3xl max-h-[80vh] overflow-hidden" onClick={e=>e.stopPropagation()}>
        <div className="p-4 border-b flex justify-between items-center">
          <div>
            <h3 className="font-bold text-lg">{channel.name}</h3>
            <div className="text-xs text-blue-600 truncate"><a href={channel.url} target="_blank" rel="noopener noreferrer">{channel.url}</a></div>
            {channel.filename && <div className="text-xs text-gray-500">File: {channel.filename}</div>}
          </div>
          <div className="flex items-center gap-2">
            <button onClick={onClose} className="p-2 rounded bg-gray-100 hover:bg-gray-200"><Icon name="x" /></button>
          </div>
        </div>
        <div className="p-4 overflow-auto max-h-[60vh]">
          {loading && <div className="text-sm text-gray-500">Loading preview…</div>}
          {error && <div className="text-sm text-red-500">{error}</div>}
          {content && (
            <div>
              <div className="text-xs text-gray-400 mb-2">Source: {content.path}</div>
              <pre className="whitespace-pre-wrap text-sm font-mono text-gray-800">{content.text.slice(0, 20000)}</pre>
              {content.text.length > 20000 && <div className="text-xs text-gray-500 mt-2">(truncated)</div>}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
