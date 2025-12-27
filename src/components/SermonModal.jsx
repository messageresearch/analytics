import React, { useState, useEffect } from 'react'
import Icon from './Icon'
import { DEFAULT_REGEX_STR } from '../constants'

const SnippetRow = ({ id, fullText, term, matchIndex, index, highlightFn }) => {
  const [expanded, setExpanded] = useState(false)
  const context = expanded ? 600 : 200
  const start = Math.max(0, matchIndex - context)
  const end = Math.min(fullText.length, matchIndex + (term?.length||0) + context)
  const snippetText = (start>0? '...' : '') + fullText.substring(start,end) + (end<fullText.length ? '...' : '')
  return (
    <div id={id} onClick={()=>setExpanded(!expanded)} className="bg-gray-50 p-4 rounded-lg border-l-4 border-yellow-400 shadow-sm transition-all duration-300 cursor-pointer hover:bg-blue-50 group">
      <div className="flex justify-between items-start mb-2"><p className="text-gray-400 text-xs font-sans font-bold uppercase tracking-wide group-hover:text-blue-500">Match #{index+1}</p><button className="text-xs text-blue-600 font-medium flex items-center gap-1 opacity-60 group-hover:opacity-100">{expanded ? 'Collapse' : <><Icon name="maximize" size={12} /> Expand Context</>}</button></div>
      <p className="text-gray-800 leading-relaxed font-serif text-sm">{highlightFn(snippetText, term)}</p>
    </div>
  )
}

export default function SermonModal({ sermon, onClose, focusMatchIndex = 0 }){
  const [fullText, setFullText] = useState('')
  const [isLoading, setIsLoading] = useState(true)
  const [viewMode, setViewMode] = useState('snippets')
  const [mentions, setMentions] = useState([])

  useEffect(()=>{
    setIsLoading(true)
    fetch(sermon.path).then(res=>res.ok?res.text():'Transcript not available.').then(text=>{
      setFullText(text); setIsLoading(false)
      const regex = sermon.searchTerm ? new RegExp(`(${sermon.searchTerm})`, 'gi') : new RegExp(DEFAULT_REGEX_STR, 'gi')
      const found = []; let match; while((match = regex.exec(text)) !== null) { found.push({ index: match.index, term: match[0] }) }
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

  const escapeRegExp = (s) => s ? s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') : ''
  const highlight = (text, term) => {
    if(!text) return ''
    const t = term || DEFAULT_REGEX_STR
    let splitter
    try {
      splitter = new RegExp(`(${t})`, 'i')
    } catch (e) {
      splitter = new RegExp(`(${escapeRegExp(t)})`, 'i')
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
          </div>
          <div className="flex gap-2"><button onClick={downloadText} className="p-2 bg-blue-50 text-blue-600 hover:bg-blue-100 rounded-lg"><Icon name="download" /></button><button onClick={onClose} className="p-2 bg-red-50 text-red-600 hover:bg-red-100 rounded-lg"><Icon name="x" /></button></div>
        </div>
        <div className="px-6 py-3 border-b flex gap-4 bg-white shadow-sm z-10">
          <button onClick={()=>setViewMode('snippets')} className={`flex items-center gap-2 text-sm font-medium px-3 py-1.5 rounded-md ${viewMode==='snippets' ? 'bg-blue-100 text-blue-700' : 'text-gray-500'}`} disabled={mentions.length===0}><Icon name="eye" size={16} /> Context ({mentions.length})</button>
          <button onClick={()=>setViewMode('full')} className={`flex items-center gap-2 text-sm font-medium px-3 py-1.5 rounded-md ${viewMode==='full' ? 'bg-blue-100 text-blue-700' : 'text-gray-500'}`}><Icon name="alignLeft" size={16} /> Full Transcript</button>
        </div>
        <div className="flex-1 overflow-y-auto p-8 bg-white font-serif text-gray-800 leading-relaxed text-base">
          {isLoading ? <p className="text-center text-gray-400 italic">Loading content...</p> : viewMode==='snippets' ? (<div className="max-w-3xl mx-auto space-y-4">{mentions.map((m,i)=><SnippetRow key={i} id={`match-${i}`} fullText={fullText} term={m.term} matchIndex={m.index} index={i} highlightFn={highlight} />)}</div>) : <div className="max-w-3xl mx-auto whitespace-pre-wrap">{highlight(fullText, sermon.searchTerm)}</div>}
        </div>
      </div>
    </div>
  )
}
