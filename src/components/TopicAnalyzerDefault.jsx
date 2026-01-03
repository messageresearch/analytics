import React, { useState, useEffect, useRef } from 'react'
import { DEFAULT_TERM, DEFAULT_REGEX_STR } from '../constants_local'
import Icon from './Icon'
import { expandRegex } from '../utils/regexExpander'

export default function TopicAnalyzerDefault({ onAnalyze, isAnalyzing, progress, initialTerm = '', initialVariations = '', matchedTerms = [], cacheStatus = null, totalTranscripts = 0 }) {
  const [term, setTerm] = useState(initialTerm || '')
  const isRegexLike = (s) => /[\\\(\)\[\]\|\^\$\.\*\+\?]/.test(s)
  const [variations, setVariations] = useState(!isRegexLike(initialVariations) ? initialVariations : '')
  const [showRegex, setShowRegex] = useState(isRegexLike(initialVariations))
  const [rawRegex, setRawRegex] = useState(isRegexLike(initialVariations) ? initialVariations : '')
  const [regexError, setRegexError] = useState(null)
  const [wholeWords, setWholeWords] = useState(true) // Default to exact word matching
  const [showPreview, setShowPreview] = useState(false)
  const [previewData, setPreviewData] = useState({ matches: [], truncated: false, error: null })
  const [showAllTerms, setShowAllTerms] = useState(false)
  
  // Format transcript count (e.g., 25910 -> "25,900+")
  const formattedCount = totalTranscripts > 0 
    ? `${(Math.floor(totalTranscripts / 100) * 100).toLocaleString()}+` 
    : '25,000+'

  const prevTermRef = useRef(term)
  useEffect(() => {
    // If the user modifies the primary term (enters a new search or clears it),
    // and the previous value was the default, clear the raw regex and hide the
    // advanced regex box to prevent accidentally using the default William Branham regex.
    const prevTerm = prevTermRef.current || ''
    const currentTerm = (term || '').trim()
    if (prevTerm === DEFAULT_TERM && currentTerm !== DEFAULT_TERM) {
      setRawRegex('')
      setShowRegex(false)
    }
    prevTermRef.current = term
  }, [term])

  const validateRegex = (r) => {
    if (!r) return null
    try { new RegExp(r); return null } catch (e) { return (e && e.message) ? e.message : 'Invalid regex' }
  }

  const handleRun = () => {
    const t = (term || '').trim()
    // Allow running when either a term exists or a raw regex is provided
    if (!t && !(showRegex && rawRegex && rawRegex.trim())) return
    if (showRegex && rawRegex && rawRegex.trim()) {
      const err = validateRegex(rawRegex)
      if (err) { setRegexError(err); return }
      setRegexError(null)
      onAnalyze(t, [], rawRegex, { wholeWords: false }) // Raw regex ignores wholeWords
      return
    }
    const vars = (variations || '').split(',').map(v => v.trim()).filter(Boolean)
    onAnalyze(t, vars, null, { wholeWords })
  }

  const handleResetDefaults = () => {
    setTerm(DEFAULT_TERM)
    // DEFAULT_REGEX_STR is regex-like so show advanced box
    setShowRegex(true)
    setRawRegex(DEFAULT_REGEX_STR)
    setVariations('')
    setRegexError(null)
    setWholeWords(true)
    // trigger analysis with default regex
    onAnalyze(DEFAULT_TERM, [], DEFAULT_REGEX_STR, { wholeWords: false })
  }

  return (
    <div className="bg-gradient-to-r from-blue-600 to-indigo-700 p-4 rounded-xl text-white shadow-lg mb-6 transition-all">
      <div className="flex flex-col lg:flex-row gap-3 items-stretch">
        <div className="flex-1 min-w-0">
          <h2 className="text-base font-bold flex items-center gap-2"><Icon name="search" size={18} /> Transcript Search Engine</h2>
          <p className="text-blue-100 text-xs mb-2">Search across all {formattedCount} sermon transcripts for any topic or phrase.</p>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
            <div>
              <label className="text-xs font-semibold uppercase tracking-wide text-blue-200 mb-0.5 block">Primary Term</label>
              <input type="text" value={term} onChange={e => setTerm(e.target.value)} onKeyDown={e => { if (e.key === 'Enter') handleRun() }} placeholder="e.g. Eagle" className="w-full text-gray-900 px-3 py-1.5 rounded-lg outline-none focus:ring-2 focus:ring-blue-400 text-sm" />
            </div>
            <div>
              <label className="text-xs font-semibold uppercase tracking-wide text-blue-200 mb-0.5 block">Variations (Comma Separated)</label>
              <input type="text" value={variations} onChange={e => setVariations(e.target.value)} onKeyDown={e => { if (e.key === 'Enter') handleRun() }} placeholder="e.g. Eagles, Prophet, Bird" className="w-full text-gray-900 px-3 py-1.5 rounded-lg outline-none focus:ring-2 focus:ring-blue-400 text-sm" />
            </div>
          </div>
          <div className="mt-1.5 flex items-center gap-4 flex-wrap">
            <label className="flex items-center gap-1.5 cursor-pointer">
              <input type="checkbox" checked={wholeWords} onChange={e => setWholeWords(e.target.checked)} className="rounded text-blue-600 focus:ring-blue-400 w-3.5 h-3.5" disabled={showRegex} />
              <span className="text-xs text-blue-100">Whole words only</span>
            </label>
            <button type="button" onClick={() => { setShowRegex(prev => !prev); setRegexError(null); if (!showRegex) setRawRegex('') }} className="text-xs text-blue-200 hover:text-white transition">
              Advanced: <span className="underline">{showRegex ? 'Hide Regex' : 'Show Regex'}</span>
            </button>
          </div>
          {showRegex && <div className="mt-2 flex items-start gap-2">
            <textarea 
              value={rawRegex} 
              onChange={e => { 
                setRawRegex(e.target.value); 
                setRegexError(validateRegex(e.target.value));
                // Auto-resize on desktop, keep 4 rows min on mobile
                if (window.innerWidth >= 640) {
                  e.target.style.height = 'auto';
                  e.target.style.height = Math.min(e.target.scrollHeight, 120) + 'px';
                }
              }} 
              placeholder="Enter raw regex pattern" 
              className="flex-1 text-xs px-2 py-1.5 rounded border text-gray-900 font-mono resize-y overflow-hidden"
              style={{ minHeight: typeof window !== 'undefined' && window.innerWidth < 640 ? '80px' : '32px', maxHeight: '120px' }}
              rows={typeof window !== 'undefined' && window.innerWidth < 640 ? 4 : 1}
            />
            {rawRegex && !regexError && (
              <button 
                type="button" 
                onClick={() => {
                  const result = expandRegex(rawRegex)
                  setPreviewData(result)
                  setShowPreview(true)
                }}
                className="text-xs bg-blue-500/50 hover:bg-blue-500/70 text-white px-2 py-1.5 rounded transition flex items-center gap-1 whitespace-nowrap"
              >
                <Icon name="eye" size={12} /> Preview
              </button>
            )}
            {regexError && <span className="text-xs text-red-200 py-1.5">Invalid</span>}
          </div>}
        </div>
        <div className="flex flex-row lg:flex-col items-center lg:items-stretch justify-center gap-2 lg:w-[130px]">
          <button type="button" onClick={handleRun} disabled={isAnalyzing || (!(term && term.trim().length) && !(showRegex && rawRegex && rawRegex.trim()))} className="bg-white text-blue-700 font-bold py-2 px-4 rounded-lg shadow-md hover:bg-blue-50 transition disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2 text-sm w-full">
            {isAnalyzing ? <><Icon name="refresh" className="animate-spin" size={16} /> Scanning...</> : <><Icon name="search" size={16} /> Search</>}
          </button>
          <button type="button" onClick={handleResetDefaults} disabled={isAnalyzing} className="bg-white/90 text-gray-700 font-medium py-0.5 px-4 rounded shadow-sm hover:bg-white transition text-xs w-full">Reset Defaults</button>
          {isAnalyzing && progress && (
            <div className="w-full space-y-1">
              <div className="w-full bg-blue-900/50 rounded-full h-2 overflow-hidden">
                <div 
                  className="h-full bg-gradient-to-r from-blue-400 to-green-400 rounded-full transition-all duration-300 ease-out"
                  style={{ width: `${progress.percent || 0}%` }}
                />
              </div>
              <p className="text-xs text-blue-100 text-center">{progress.status}</p>
              {progress.detail && <p className="text-xs text-blue-300/70 text-center">{progress.detail}</p>}
            </div>
          )}
          {cacheStatus && cacheStatus.total > 0 && !isAnalyzing && (
            <div className="text-xs text-blue-200 flex items-center gap-1" title="Cached chunks enable faster repeat searches">
              <Icon name="database" size={12} />
              <span>{cacheStatus.cached}/{cacheStatus.total}</span>
              {cacheStatus.cached === cacheStatus.total && <span className="text-green-300">⚡</span>}
            </div>
          )}
        </div>
      </div>
      {/* Display matched terms after analysis - shows results from entire database, not filtered view */}
      {matchedTerms && matchedTerms.length > 0 && !isAnalyzing && (
        <div className="mt-2 pt-2 border-t border-blue-400/30">
          <div className="flex items-center gap-2 mb-1">
            <p className="text-xs text-blue-200 font-semibold">Terms Found in Entire Database</p>
            <span className="text-xs text-blue-300/60 italic">• filtered results below may differ</span>
          </div>
          <div className="flex flex-wrap gap-1">
            {matchedTerms.slice(0, showAllTerms ? matchedTerms.length : 30).map((t, i) => {
              const maxCount = matchedTerms[0]?.count || 1
              const opacity = 0.3 + (t.count / maxCount) * 0.7
              return (
                <span key={i} className="text-white text-xs px-1.5 py-0.5 rounded" style={{ backgroundColor: `rgba(59, 130, 246, ${opacity})` }}>
                  {t.term} <span className="opacity-75">({t.count.toLocaleString()})</span>
                </span>
              )
            })}
            {matchedTerms.length > 30 && (
              <button 
                type="button" 
                onClick={() => setShowAllTerms(!showAllTerms)} 
                className="text-blue-200 text-xs self-center hover:text-white transition underline cursor-pointer"
              >
                {showAllTerms ? 'Show less' : `+${matchedTerms.length - 30} more`}
              </button>
            )}
          </div>
        </div>
      )}

      {/* Regex Preview Modal */}
      {showPreview && (
        <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4" onClick={() => setShowPreview(false)}>
          <div className="bg-white rounded-xl shadow-2xl max-w-2xl w-full max-h-[80vh] overflow-hidden" onClick={e => e.stopPropagation()}>
            <div className="bg-gradient-to-r from-blue-600 to-indigo-700 text-white p-4 flex justify-between items-center">
              <h3 className="font-bold flex items-center gap-2"><Icon name="eye" /> Regex Pattern Preview</h3>
              <button onClick={() => setShowPreview(false)} className="text-white/80 hover:text-white"><Icon name="x" /></button>
            </div>
            <div className="p-4">
              {previewData.error ? (
                <div className="text-red-600 bg-red-50 p-3 rounded-lg">
                  <strong>Error expanding pattern:</strong> {previewData.error}
                  <p className="text-sm mt-2 text-red-500">Some complex patterns cannot be fully expanded. Try simplifying the pattern or use regex101.com for testing.</p>
                </div>
              ) : (
                <>
                  <div className="text-sm text-gray-600 mb-3">
                    <strong>{previewData.matches.length.toLocaleString()}</strong> possible matches found
                    {previewData.truncated && <span className="text-amber-600 ml-2">(truncated at 500 for performance)</span>}
                  </div>
                  <div className="bg-gray-50 rounded-lg p-3 max-h-[50vh] overflow-y-auto">
                    {previewData.matches && previewData.matches.length > 0 ? (
                      <div className="grid grid-cols-2 md:grid-cols-3 gap-1 text-sm font-mono">
                        {previewData.matches.map((match, i) => (
                          <div key={i} className="bg-white text-gray-900 px-2 py-1 rounded border border-gray-200 truncate" title={String(match)}>
                            {String(match) || <span className="text-gray-400">(empty)</span>}
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div className="text-gray-500 text-center py-4">No matches found. The pattern may be too complex to expand.</div>
                    )}
                  </div>
                  <div className="mt-4 text-xs text-gray-500">
                    <p><strong>Note:</strong> This preview shows possible string matches based on expanding character classes and alternations.
                      The actual search may find additional matches depending on quantifiers and context.</p>
                  </div>
                </>
              )}
            </div>
            <div className="border-t p-3 flex justify-between items-center bg-gray-50">
              <a href="https://regex101.com/" target="_blank" rel="noopener noreferrer" className="text-blue-600 text-sm hover:underline flex items-center gap-1">
                <Icon name="link" size={14} /> Test pattern on regex101.com ↗
              </a>
              <button onClick={() => setShowPreview(false)} className="bg-blue-600 text-white px-4 py-2 rounded-lg text-sm hover:bg-blue-700 transition">Close</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
