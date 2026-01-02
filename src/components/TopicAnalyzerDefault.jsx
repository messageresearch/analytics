import React, { useState, useEffect, useRef } from 'react'
import { DEFAULT_TERM, DEFAULT_REGEX_STR } from '../constants_local'
import Icon from './Icon'
import { expandRegex } from '../utils/regexExpander'

export default function TopicAnalyzerDefault({ onAnalyze, isAnalyzing, progress, initialTerm = '', initialVariations = '', matchedTerms = [], cacheStatus = null }) {
  const [term, setTerm] = useState(initialTerm || '')
  const isRegexLike = (s) => /[\\\(\)\[\]\|\^\$\.\*\+\?]/.test(s)
  const [variations, setVariations] = useState(!isRegexLike(initialVariations) ? initialVariations : '')
  const [showRegex, setShowRegex] = useState(isRegexLike(initialVariations))
  const [rawRegex, setRawRegex] = useState(isRegexLike(initialVariations) ? initialVariations : '')
  const [regexError, setRegexError] = useState(null)
  const [wholeWords, setWholeWords] = useState(true) // Default to exact word matching
  const [showPreview, setShowPreview] = useState(false)
  const [previewData, setPreviewData] = useState({ matches: [], truncated: false, error: null })

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
    <div className="bg-gradient-to-r from-blue-600 to-indigo-700 p-6 rounded-xl text-white shadow-lg mb-8 transition-all">
      <div className="flex flex-col md:flex-row gap-6 items-center">
        <div className="flex-1">
          <h2 className="text-xl font-bold flex items-center gap-2 mb-2"><Icon name="activity" /> Global Mention Tracker</h2>
          <p className="text-blue-100 text-sm mb-4">Analyze all videos over time for a specific topic. This will scan the entire database dynamically.</p>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="text-xs font-bold uppercase tracking-wide text-blue-200 mb-1 block">Primary Term</label>
              <input type="text" value={term} onChange={e => setTerm(e.target.value)} onKeyDown={e => { if (e.key === 'Enter') handleRun() }} placeholder="e.g. Eagle" className="w-full text-gray-900 px-3 py-2 rounded-lg outline-none focus:ring-2 focus:ring-blue-400" />
            </div>
            <div>
              <label className="text-xs font-bold uppercase tracking-wide text-blue-200 mb-1 block">Variations (Comma Separated)</label>
              <input type="text" value={variations} onChange={e => setVariations(e.target.value)} onKeyDown={e => { if (e.key === 'Enter') handleRun() }} placeholder="e.g. Eagles, Prophet, Bird" className="w-full text-gray-900 px-3 py-2 rounded-lg outline-none focus:ring-2 focus:ring-blue-400" />
              <div className="mt-2 flex items-center gap-4 flex-wrap">
                <label className="flex items-center gap-2 cursor-pointer">
                  <input type="checkbox" checked={wholeWords} onChange={e => setWholeWords(e.target.checked)} className="rounded text-blue-600 focus:ring-blue-400" disabled={showRegex} />
                  <span className="text-xs text-blue-100">Whole words only</span>
                </label>
                <div className="flex items-center gap-2">
                  <label className="text-xs text-blue-100">Advanced:</label>
                  <button type="button" onClick={() => { setShowRegex(prev => !prev); setRegexError(null); if (!showRegex) setRawRegex('') }} className="text-xs text-blue-200 underline">{showRegex ? 'Hide Regex' : 'Show Regex'}</button>
                </div>
              </div>
              {showRegex && <div className="w-full mt-2">
                <textarea value={rawRegex} onChange={e => { setRawRegex(e.target.value); setRegexError(validateRegex(e.target.value)) }} placeholder="Enter raw regex pattern" className="w-full h-20 text-sm p-2 rounded border text-gray-900" />
                {regexError && <div className="text-xs text-red-200 mt-1">Regex error: {regexError}</div>}
                {rawRegex && !regexError && (
                  <button 
                    type="button" 
                    onClick={() => {
                      const result = expandRegex(rawRegex)
                      console.log('Regex expansion result:', result)
                      setPreviewData(result)
                      setShowPreview(true)
                    }}
                    className="mt-2 text-xs bg-blue-500/50 hover:bg-blue-500/70 text-white px-3 py-1 rounded transition flex items-center gap-1"
                  >
                    <Icon name="eye" size={12} /> Preview Pattern Matches
                  </button>
                )}
              </div>}
            </div>
          </div>
          {/* Display matched terms after analysis - shows results from entire database, not filtered view */}
          {matchedTerms && matchedTerms.length > 0 && !isAnalyzing && (
            <div className="mt-4 pt-4 border-t border-blue-400/30">
              <p className="text-xs text-blue-200 font-semibold mb-2">Terms Found in Results <span className="font-normal">(entire database, may differ from filtered view below)</span>:</p>
              <div className="flex flex-wrap gap-2">
                {matchedTerms.slice(0, 20).map((t, i) => (
                  <span key={i} className="bg-blue-500/40 text-white text-xs px-2 py-1 rounded-full">{t.term} <span className="text-blue-200">({t.count.toLocaleString()})</span></span>
                ))}
                {matchedTerms.length > 20 && <span className="text-blue-200 text-xs">+{matchedTerms.length - 20} more</span>}
              </div>
            </div>
          )}
        </div>
        <div className="flex flex-col items-end justify-center h-full">
          <div className="flex flex-col items-end gap-3">
            <div className="flex gap-2">
              <button onClick={handleRun} disabled={isAnalyzing || (!(term && term.trim().length) && !(showRegex && rawRegex && rawRegex.trim()))} className="bg-white text-blue-700 font-bold py-3 px-6 rounded-lg shadow-md hover:bg-blue-50 transition disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2">
                {isAnalyzing ? <><Icon name="refresh" className="animate-spin" /> Scanning...</> : <><Icon name="search" /> Run Analysis</>}
              </button>
              <button onClick={handleResetDefaults} disabled={isAnalyzing} className="bg-white text-gray-700 font-medium py-2 px-3 rounded-lg shadow-sm hover:bg-gray-50 transition">Reset Defaults</button>
            </div>
            {isAnalyzing && <p className="text-xs text-blue-200 mt-2 font-mono">{progress}</p>}
            {/* Cache status indicator */}
            {cacheStatus && cacheStatus.total > 0 && !isAnalyzing && (
              <div className="text-xs text-blue-200 flex items-center gap-1" title="Cached chunks enable faster repeat searches">
                <Icon name="database" size={12} />
                <span>{cacheStatus.cached}/{cacheStatus.total} cached</span>
                {cacheStatus.cached === cacheStatus.total && <span className="text-green-300 ml-1">⚡ Instant</span>}
              </div>
            )}
          </div>
        </div>
      </div>

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
