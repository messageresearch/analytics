import React, { useState, useRef, useEffect, memo } from 'react'
import ChannelChart from './ChannelChart'

/**
 * Lazily renders a ChannelChart only when it's about to enter the viewport.
 * This prevents rendering all 39 charts at once on page load.
 */
function LazyChannelChart({ church, data, raw, color, domain, transcriptCounts, onExpand }) {
  const [isVisible, setIsVisible] = useState(false)
  const containerRef = useRef(null)

  useEffect(() => {
    const node = containerRef.current
    if (!node) return

    // Use IntersectionObserver to detect when the chart is near viewport
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setIsVisible(true)
          observer.disconnect() // Only need to load once
        }
      },
      {
        root: null, // viewport
        rootMargin: '200px', // Load 200px before entering viewport
        threshold: 0
      }
    )

    observer.observe(node)

    return () => observer.disconnect()
  }, [])

  return (
    <div ref={containerRef} className="min-h-[180px]">
      {isVisible ? (
        <ChannelChart
          church={church}
          data={data}
          raw={raw}
          color={color}
          domain={domain}
          transcriptCounts={transcriptCounts}
          onExpand={onExpand}
        />
      ) : (
        // Placeholder while not visible - matches chart height
        <div className="animate-pulse">
          <div className="flex justify-between items-center mb-2">
            <div className="h-4 bg-gray-200 rounded w-1/3"></div>
            <div className="flex items-center gap-3">
              <div className="h-3 bg-gray-200 rounded w-12"></div>
              <div className="h-3 bg-gray-200 rounded w-12"></div>
            </div>
          </div>
          <div className="flex items-end gap-4 mb-2">
            <div className="h-3 bg-gray-200 rounded w-16"></div>
            <div className="h-3 bg-gray-200 rounded w-24"></div>
            <div className="h-3 bg-gray-200 rounded w-28"></div>
          </div>
          <div className="h-[120px] bg-gray-100 rounded"></div>
        </div>
      )}
    </div>
  )
}

// Memoize to prevent unnecessary re-renders when parent state changes
export default memo(LazyChannelChart)
