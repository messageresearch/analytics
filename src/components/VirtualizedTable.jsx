import React, { useMemo, useState, useEffect, forwardRef, useRef } from 'react'
import { FixedSizeList as List } from 'react-window'
import Icon from './Icon'

export default function VirtualizedTable({
  columns,
  data,
  rowHeight = 64,
  height = 600,
  onRowClick,
  sortConfig = {},
  onSort = () => {},
  filters = {},
  onFilterChange = () => {}
}){
  // Column width customization state
  const [columnWidths, setColumnWidths] = useState({})
  const [isMobile, setIsMobile] = useState(false)
  const [resizing, setResizing] = useState(null)
  const headerRef = useRef(null)
  const dataRef = useRef(null)
  const listRef = useRef(null)
  const [scrollLeft, setScrollLeft] = useState(0)

  // Initialize mobile detection and column widths
  useEffect(() => {
    if (typeof window === 'undefined') return
    
    // Set initial mobile state
    setIsMobile(window.innerWidth < 768)
    
    // Load saved column widths
    if (typeof localStorage !== 'undefined') {
      try {
        const saved = localStorage.getItem('columnWidths')
        if (saved) {
          setColumnWidths(JSON.parse(saved))
        }
      } catch (e) {
        console.warn('Failed to load column widths:', e)
      }
    }
  }, [])

  // Save column widths to localStorage whenever they change
  useEffect(() => {
    if (typeof window === 'undefined' || typeof localStorage === 'undefined') return
    if (Object.keys(columnWidths).length === 0) return
    
    try {
      localStorage.setItem('columnWidths', JSON.stringify(columnWidths))
    } catch (e) {
      console.warn('Failed to save column widths:', e)
    }
  }, [columnWidths])

  // Handle window resize for mobile detection
  useEffect(() => {
    if (typeof window === 'undefined') return
    
    const handleResize = () => {
      setIsMobile(window.innerWidth < 768)
    }
    
    window.addEventListener('resize', handleResize)
    return () => {
      try {
        window.removeEventListener('resize', handleResize)
      } catch (e) {}
    }
  }, [])

  // Skip scroll sync for now - it's causing issues with react-window
  // The header and List will scroll independently but both are visible
  useEffect(() => {
    return () => {}
  }, [])

  // Get effective column width (customized or default, adapted for mobile)
  const getColWidth = (col) => {
    if (columnWidths[col.key]) return columnWidths[col.key]
    
    if (isMobile) {
      // Use much narrower widths on mobile
      const mobileWidths = {
        'date': '70px',
        'church': '100px',
        'title': '120px',
        'type': '70px',
        'speaker': '90px',
        'mentionCount': '60px',
        'mentionsPerHour': '60px',
        'action': '40px'
      }
      return mobileWidths[col.key] || '80px'
    }
    
    return col.width || '1fr'
  }

  // Handle column resize
  const handleMouseDown = (e, colKey) => {
    e.preventDefault()
    const startX = e.clientX
    const startWidth = columnWidths[colKey] || columns.find(c => c.key === colKey)?.width || '120px'
    const startWidthPx = parseInt(startWidth)

    const handleMouseMove = (moveEvent) => {
      const diff = moveEvent.clientX - startX
      const newWidth = Math.max(60, startWidthPx + diff)
      setColumnWidths(prev => ({ ...prev, [colKey]: `${newWidth}px` }))
    }

    const handleMouseUp = () => {
      document.removeEventListener('mousemove', handleMouseMove)
      document.removeEventListener('mouseup', handleMouseUp)
      setResizing(null)
    }

    setResizing(colKey)
    document.addEventListener('mousemove', handleMouseMove)
    document.addEventListener('mouseup', handleMouseUp)
  }

  const gridTemplate = useMemo(() => columns.map(c => getColWidth(c)).join(' '), [columns, columnWidths])

  // Compute a pixel width total for all columns so we can force the
  // react-window inner element to that width. This ensures horizontal
  // scrolling occurs on the shared outer container (`dataRef`) so the
  // sticky header scrolls horizontally together with the rows.
  const totalWidth = useMemo(() => {
    const fallbackPx = 200 // larger fallback for fractional sizes to ensure content fits
    let total = 0
    columns.forEach(c => {
      const w = getColWidth(c)
      if (typeof w === 'string' && w.endsWith('px')) {
        total += parseInt(w, 10) || fallbackPx
      } else if (typeof w === 'string' && w.endsWith('%')) {
        // percentage widths can't be resolved here; use fallback
        total += fallbackPx
      } else if (typeof w === 'string' && w.includes('fr')) {
        // For fractional units, multiply by the fr value for better sizing
        const frMatch = w.match(/(\d+)fr/)
        const frValue = frMatch ? parseInt(frMatch[1], 10) : 1
        total += fallbackPx * frValue
      } else if (typeof w === 'number') {
        total += w
      } else {
        total += fallbackPx
      }
    })
    // Add some padding to ensure all columns are visible
    return total + 60
  }, [columns, columnWidths, isMobile])

  // Custom inner element for react-window to force computed width
  const InnerElement = forwardRef(({ style, children, ...rest }, ref) => {
    const computedWidth = Math.max(style?.width || 0, totalWidth)
    const safeWidth = Number.isFinite(computedWidth) ? computedWidth : totalWidth
    const newStyle = { ...style, width: safeWidth }
    return (
      <div ref={ref} style={newStyle} {...rest}>
        {children}
      </div>
    )
  })

  const SortIndicator = ({ columnKey }) => {
    if (!sortConfig || sortConfig.key !== columnKey) return null
    return <span className="ml-2 text-xs">{sortConfig.direction === 'asc' ? '▲' : '▼'}</span>
  }

  // Table Row View (used for both mobile and desktop)
  const Row = ({ index, style }) => {
    const row = data[index]
    return (
      <div
        style={{ ...style }}
        className={`hover:bg-gray-50 border-b cursor-pointer ${isMobile ? 'text-xs' : ''}`}
        onClick={() => onRowClick && onRowClick(row)}
      >
        <div style={{ display: 'grid', gridTemplateColumns: gridTemplate, gap: isMobile ? '8px' : '12px', alignItems: 'center', padding: '8px 12px' }}>
          {columns.map(col => {
            const content = col.render ? col.render(row) : row[col.key]
            const title = typeof content === 'string' ? content : (typeof row[col.key] === 'string' ? row[col.key] : '')
            return (
              <div 
                key={col.key} 
                title={title} 
                className={`${col.className || (isMobile ? 'text-xs text-gray-700' : 'text-sm text-gray-700')} ${col.noTruncate ? '' : 'truncate'} ${col.centered ? 'text-center' : ''}`}
              >
                {content}
              </div>
            )
          })}
        </div>
      </div>
    )
  }

  // Sync horizontal scroll between header and list
  const handleContainerScroll = (e) => {
    setScrollLeft(e.target.scrollLeft)
  }

  // Double the default height for larger viewing area
  const effectiveHeight = isMobile ? Math.min(height * 2, typeof window !== 'undefined' ? window.innerHeight - 200 : 800) : height * 2

  // Single scrollable container with sticky header and virtualized list
  return (
    <div className="w-full border rounded-lg bg-white relative overflow-hidden">
      {/* Outer scrollable container for horizontal scroll */}
      <div
        ref={dataRef}
        className="overflow-x-auto overflow-y-hidden"
        style={{ 
          WebkitOverflowScrolling: 'touch', 
          width: '100%',
        }}
        onScroll={handleContainerScroll}
      >
        <div style={{ minWidth: totalWidth, width: 'max-content' }}>
          {/* Fixed header that stays in place during vertical scroll */}
          <div
            ref={headerRef}
            className={`sticky top-0 z-20 bg-gray-50 border-b ${isMobile ? 'text-xs' : ''}`}
            style={{
              minWidth: totalWidth,
              width: totalWidth,
              display: 'grid',
              gridTemplateColumns: gridTemplate,
              gap: isMobile ? '8px' : '12px',
              alignItems: 'center',
              whiteSpace: 'nowrap',
              padding: '12px 12px',
            }}
          >
            {columns.map((col, idx) => (
              <div key={col.key} className="relative">
                <div className={col.headerClass || `text-xs text-gray-600 font-medium flex items-center ${col.centered ? 'justify-center' : ''}`}>
                  <button onClick={() => onSort(col.key)} className={`flex items-center w-full hover:text-gray-900 ${col.centered ? 'justify-center text-center' : 'text-left'}`}>
                    <span className="truncate">{col.label}</span>
                    <SortIndicator columnKey={col.key} />
                  </button>
                </div>
                {!isMobile && idx < columns.length - 1 && (
                  <div
                    onMouseDown={(e) => handleMouseDown(e, col.key)}
                    className={`absolute right-0 top-0 bottom-0 w-1 cursor-col-resize hover:bg-blue-500 transition ${
                      resizing === col.key ? 'bg-blue-500' : 'bg-gray-300'
                    }`}
                    title="Drag to resize column"
                  />
                )}
              </div>
            ))}
          </div>
          
          {/* Data rows - virtualized List for performance */}
          <List
            ref={listRef}
            height={effectiveHeight}
            itemCount={data.length}
            itemSize={isMobile ? 48 : rowHeight}
            width={totalWidth}
            style={{ overflowX: 'hidden', overflowY: 'auto' }}
            innerElementType={InnerElement}
          >
            {Row}
          </List>
        </div>
      </div>
    </div>
  )
}
