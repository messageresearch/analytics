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

  // Sync horizontal scroll between header and List's internal scroll (bidirectional)
  useEffect(() => {
    if (typeof window === 'undefined') return
    
    let isHeaderScrolling = false
    let isListScrolling = false
    
    // Try to find the scrollable element inside the List component
    const findScrollableElement = () => {
      if (listRef.current && listRef.current.scrollableNodeRef) {
        return listRef.current.scrollableNodeRef
      }
      // Fallback: look for a scrollable div in the DOM
      const listElement = document.querySelector('[role="presentation"]')
      return listElement
    }
    
    const scrollableElement = findScrollableElement()
    if (!scrollableElement || !headerRef.current) return
    
    // When List scrolls, update header
    const handleListScroll = (e) => {
      if (!isHeaderScrolling) {
        isListScrolling = true
        headerRef.current.scrollLeft = e.target.scrollLeft
        isListScrolling = false
      }
    }
    
    // When header scrolls, update List
    const handleHeaderScroll = (e) => {
      if (!isListScrolling) {
        isHeaderScrolling = true
        scrollableElement.scrollLeft = e.target.scrollLeft
        isHeaderScrolling = false
      }
    }
    
    scrollableElement.addEventListener('scroll', handleListScroll)
    headerRef.current.addEventListener('scroll', handleHeaderScroll)
    
    return () => {
      scrollableElement.removeEventListener('scroll', handleListScroll)
      if (headerRef.current) {
        headerRef.current.removeEventListener('scroll', handleHeaderScroll)
      }
    }
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
    const fallbackPx = 120 // default fallback per column when fractional sizes used
    let total = 0
    columns.forEach(c => {
      const w = getColWidth(c)
      if (typeof w === 'string' && w.endsWith('px')) {
        total += parseInt(w, 10) || fallbackPx
      } else if (typeof w === 'string' && w.endsWith('%')) {
        // percentage widths can't be resolved here; use fallback
        total += fallbackPx
      } else if (typeof w === 'string' && w.includes('fr')) {
        total += fallbackPx
      } else if (typeof w === 'number') {
        total += w
      } else {
        total += fallbackPx
      }
    })
    return total
  }, [columns, columnWidths, isMobile])

  // Custom inner element for react-window to force computed width
  const InnerElement = forwardRef(({ style, children, ...rest }, ref) => {
    const newStyle = { ...style, width: Math.max(style?.width || 0, totalWidth) }
    return (
      <div ref={ref} style={newStyle} {...rest}>
        {children}
      </div>
    )
  })

  const SortIndicator = ({ key }) => {
    if (!sortConfig || sortConfig.key !== key) return null
    return <span className="ml-2 text-xs">{sortConfig.direction === 'asc' ? '▲' : '▼'}</span>
  }

  // Table Row View (used for both mobile and desktop)
  const Row = ({ index, style }) => {
    const row = data[index]
    return (
      <div
        style={{ ...style }}
        className={`px-3 py-2 hover:bg-gray-50 border-b cursor-pointer ${isMobile ? 'text-xs' : ''}`}
        onClick={() => onRowClick && onRowClick(row)}
      >
        <div style={{ display: 'grid', gridTemplateColumns: gridTemplate, gap: isMobile ? '8px' : '12px', alignItems: 'center' }}>
          {columns.map(col => {
            const content = col.render ? col.render(row) : row[col.key]
            const title = typeof content === 'string' ? content : (typeof row[col.key] === 'string' ? row[col.key] : '')
            return (
              <div key={col.key} title={title} className={`${col.className || (isMobile ? 'text-xs text-gray-700' : 'text-sm text-gray-700')} truncate`}>
                {content}
              </div>
            )
          })}
        </div>
      </div>
    )
  }

  // Single table view for both mobile and desktop with locked header and data scroll
  return (
    <div className="w-full border rounded-lg bg-white overflow-hidden flex flex-col">
      {/* Header - scrollable, will be synced with List scroll */}
      <div ref={headerRef} className={`px-3 py-3 border-b bg-gray-50 ${isMobile ? 'text-xs' : ''} overflow-x-auto overflow-y-hidden`}>
        <div style={{ display:'grid', gridTemplateColumns: gridTemplate, gap: isMobile ? '8px' : '12px', alignItems: 'center', whiteSpace: 'nowrap' }}>
          {columns.map((col, idx) => (
            <div key={col.key} className="relative">
              <div className={col.headerClass || `text-xs text-gray-600 font-medium flex items-center`}>
                <button onClick={() => onSort(col.key)} className="flex items-center text-left w-full hover:text-gray-900">
                  <span>{col.label}</span>
                  <SortIndicator key={col.key} />
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
      </div>
      
      {/* List container - scroll position will be synced to header */}
      <div className="flex-1 overflow-hidden">
        {/* Data rows - List ref allows us to sync scroll to header */}
        <List
          ref={listRef}
          height={isMobile ? Math.min(height, typeof window !== 'undefined' ? window.innerHeight - 300 : 400) : height}
          itemCount={data.length}
          itemSize={isMobile ? 48 : rowHeight}
          width={'100%'}
          innerElementType={InnerElement}
        >
          {Row}
        </List>
      </div>
    </div>
  )
}
