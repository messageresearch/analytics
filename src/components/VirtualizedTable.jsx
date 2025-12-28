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

  // Sync horizontal scroll between header and data rows
  useEffect(() => {
    if (typeof window === 'undefined' || !dataRef.current) return
    
    const handleDataScroll = (e) => {
      if (headerRef.current) {
        headerRef.current.scrollLeft = e.target.scrollLeft
      }
    }
    
    dataRef.current.addEventListener('scroll', handleDataScroll)
    return () => {
      if (dataRef.current) {
        dataRef.current.removeEventListener('scroll', handleDataScroll)
      }
    }
  }, [])

  // Also sync when header is scrolled
  useEffect(() => {
    if (typeof window === 'undefined' || !headerRef.current) return
    
    const handleHeaderScroll = (e) => {
      if (dataRef.current) {
        dataRef.current.scrollLeft = e.target.scrollLeft
      }
    }
    
    headerRef.current.addEventListener('scroll', handleHeaderScroll)
    return () => {
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

  // Single table view for both mobile and desktop with frozen header
  return (
    <div className="w-full border rounded-lg bg-white overflow-hidden flex flex-col">
      {/* Frozen header - always visible at top */}
      <div ref={headerRef} className={`px-3 py-3 border-b bg-gray-50 ${isMobile ? 'text-xs' : ''} overflow-x-auto`} style={{ overflowY: 'hidden' }}>
        <div style={{ display:'grid', gridTemplateColumns: gridTemplate, gap: isMobile ? '8px' : '12px', alignItems: 'center' }}>
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
      
      {/* Data rows with synchronized horizontal scroll */}
      <div ref={dataRef} className="flex-1 overflow-x-auto">
        <List 
          height={isMobile ? Math.min(height, typeof window !== 'undefined' ? window.innerHeight - 300 : 400) : height} 
          itemCount={data.length} 
          itemSize={isMobile ? 48 : rowHeight} 
          width={'100%'}
        >
          {Row}
        </List>
      </div>
    </div>
  )
}
