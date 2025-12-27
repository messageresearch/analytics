import React, { useMemo, useState, useEffect } from 'react'
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
  const [isMobile, setIsMobile] = useState(window.innerWidth < 768)
  const [resizing, setResizing] = useState(null)

  // Initialize column widths from localStorage or use defaults
  useEffect(() => {
    const saved = localStorage.getItem('columnWidths')
    if (saved) {
      try {
        setColumnWidths(JSON.parse(saved))
      } catch (e) {}
    }
  }, [])

  // Save column widths to localStorage whenever they change
  useEffect(() => {
    if (Object.keys(columnWidths).length > 0) {
      localStorage.setItem('columnWidths', JSON.stringify(columnWidths))
    }
  }, [columnWidths])

  // Handle window resize for mobile detection
  useEffect(() => {
    const handleResize = () => setIsMobile(window.innerWidth < 768)
    window.addEventListener('resize', handleResize)
    return () => window.removeEventListener('resize', handleResize)
  }, [])

  // Get effective column width (customized or default)
  const getColWidth = (col) => columnWidths[col.key] || col.width || '1fr'

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

  // Mobile Card View
  const CardRow = ({ index }) => {
    const row = data[index]
    return (
      <div
        className="bg-white border-b p-4 hover:bg-gray-50 cursor-pointer transition"
        onClick={() => onRowClick && onRowClick(row)}
      >
        <div className="grid grid-cols-2 gap-3 text-sm">
          {columns.filter(col => col.key !== 'action').map(col => (
            <div key={col.key} className="flex flex-col">
              <div className="text-xs font-bold text-gray-600 uppercase tracking-wide">{col.label}</div>
              <div className="text-gray-900 truncate">
                {col.render ? col.render(row) : row[col.key]}
              </div>
            </div>
          ))}
          <div className="col-span-2 flex justify-end pt-2 border-t">
            <button
              onClick={(e) => {
                e.stopPropagation()
                const a = document.createElement('a')
                a.href = row.path
                a.download = `${row.date} - ${row.title}.txt`
                a.click()
              }}
              className="text-gray-400 hover:text-blue-600 transition"
              title="Download transcript"
            >
              <Icon name="download" size={18} />
            </button>
          </div>
        </div>
      </div>
    )
  }

  // Desktop Table Row View
  const Row = ({ index, style }) => {
    const row = data[index]
    return (
      <div
        style={{ ...style }}
        className="px-4 py-3 hover:bg-gray-50 border-b cursor-pointer"
        onClick={() => onRowClick && onRowClick(row)}
      >
        <div style={{ display: 'grid', gridTemplateColumns: gridTemplate, gap: '12px', alignItems: 'center' }}>
          {columns.map(col => {
            const content = col.render ? col.render(row) : row[col.key]
            const title = typeof content === 'string' ? content : (typeof row[col.key] === 'string' ? row[col.key] : '')
            return (
              <div key={col.key} title={title} className={`${col.className || 'text-sm text-gray-700'} truncate`}>
                {content}
              </div>
            )
          })}
        </div>
      </div>
    )
  }

  if (isMobile) {
    return (
      <div className="w-full border rounded-lg bg-white">
        <div className="px-4 py-3 border-b bg-gray-50">
          <p className="text-xs text-gray-600 font-medium">Showing {data.length.toLocaleString()} sermons</p>
        </div>
        <div className="overflow-y-auto" style={{ maxHeight: height }}>
          {data.map((row, idx) => (
            <CardRow key={idx} index={idx} />
          ))}
        </div>
      </div>
    )
  }

  // Desktop table view
  return (
    <div className="w-full border rounded-lg bg-white overflow-x-auto">
      <div className="px-4 py-3 border-b bg-gray-50 sticky top-0 z-10">
        <div style={{ display:'grid', gridTemplateColumns: gridTemplate, gap: '12px', alignItems: 'center' }}>
          {columns.map((col, idx) => (
            <div key={col.key} className="relative">
              <div className={col.headerClass || 'text-xs text-gray-600 font-medium flex items-center'}>
                <button onClick={() => onSort(col.key)} className="flex items-center text-left w-full hover:text-gray-900">
                  <span>{col.label}</span>
                  <SortIndicator key={col.key} />
                </button>
              </div>
              {idx < columns.length - 1 && (
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
        {/* filters row */}
        <div className="mt-2" style={{ display:'grid', gridTemplateColumns: gridTemplate, gap: '12px', alignItems: 'center' }}>
          {columns.map(col => (
            <div key={col.key}>
              {col.filterKey ? (
                <input
                  type={col.filterType || 'text'}
                  placeholder={col.filterPlaceholder || 'Filter...'}
                  value={filters[col.filterKey] || ''}
                  onChange={(e)=>onFilterChange(col.filterKey, e.target.value)}
                  className="w-full text-xs p-1 border rounded"
                />
              ) : <div />}
            </div>
          ))}
        </div>
      </div>
      <List height={height} itemCount={data.length} itemSize={rowHeight} width={'100%'}>
        {Row}
      </List>
    </div>
  )
}
