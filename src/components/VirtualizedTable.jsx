import React, { useMemo } from 'react'
import { FixedSizeList as List } from 'react-window'

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
  const gridTemplate = useMemo(() => columns.map(c => c.width || '1fr').join(' '), [columns])

  const SortIndicator = ({ key }) => {
    if (!sortConfig || sortConfig.key !== key) return null
    return <span className="ml-2 text-xs">{sortConfig.direction === 'asc' ? '▲' : '▼'}</span>
  }

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

  return (
    <div className="w-full border rounded-lg bg-white">
      <div className="px-4 py-3 border-b bg-gray-50">
        <div style={{ display:'grid', gridTemplateColumns: gridTemplate, gap: '12px', alignItems: 'center' }}>
          {columns.map(col => (
            <div key={col.key} className={col.headerClass || 'text-xs text-gray-600 font-medium flex items-center'}>
              <button onClick={() => onSort(col.key)} className="flex items-center text-left w-full">
                <span>{col.label}</span>
                <SortIndicator key={col.key} />
              </button>
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
