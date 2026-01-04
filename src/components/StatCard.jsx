import React from 'react'
import Icon from './Icon'

export default function StatCard({ title, value, sub, icon, color='blue', fullWidth=false, onClick=null }){
  const clickable = !!onClick
  return (
    <div 
      className={`bg-white p-2 sm:p-3 rounded-lg sm:rounded-xl border border-${color}-100 shadow-sm stat-card flex justify-between items-start ${fullWidth ? 'col-span-2' : ''} ${clickable ? 'cursor-pointer hover:bg-gray-50 hover:border-'+color+'-300 transition-colors' : ''}`}
      onClick={onClick}
      role={clickable ? 'button' : undefined}
      tabIndex={clickable ? 0 : undefined}
      onKeyDown={clickable ? (e) => { if (e.key === 'Enter' || e.key === ' ') onClick() } : undefined}
    >
      <div className="min-w-0 flex-1">
        <p className="text-gray-500 text-[8px] sm:text-[10px] font-bold uppercase tracking-wider leading-tight">{title}</p>
        <h3 className={`text-base sm:text-xl font-bold text-gray-900 mt-0.5`}>{value}</h3>
        {sub && <p className={`text-[8px] sm:text-[10px] text-${color}-600 mt-0.5 font-medium leading-tight`}>{sub}</p>}
      </div>
      <div className={`p-1 sm:p-1.5 bg-${color}-50 text-${color}-600 rounded-md flex-shrink-0 ml-1`}><Icon name={icon} size={14} className="sm:w-4 sm:h-4" /></div>
    </div>
  )
}
