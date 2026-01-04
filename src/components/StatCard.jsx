import React from 'react'
import Icon from './Icon'

export default function StatCard({ title, value, sub, icon, color='blue', fullWidth=false, onClick=null }){
  const clickable = !!onClick
  return (
    <div 
      className={`bg-white p-5 rounded-xl border border-${color}-100 shadow-sm stat-card flex justify-between items-start ${fullWidth ? 'col-span-2' : ''} ${clickable ? 'cursor-pointer hover:bg-gray-50 hover:border-'+color+'-300 transition-colors' : ''}`}
      onClick={onClick}
      role={clickable ? 'button' : undefined}
      tabIndex={clickable ? 0 : undefined}
      onKeyDown={clickable ? (e) => { if (e.key === 'Enter' || e.key === ' ') onClick() } : undefined}
    >
      <div>
        <p className="text-gray-500 text-xs font-bold uppercase tracking-wider">{title}</p>
        <h3 className={`text-2xl font-bold text-gray-900 mt-1`}>{value}</h3>
        {sub && <p className={`text-xs text-${color}-600 mt-1 font-medium`}>{sub}</p>}
      </div>
      <div className={`p-2 bg-${color}-50 text-${color}-600 rounded-lg`}><Icon name={icon} size={20} /></div>
    </div>
  )
}
