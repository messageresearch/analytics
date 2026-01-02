import React from 'react'

export default function Icon({ name, size = 18, className = '' }){
  const icons = {
    search: <path d="m21 21-5.197-5.197m0 0A7.5 7.5 0 1 0 5.196 5.196a7.5 7.5 0 0 0 10.607 10.607Z" />,
    filter: <path d="M12.37 19.63c.45.63 1.37.63 1.82 0l8.5-12A1.5 1.5 0 0 0 21.46 5.5H2.54a1.5 1.5 0 0 0-1.23 2.13l8.5 12Z" />,
    download: <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4m7-10l5 5m0 0-5 5m5-5H3" />,
    x: <path d="M18 6 6 18M6 6l12 12" />,
    barChart: <path d="M18 20V10M12 20V4M6 20v-6" />,
    fileText: <path d="M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z" />,
    users: <path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2" />,
    chevronDown: <path d="m6 9 6 6 6-6" />,
    info: <path d="M12 16v-4m0-4h.01M22 12c0 5.523-4.477 10-10 10S2 17.523 2 12 6.477 2 12 2s10 4.477 10 10Z" />,
    warning: <path d="M12 9v4m0 4h.01M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0Z" />,
    lineChart: <path d="M3 3v18h18" />,
    eye: <path d="M2 12s3-7 10-7 10 7 10 7-3 7-10 7-10-7-10-7Z" />,
    alignLeft: <path d="M21 6H3m18 6H3m18 6H3" />,
    activity: <path d="M22 12h-4l-3 9L9 3l-3 9H2" />,
    grid: <path d="M3 3h7v7H3zM14 3h7v7h-7zM14 14h7v7h-7zM3 14h7v7H3z" />,
    maximize: <path d="M15 3h6v6M9 21H3v-6M21 3l-7 7M3 21l7-7" />,
    calendar: <path d="M8 2v4m8-4v4m-12 4h16m-16 4h16m-16 4h16" />,
    sortAsc: <path d="m18 15 4 4 4-4M22 19V3M6 20V4M2 8l4-4 4 4M2 16h10" />,
    sortDesc: <path d="m18 9 4-4 4 4M22 5v16M6 20V4M2 8l4-4 4 4M2 16h10" />,
    refresh: <g><path d="M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8" /><path d="M21 3v5h-5" /><path d="M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16" /><path d="M8 16H3v5" /></g>,
    layers: <g><path d="m12.83 2.18a2 2 0 0 0-1.66 0L2.6 6.08a1 1 0 0 0 0 1.83l8.58 3.91a2 2 0 0 0 1.66 0l8.58-3.9a1 1 0 0 0 0-1.83Z" /><path d="m22 17.65-9.17 4.16a2 2 0 0 1-1.66 0L2 17.65" /><path d="m22 12.65-9.17 4.16a2 2 0 0 1-1.66 0L2 12.65" /></g>,
    link: <path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71" />,
    database: <g><ellipse cx="12" cy="5" rx="9" ry="3" /><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" /><path d="M3 12c0 1.66 4 3 9 3s9-1.34 9-3" /></g>
  }
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className={className}>
      {icons[name] || icons.search}
    </svg>
  )
}
