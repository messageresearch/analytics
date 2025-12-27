const cache = new Map()
const CACHE_LIMIT = 50

function makeKey(data, threshold, tag){
  if(!data || data.length===0) return `empty-${threshold}-${tag}`
  const first = data[0].timestamp || 0
  const last = data[data.length-1].timestamp || 0
  return `${data.length}-${first}-${last}-${threshold}-${tag}`
}

export default function resampleData(data, threshold = 800, tag = ''){
  const key = makeKey(data, threshold, tag)
  if(cache.has(key)) return cache.get(key)

  if(!data || data.length === 0) return []
  if (data.length <= threshold) { cache.set(key, data); return data }

  const buckets = {}
  const useMonth = data.length > 3000
  data.forEach(item => {
    if(!item.timestamp || isNaN(item.timestamp)) return
    const date = new Date(item.timestamp)
    let key; let ts
    if (useMonth) {
      key = `${date.getFullYear()}-${date.getMonth()}`
      ts = new Date(date.getFullYear(), date.getMonth(), 1).getTime()
    } else {
      const d = new Date(date)
      const day = d.getDay(), diff = d.getDate() - day + (day == 0 ? -6 : 1)
      const monday = new Date(d.setDate(diff))
      monday.setHours(0,0,0,0)
      key = monday.getTime()
      ts = key
    }
    if (!buckets[key]) buckets[key] = { timestamp: ts, mentionCount: 0, count: 0, rollingSum: 0 }
    buckets[key].mentionCount += item.mentionCount || 0
    buckets[key].rollingSum += (item.rollingAvg || 0)
    buckets[key].count++
  })

  const out = Object.values(buckets).sort((a,b)=>a.timestamp-b.timestamp).map(b=>({
    ...b,
    rollingAvg: parseFloat((b.rollingSum / b.count).toFixed(1)),
    title: useMonth ? 'Monthly Aggregate' : 'Weekly Aggregate',
    church: 'Multiple', speaker: 'Multiple'
  }))

  cache.set(key, out)
  if(cache.size > CACHE_LIMIT){
    const firstKey = cache.keys().next().value
    cache.delete(firstKey)
  }
  return out
}
