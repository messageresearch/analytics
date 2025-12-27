import { useRef, useEffect, useCallback } from 'react'

export default function useDebouncedCallback(fn, delay = 250){
  const timerRef = useRef(null)
  useEffect(()=> () => { if (timerRef.current) clearTimeout(timerRef.current) }, [])
  return useCallback((...args) => {
    if (timerRef.current) clearTimeout(timerRef.current)
    timerRef.current = setTimeout(() => { fn(...args) }, delay)
  }, [fn, delay])
}
