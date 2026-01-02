/**
 * Regex Expander - Expands a regex pattern into a list of matching strings
 * Handles: character classes [], alternations |, optional ?, quantifiers *, +
 * Limited expansion to prevent memory issues with complex patterns
 */

const MAX_EXPANSIONS = 500 // Safety limit

/**
 * Parse and expand a simple regex into all possible matches
 * @param {string} pattern - The regex pattern to expand
 * @returns {{ matches: string[], truncated: boolean, error: string|null }}
 */
export function expandRegex(pattern) {
  try {
    // Clean pattern - remove word boundaries and anchors
    let clean = pattern
      .replace(/\\b/g, '')  // Remove word boundaries
      .replace(/^\^/, '')    // Remove start anchor
      .replace(/\$$/, '')    // Remove end anchor
    
    const results = expand(clean)
    const truncated = results.length > MAX_EXPANSIONS
    return {
      matches: results.slice(0, MAX_EXPANSIONS),
      truncated,
      error: null
    }
  } catch (e) {
    return {
      matches: [],
      truncated: false,
      error: e.message || 'Failed to expand pattern'
    }
  }
}

/**
 * Recursive expansion of regex pattern
 */
function expand(pattern) {
  if (!pattern) return ['']
  
  // Handle alternation at the top level (respecting grouping)
  const altParts = splitAlternation(pattern)
  if (altParts.length > 1) {
    let results = []
    for (const part of altParts) {
      results = results.concat(expand(part))
      if (results.length > MAX_EXPANSIONS) break
    }
    return results
  }
  
  // Parse from left to right
  let i = 0
  let parts = []
  
  while (i < pattern.length && parts.length < 100) {
    // Handle groups (?:...) or (...)
    if (pattern[i] === '(' || (pattern.slice(i, i+3) === '(?:')) {
      const groupStart = pattern[i] === '(' && pattern[i+1] !== '?' ? i : i + 3
      const actualStart = pattern[i] === '(' && pattern[i+1] !== '?' ? i + 1 : i + 3
      const groupEnd = findMatchingParen(pattern, i)
      if (groupEnd === -1) throw new Error('Unmatched parenthesis')
      
      const groupContent = pattern.slice(actualStart, groupEnd)
      const afterGroup = pattern[groupEnd + 1]
      
      // Check for quantifier after group
      if (afterGroup === '?') {
        // Optional group
        parts.push({ type: 'optional', content: groupContent })
        i = groupEnd + 2
      } else if (afterGroup === '*') {
        // Zero or more - just use 0, 1, 2 instances
        parts.push({ type: 'star', content: groupContent })
        i = groupEnd + 2
      } else if (afterGroup === '+') {
        // One or more - use 1, 2 instances
        parts.push({ type: 'plus', content: groupContent })
        i = groupEnd + 2
      } else {
        parts.push({ type: 'group', content: groupContent })
        i = groupEnd + 1
      }
    }
    // Handle character class [...]
    else if (pattern[i] === '[') {
      const classEnd = pattern.indexOf(']', i + 1)
      if (classEnd === -1) throw new Error('Unmatched bracket')
      
      const classContent = pattern.slice(i + 1, classEnd)
      const chars = expandCharClass(classContent)
      const afterClass = pattern[classEnd + 1]
      
      if (afterClass === '?') {
        parts.push({ type: 'optionalChars', chars })
        i = classEnd + 2
      } else if (afterClass === '*') {
        parts.push({ type: 'starChars', chars })
        i = classEnd + 2
      } else if (afterClass === '+') {
        parts.push({ type: 'plusChars', chars })
        i = classEnd + 2
      } else {
        parts.push({ type: 'chars', chars })
        i = classEnd + 1
      }
    }
    // Handle escape sequences
    else if (pattern[i] === '\\') {
      const next = pattern[i + 1]
      if (next === 's') {
        // \s matches whitespace - use space
        const afterEscape = pattern[i + 2]
        if (afterEscape === '+' || afterEscape === '*') {
          parts.push({ type: 'literal', value: ' ' })
          i += 3
        } else {
          parts.push({ type: 'literal', value: ' ' })
          i += 2
        }
      } else if (next === 'd') {
        // \d matches digit - expand to 0-9
        parts.push({ type: 'chars', chars: '0123456789'.split('') })
        i += 2
      } else if (next === 'w') {
        // \w is too large to expand, just use placeholder
        parts.push({ type: 'literal', value: '[word]' })
        i += 2
      } else {
        // Escaped literal
        parts.push({ type: 'literal', value: next || '\\' })
        i += 2
      }
    }
    // Handle dot (any char) - just use a placeholder
    else if (pattern[i] === '.') {
      parts.push({ type: 'literal', value: '.' })
      i++
    }
    // Handle quantifiers on previous literal
    else if (pattern[i] === '?' && parts.length > 0 && parts[parts.length-1].type === 'literal') {
      const last = parts.pop()
      parts.push({ type: 'optionalLiteral', value: last.value })
      i++
    }
    else if (pattern[i] === '*' && parts.length > 0 && parts[parts.length-1].type === 'literal') {
      const last = parts.pop()
      parts.push({ type: 'starLiteral', value: last.value })
      i++
    }
    else if (pattern[i] === '+' && parts.length > 0 && parts[parts.length-1].type === 'literal') {
      const last = parts.pop()
      parts.push({ type: 'plusLiteral', value: last.value })
      i++
    }
    // Regular character
    else if (!'()?*+|'.includes(pattern[i])) {
      parts.push({ type: 'literal', value: pattern[i] })
      i++
    }
    else {
      i++ // Skip unhandled
    }
  }
  
  // Now expand all parts
  return expandParts(parts)
}

/**
 * Split by alternation | at the top level only
 */
function splitAlternation(pattern) {
  const parts = []
  let depth = 0
  let current = ''
  
  for (let i = 0; i < pattern.length; i++) {
    const c = pattern[i]
    if (c === '(' || c === '[') depth++
    else if (c === ')' || c === ']') depth--
    else if (c === '|' && depth === 0) {
      parts.push(current)
      current = ''
      continue
    }
    current += c
  }
  parts.push(current)
  return parts
}

/**
 * Find matching closing parenthesis
 */
function findMatchingParen(pattern, start) {
  let depth = 0
  for (let i = start; i < pattern.length; i++) {
    if (pattern[i] === '(') depth++
    else if (pattern[i] === ')') {
      depth--
      if (depth === 0) return i
    }
  }
  return -1
}

/**
 * Expand character class content
 */
function expandCharClass(content) {
  const chars = []
  let i = 0
  while (i < content.length) {
    // Handle range a-z
    if (i + 2 < content.length && content[i + 1] === '-') {
      const start = content.charCodeAt(i)
      const end = content.charCodeAt(i + 2)
      for (let c = start; c <= end && chars.length < 50; c++) {
        chars.push(String.fromCharCode(c))
      }
      i += 3
    } else {
      chars.push(content[i])
      i++
    }
  }
  return chars
}

/**
 * Expand parts into all combinations
 */
function expandParts(parts) {
  if (parts.length === 0) return ['']
  
  let results = ['']
  
  for (const part of parts) {
    if (results.length > MAX_EXPANSIONS) break
    
    let partOptions = []
    
    switch (part.type) {
      case 'literal':
        partOptions = [part.value]
        break
      case 'optionalLiteral':
        partOptions = ['', part.value]
        break
      case 'starLiteral':
        partOptions = ['', part.value, part.value + part.value]
        break
      case 'plusLiteral':
        partOptions = [part.value, part.value + part.value]
        break
      case 'chars':
        partOptions = part.chars
        break
      case 'optionalChars':
        partOptions = ['', ...part.chars]
        break
      case 'starChars':
        // 0, 1, or 2 of any char
        partOptions = ['']
        for (const c of part.chars.slice(0, 10)) {
          partOptions.push(c)
          partOptions.push(c + c)
        }
        break
      case 'plusChars':
        // 1 or 2 of any char
        partOptions = []
        for (const c of part.chars.slice(0, 10)) {
          partOptions.push(c)
          partOptions.push(c + c)
        }
        break
      case 'group':
        partOptions = expand(part.content)
        break
      case 'optional':
        partOptions = ['', ...expand(part.content)]
        break
      case 'star':
        const starExp = expand(part.content)
        partOptions = ['']
        for (const e of starExp.slice(0, 5)) {
          partOptions.push(e)
        }
        break
      case 'plus':
        partOptions = expand(part.content)
        break
      default:
        partOptions = ['']
    }
    
    // Combine with existing results
    const newResults = []
    for (const r of results) {
      for (const o of partOptions) {
        newResults.push(r + o)
        if (newResults.length > MAX_EXPANSIONS) break
      }
      if (newResults.length > MAX_EXPANSIONS) break
    }
    results = newResults
  }
  
  // Remove duplicates and sort
  return [...new Set(results)].sort()
}

export default expandRegex
