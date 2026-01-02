/**
 * Regex Expander - Expands a regex pattern into a list of matching strings
 * Simplified approach that handles common patterns
 */

const MAX_EXPANSIONS = 500

/**
 * Expand a regex pattern into possible matching strings
 */
export function expandRegex(pattern) {
  try {
    // Clean pattern - remove word boundaries and anchors
    let clean = pattern
      .replace(/\\b/g, '')
      .replace(/^\^/, '')
      .replace(/\$$/, '')
      .trim()
    
    const results = expandPattern(clean)
    const unique = [...new Set(results)].sort()
    const truncated = unique.length > MAX_EXPANSIONS
    
    return {
      matches: unique.slice(0, MAX_EXPANSIONS),
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
 * Main expansion logic using a token-based approach
 */
function expandPattern(pattern) {
  const tokens = tokenize(pattern)
  return expandTokens(tokens)
}

/**
 * Tokenize the pattern into manageable parts
 */
function tokenize(pattern) {
  const tokens = []
  let i = 0
  
  while (i < pattern.length) {
    // Non-capturing group (?:...)
    if (pattern.slice(i, i + 3) === '(?:') {
      const end = findClosingParen(pattern, i)
      if (end === -1) throw new Error('Unmatched parenthesis')
      const content = pattern.slice(i + 3, end)
      const quantifier = pattern[end + 1]
      
      if (quantifier === '*') {
        tokens.push({ type: 'group', content, min: 0, max: 2 })
        i = end + 2
      } else if (quantifier === '+') {
        tokens.push({ type: 'group', content, min: 1, max: 2 })
        i = end + 2
      } else if (quantifier === '?') {
        tokens.push({ type: 'group', content, min: 0, max: 1 })
        i = end + 2
      } else {
        tokens.push({ type: 'group', content, min: 1, max: 1 })
        i = end + 1
      }
    }
    // Capturing group (...)
    else if (pattern[i] === '(') {
      const end = findClosingParen(pattern, i)
      if (end === -1) throw new Error('Unmatched parenthesis')
      const content = pattern.slice(i + 1, end)
      const quantifier = pattern[end + 1]
      
      if (quantifier === '*') {
        tokens.push({ type: 'group', content, min: 0, max: 2 })
        i = end + 2
      } else if (quantifier === '+') {
        tokens.push({ type: 'group', content, min: 1, max: 2 })
        i = end + 2
      } else if (quantifier === '?') {
        tokens.push({ type: 'group', content, min: 0, max: 1 })
        i = end + 2
      } else {
        tokens.push({ type: 'group', content, min: 1, max: 1 })
        i = end + 1
      }
    }
    // Character class [...]
    else if (pattern[i] === '[') {
      const end = pattern.indexOf(']', i + 1)
      if (end === -1) throw new Error('Unmatched bracket')
      const content = pattern.slice(i + 1, end)
      const chars = expandCharClass(content)
      const quantifier = pattern[end + 1]
      
      if (quantifier === '*') {
        tokens.push({ type: 'chars', chars, min: 0, max: 2 })
        i = end + 2
      } else if (quantifier === '+') {
        tokens.push({ type: 'chars', chars, min: 1, max: 2 })
        i = end + 2
      } else if (quantifier === '?') {
        tokens.push({ type: 'chars', chars, min: 0, max: 1 })
        i = end + 2
      } else {
        tokens.push({ type: 'chars', chars, min: 1, max: 1 })
        i = end + 1
      }
    }
    // Escape sequences
    else if (pattern[i] === '\\') {
      const next = pattern[i + 1]
      if (next === 's') {
        // \s+ or \s* - just use single space
        const quantifier = pattern[i + 2]
        if (quantifier === '+' || quantifier === '*') {
          tokens.push({ type: 'literal', value: ' ' })
          i += 3
        } else {
          tokens.push({ type: 'literal', value: ' ' })
          i += 2
        }
      } else if (next === 'd') {
        tokens.push({ type: 'chars', chars: ['0','1','2','3','4','5','6','7','8','9'], min: 1, max: 1 })
        i += 2
      } else if (next === 'w') {
        tokens.push({ type: 'literal', value: '_' }) // Simplified
        i += 2
      } else if (next === 'n') {
        tokens.push({ type: 'literal', value: '\n' })
        i += 2
      } else if (next === 't') {
        tokens.push({ type: 'literal', value: '\t' })
        i += 2
      } else {
        // Escaped literal character
        tokens.push({ type: 'literal', value: next || '\\' })
        i += 2
      }
    }
    // Skip standalone quantifiers (they were handled above)
    else if ('*+?'.includes(pattern[i])) {
      // This handles quantifiers on the previous literal
      if (tokens.length > 0 && tokens[tokens.length - 1].type === 'literal') {
        const last = tokens.pop()
        if (pattern[i] === '*') {
          tokens.push({ type: 'literal_q', value: last.value, min: 0, max: 2 })
        } else if (pattern[i] === '+') {
          tokens.push({ type: 'literal_q', value: last.value, min: 1, max: 2 })
        } else if (pattern[i] === '?') {
          tokens.push({ type: 'literal_q', value: last.value, min: 0, max: 1 })
        }
      }
      i++
    }
    // Pipe for alternation at this level (shouldn't happen after tokenizing groups)
    else if (pattern[i] === '|') {
      // We'll handle alternation differently
      i++
    }
    // Regular character
    else {
      tokens.push({ type: 'literal', value: pattern[i] })
      i++
    }
  }
  
  return tokens
}

/**
 * Find the closing parenthesis
 */
function findClosingParen(pattern, start) {
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
 * Expand character class like [aeiou] or [a-z]
 */
function expandCharClass(content) {
  const chars = []
  let i = 0
  while (i < content.length) {
    // Handle range like a-z
    if (i + 2 < content.length && content[i + 1] === '-' && content[i] !== '^') {
      const start = content.charCodeAt(i)
      const end = content.charCodeAt(i + 2)
      for (let c = start; c <= end && chars.length < 26; c++) {
        chars.push(String.fromCharCode(c))
      }
      i += 3
    } 
    // Skip negation marker
    else if (content[i] === '^') {
      i++
    }
    else {
      chars.push(content[i])
      i++
    }
  }
  return chars
}

/**
 * Expand tokens into all possible strings
 */
function expandTokens(tokens) {
  let results = ['']
  
  for (const token of tokens) {
    if (results.length > MAX_EXPANSIONS) break
    
    const options = getTokenOptions(token)
    const newResults = []
    
    for (const prefix of results) {
      for (const suffix of options) {
        newResults.push(prefix + suffix)
        if (newResults.length > MAX_EXPANSIONS * 2) break
      }
      if (newResults.length > MAX_EXPANSIONS * 2) break
    }
    
    results = newResults
  }
  
  return results
}

/**
 * Get all possible values for a token
 */
function getTokenOptions(token) {
  switch (token.type) {
    case 'literal':
      return [token.value]
      
    case 'literal_q': {
      const opts = []
      for (let count = token.min; count <= token.max; count++) {
        opts.push(token.value.repeat(count))
      }
      return opts
    }
    
    case 'chars': {
      const opts = []
      if (token.min === 0) opts.push('')
      // For each repeat count, add all char combinations
      for (let count = Math.max(1, token.min); count <= token.max; count++) {
        if (count === 1) {
          opts.push(...token.chars)
        } else {
          // For count > 1, just use same char repeated (simplified)
          for (const c of token.chars) {
            opts.push(c.repeat(count))
          }
        }
      }
      return opts.length > 0 ? opts : ['']
    }
    
    case 'group': {
      // Check if group contains alternation
      const alts = splitByPipe(token.content)
      let groupOptions = []
      
      for (const alt of alts) {
        const expanded = expandPattern(alt)
        groupOptions.push(...expanded)
        if (groupOptions.length > MAX_EXPANSIONS) break
      }
      
      // Apply quantifier
      const opts = []
      if (token.min === 0) opts.push('')
      
      for (let count = Math.max(1, token.min); count <= token.max; count++) {
        if (count === 1) {
          opts.push(...groupOptions)
        } else {
          for (const g of groupOptions.slice(0, 50)) {
            opts.push(g.repeat(count))
          }
        }
      }
      
      return opts.length > 0 ? opts : ['']
    }
    
    default:
      return ['']
  }
}

/**
 * Split by pipe at the top level only (respecting nested groups)
 */
function splitByPipe(pattern) {
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
  
  return parts.filter(p => p.length > 0)
}

export default expandRegex
