// AUTO-GENERATED - DO NOT EDIT MANUALLY
// Generated from config/search_config.json
// Run: python3 config/generate_frontend_constants.py

export const DEFAULT_TERM = 'William Branham'
export const DEFAULT_REGEX_STR = '\\b(?:(?:brother\\s+william)|william|brother)\\s+br[aeiou]n[dh]*[aeiou]m\\b'
export const DEFAULT_VARIATIONS = '(?:brother\\s+william|william|brother)\\s+br[aeiou]n[dh]*[aeiou]m'
export const WORDS_PER_MINUTE = 140

// UI constants (not from config - can be moved to config later if needed)
export const CHART_POINT_THRESHOLD = 800
export const COLORS = ['#2563eb','#dc2626','#16a34a','#d97706','#9333ea','#0891b2','#be123c','#4f46e5']
export const getColor = (index) => COLORS[index % COLORS.length]
