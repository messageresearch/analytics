#!/usr/bin/env python3
"""
Generate frontend constants from shared config.

This script reads from config/search_config.json and generates
src/constants_generated.js to keep frontend and backend in sync.

Run this after modifying search_config.json:
    python3 config/generate_frontend_constants.py
"""

import json
import os
from pathlib import Path

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
SEARCH_CONFIG = SCRIPT_DIR / 'search_config.json'
OUTPUT_FILE = PROJECT_ROOT / 'src' / 'constants_generated.js'

def main():
    # Load search config
    with open(SEARCH_CONFIG, 'r') as f:
        config = json.load(f)
    
    # Need to escape backslashes for JS string
    mention_regex = config.get("mention_regex", "").replace("\\", "\\\\")
    default_regex = config.get("default_regex_with_boundaries", "").replace("\\", "\\\\")
    
    # Generate JS content
    js_content = f'''// AUTO-GENERATED - DO NOT EDIT MANUALLY
// Generated from config/search_config.json
// Run: python3 config/generate_frontend_constants.py

export const DEFAULT_TERM = '{config.get("default_term", "William Branham")}'
export const DEFAULT_REGEX_STR = '{default_regex}'
export const DEFAULT_VARIATIONS = '{mention_regex}'
export const WORDS_PER_MINUTE = {config.get("words_per_minute", 140)}

// UI constants (not from config - can be moved to config later if needed)
export const CHART_POINT_THRESHOLD = 800
export const COLORS = ['#2563eb','#dc2626','#16a34a','#d97706','#9333ea','#0891b2','#be123c','#4f46e5']
export const getColor = (index) => COLORS[index % COLORS.length]
'''
    
    # Write output
    with open(OUTPUT_FILE, 'w') as f:
        f.write(js_content)
    
    print(f"✅ Generated {OUTPUT_FILE}")
    print(f"   DEFAULT_TERM: {config.get('default_term')}")
    print(f"   WORDS_PER_MINUTE: {config.get('words_per_minute')}")

if __name__ == '__main__':
    main()
