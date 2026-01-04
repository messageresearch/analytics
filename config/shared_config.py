"""
Shared configuration loader for sermon processing scripts.

This module provides a single source of truth for configuration values
used by update_sermons.py and generate_site_data.py.

SAFETY: All functions include fallback defaults so the scripts work
even if config files are missing or malformed.
"""

import os
import json
from pathlib import Path

# Find the project root (where this file lives or parent of config/)
_THIS_DIR = Path(__file__).parent
_PROJECT_ROOT = _THIS_DIR.parent if _THIS_DIR.name == 'config' else _THIS_DIR

CONFIG_DIR = _PROJECT_ROOT / 'config'
SPEAKERS_CONFIG_FILE = CONFIG_DIR / 'speakers_config.json'
SEARCH_CONFIG_FILE = CONFIG_DIR / 'search_config.json'


def _load_json_config(filepath, default=None):
    """Safely load a JSON config file with fallback."""
    if default is None:
        default = {}
    try:
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f"⚠️ Warning: Could not load {filepath}: {e}")
    return default


# --- SPEAKER CONFIGURATION ---

_speakers_config = None

def get_speakers_config():
    """Load speaker configuration (cached after first call)."""
    global _speakers_config
    if _speakers_config is None:
        _speakers_config = _load_json_config(SPEAKERS_CONFIG_FILE, {})
    return _speakers_config


def get_invalid_speakers():
    """
    Get the set of speaker names that should be treated as 'Unknown Speaker'.
    These are often misdetected sermon titles or other non-person entries.
    
    Returns:
        set: Speaker names to filter out
    """
    config = get_speakers_config()
    invalid = config.get('invalid_speakers', [])
    
    # Fallback hardcoded list if config is missing
    if not invalid:
        invalid = [
            "Eduan Naude", "Eduan Naudé", "Eduan Naud", 
            "Forest Farmer The Fruit", "Forrest Farmer",
            "Financial Jubilee", "Finding Yourself", 
            "Fitly Joined Together", "Five Comings"
        ]
    
    return set(invalid)


def get_speaker_normalization_rules():
    """
    Get speaker name normalization rules (maps variant -> canonical).
    
    Returns:
        dict: Mapping of variant names to canonical names
    """
    config = get_speakers_config()
    return config.get('normalization_rules', {})


def get_choir_mappings():
    """
    Get choir name mappings based on keywords.
    
    Returns:
        dict: Mapping of keyword -> choir display name
    """
    config = get_speakers_config()
    return config.get('choir_mappings', {
        "evening light": "Evening Light Choir",
        "bethel": "Bethel Tabernacle Choir",
        "default": "Church Choir"
    })


def normalize_speaker_name(speaker):
    """
    Apply normalization rules to a speaker name.
    
    Args:
        speaker: Raw speaker name
        
    Returns:
        str: Normalized speaker name, or 'Unknown Speaker' if invalid
    """
    if not speaker:
        return "Unknown Speaker"
    
    speaker = speaker.strip()
    
    # Check if this is an invalid speaker
    if speaker in get_invalid_speakers():
        return "Unknown Speaker"
    
    # Apply normalization rules
    rules = get_speaker_normalization_rules()
    if speaker in rules:
        return rules[speaker]
    
    return speaker


# --- SEARCH CONFIGURATION ---

_search_config = None

def get_search_config():
    """Load search configuration (cached after first call)."""
    global _search_config
    if _search_config is None:
        _search_config = _load_json_config(SEARCH_CONFIG_FILE, {})
    return _search_config


def get_mention_regex():
    """
    Get the regex pattern for counting Branham mentions.
    
    Returns:
        str: Regex pattern (without word boundaries)
    """
    config = get_search_config()
    return config.get(
        'mention_regex',
        r"(?:brother\s+william|william|brother)\s+br[aeiou]n[dh]*[aeiou]m"
    )


def get_default_search_regex():
    """
    Get the default search regex (with word boundaries).
    This must match what the frontend uses.
    
    Returns:
        str: Regex pattern with word boundaries
    """
    config = get_search_config()
    return config.get(
        'default_regex_with_boundaries',
        r"\b(?:(?:brother\s+william)|william|brother)\s+br[aeiou]n[dh]*[aeiou]m\b"
    )


def get_chunk_size_limit():
    """
    Get the maximum size for text chunk files (in bytes).
    
    Returns:
        int: Chunk size limit in bytes (default 5MB)
    """
    config = get_search_config()
    return config.get('chunk_size_limit_bytes', 5 * 1024 * 1024)


# --- PATH CONFIGURATION ---

def get_data_dir():
    """Get the data directory path."""
    return str(_PROJECT_ROOT / 'data')


def get_output_dir():
    """Get the site_api output directory path."""
    return str(_PROJECT_ROOT / 'site_api')


def get_docs_dir():
    """Get the docs directory for GitHub Pages."""
    return str(_PROJECT_ROOT / 'docs')


# --- UTILITY: Reload configs (for testing) ---

def reload_configs():
    """Force reload all cached configurations."""
    global _speakers_config, _search_config
    _speakers_config = None
    _search_config = None


# --- SELF-TEST ---

if __name__ == '__main__':
    print("Testing shared_config module...")
    print(f"Project root: {_PROJECT_ROOT}")
    print(f"Config dir: {CONFIG_DIR}")
    print(f"Invalid speakers: {get_invalid_speakers()}")
    print(f"Mention regex: {get_mention_regex()}")
    print(f"Chunk size: {get_chunk_size_limit():,} bytes")
    print("✅ Config module loaded successfully")
