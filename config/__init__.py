"""
Configuration module for WMB Mentions project.

Usage:
    from config import shared_config
    
    invalid = shared_config.get_invalid_speakers()
    regex = shared_config.get_mention_regex()
"""

from .shared_config import (
    # Speaker config
    get_invalid_speakers,
    get_speaker_normalization_rules,
    get_choir_mappings,
    normalize_speaker_name,
    
    # Search config
    get_mention_regex,
    get_default_search_regex,
    get_chunk_size_limit,
    
    # Paths
    get_data_dir,
    get_output_dir,
    get_docs_dir,
    
    # Utilities
    reload_configs,
)

__all__ = [
    'get_invalid_speakers',
    'get_speaker_normalization_rules', 
    'get_choir_mappings',
    'normalize_speaker_name',
    'get_mention_regex',
    'get_default_search_regex',
    'get_chunk_size_limit',
    'get_data_dir',
    'get_output_dir',
    'get_docs_dir',
    'reload_configs',
]
