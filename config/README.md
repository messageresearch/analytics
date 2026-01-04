# Configuration Module

This folder contains centralized configuration that's shared between:
- Python backend (`update_sermons.py`, `generate_site_data.py`)
- JavaScript frontend (`src/constants_*.js`)

## Files

| File | Purpose |
|------|---------|
| `speakers_config.json` | Speaker normalization rules, invalid speakers list |
| `search_config.json` | Search regex patterns, chunk sizes, UI constants |
| `shared_config.py` | Python module to load configs with fallbacks |
| `generate_frontend_constants.py` | Script to generate JS constants from config |

## Usage

### Python Scripts

```python
from config import shared_config

# Get invalid speakers (returns set)
invalid = shared_config.get_invalid_speakers()

# Get mention regex
regex = shared_config.get_mention_regex()

# Normalize a speaker name
speaker = shared_config.normalize_speaker_name("Dan Evans")  # Returns "Daniel Evans"
```

### Adding a New Invalid Speaker

1. Edit `config/speakers_config.json`
2. Add the name to the `invalid_speakers` array
3. Run `python3 generate_site_data.py` to regenerate site data

### Syncing Frontend Constants

After modifying `search_config.json`:

```bash
python3 config/generate_frontend_constants.py
npm run build
```

## Fallback Behavior

All config functions include hardcoded fallbacks, so the scripts work even if:
- Config files are missing
- Config files have parse errors
- New fields are added to config but not yet in the file

This ensures backwards compatibility during migration.
