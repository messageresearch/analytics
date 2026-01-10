# Utility Scripts

One-off maintenance, debugging, and fix scripts for the sermon archive.

## Categories

### Cleaning Scripts
- **clean_csv_speaker_timestamped.py** - Cleans speaker names from timestamped CSV files
- **clean_speakers_file.py** - Cleans and normalizes speakers.json
- **clean_speakers_json.py** - Additional speaker JSON cleaning
- **clean_specific_bad_names.py** - Targets specific known bad speaker names

### Fixing Scripts  
- **fix_clean_headers.py** - Fixes transcript file headers
- **fix_transcript_whitespace.py** - Normalizes whitespace in transcripts
- **fix_wmb_bad_venues.py** - Corrects William Branham venue data
- **fix_wmb_location_comprehensive.py** - Comprehensive location fixes for WMB sermons
- **fix_wmb_location_duration.py** - Fixes location and duration metadata
- **fix_wmb_venue_location.py** - Repairs venue/location field issues

### Debugging Scripts
- **debug_speaker.py** - Debug speaker detection logic
- **debug_speaker_patterns.py** - Test speaker pattern matching
- **debug_speaker_v2.py** - Enhanced speaker debugging
- **debug_speaker_validation.py** - Validates speaker detection rules

### Data Processing
- **cleanup_duplicate_dates.py** - Removes duplicate date entries
- **heal_spoken_word_dates.py** - Fixes dates for Spoken Word Tabernacle
- **incorrect_speakers_extract.py** - Extracts incorrectly detected speakers
- **incorrect_speakers_from_json.py** - Finds errors in speakers.json
- **propose_speakers_json_cleaning.py** - Suggests cleaning operations
- **remove_timestamped_from_speakers.py** - Removes timestamped suffixes
- **run_reprocess.py** - Reprocesses specific records
- **scan_prefix_clusters.py** - Analyzes speaker name prefix patterns
- **scan_speakers_prefix_contamination.py** - Detects prefix contamination
- **speaker_redetection.py** - Re-runs speaker detection
- **split_wmb_sermons.py** - Splits combined WMB sermon files

## Usage

Run from this directory:
```bash
cd utilities
python3 <script_name>.py
```

Most scripts will read from `../data/` and write outputs to `../logs/` or back to `../data/`

## Important Notes

- These are maintenance scripts - use with caution on production data
- Always backup data before running fix/clean scripts
- Some scripts import from `update_sermons.py` in the parent directory
