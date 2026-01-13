# Message Analytics - Sermon Transcript Search Engine

A powerful, full-text search engine and analytics dashboard for exploring **33,000+ sermon transcripts** from over **59 churches** in the William Branham Message community. The platform enables users to search any topic or phrase across all transcripts and visualize mention trends over time.

**Live Site:** [messageanalytics.github.io](https://messageanalytics.github.io)

---

## 🎯 Project Overview

This project solves a unique challenge: making decades of sermon content searchable and analyzable. The core feature is tracking mentions of "Brother Branham" (William Branham) across all transcripts, but the search engine supports **any search term** including:

### User-facing tour

- **Search & Stats**: Instantly search across all transcripts, filter by speaker, church, or date, and view live statistics (total sermons, churches, matches, etc.). Click any result to open a modal with transcript, jump-to-timestamp links, and metadata.
- **Sources (Data)**: Explore a per-channel breakdown of all sources, including scrape stats, last update, and direct YouTube links. Search and filter the table to audit data coverage and channel health.
- **Branham Archive**: Dedicated app for the William Branham sermon collection, with advanced search, transcript jump links, and metadata. Includes its own stats and chunked data for fast access.

### Key Statistics
- **33,317** searchable transcripts
- **59** churches tracked
- **142,944** default term matches
- **41,417** total videos indexed (including those without transcripts)

---

## ⭐ Major Recent Updates (Jan 2026)

### 1) Timestamped transcripts + click-to-jump video verification

A major research/verification upgrade: many sermons now have a companion **timestamped transcript** (a sibling `*.timestamped.txt` file) containing inline time tokens like `[12:34]`.

In the sermon viewer, these timestamps become **clickable links** that open the YouTube video **at the exact moment** the line was spoken. This dramatically improves:
- **Verification**: instantly check the source audio/video context
- **Research & analysis**: rapidly jump between relevant moments
- **Citation workflows**: share links that land on the quote

### 2) Branham Archive added (William Branham sermons)

The site now includes a dedicated **Branham Archive** experience (a separate app/view) designed specifically for searching and analyzing William Branham’s own sermons.

Key benefits:
- **Separation of datasets**: keeps the modern church transcript corpus distinct from WMB’s sermon corpus
- **Purpose-built metadata**: venue, location, date codes, duration, and detected topics (theme tags)
- **Faster navigation**: a focused interface for WMB-only research without needing to filter out unrelated channels

### 3) Improved speaker detection + metadata reliability

Speaker detection and metadata extraction have been hardened in multiple ways:
- **Improved speaker detection logic** in `update_sermons.py` (NLP + heuristics + explicit non-person pattern filtering)
- **Metadata healing** to correct common misattributions and normalize invalid speakers
- **Better source metadata capture** (e.g., backfilling missing descriptions and improving URL recovery)
- **Deduplication strategy**: when the same video appears multiple times, keep the newest metadata to avoid stale copies

### 4) Mobile performance & stability improvements

The app has explicit mobile-first performance safeguards for large-scale searches:
- **Memory-conscious search behavior** (e.g., skipping expensive caching paths on mobile)
- **Virtualized tables** with mobile column hiding (`hideOnMobile`) and tuned row sizing
- UI refinements that reduce lockups during heavy scans and filtering

### 5) Revamped “Sources” (Data) tab for dataset transparency + fast lookup

The **Sources** tab was expanded into a practical data-audit + lookup hub:
- **Channel scrape status at a glance**: see which channels are included, how many transcripts are present per channel, and the **latest transcript date** per channel (helps spot stale or not-yet-scraped sources)
- **Searchable video databases**:
  - a **global** searchable table across all channels (metadata search by title/speaker/church/date)
  - **per-channel** expandable tables with quick search + column sorting
- **Direct source linking**: one-click links back to the original YouTube video for verification
- **Quick transcript preview**: when a transcript exists, rows can open the transcript viewer directly
- **Exports**: export the channels list (CSV) and build a master CSV of sermon metadata (including YouTube links)

---

## 📏 Project & Dataset Size

### Codebase
| Category | Size |
|----------|------|
| **Frontend (React/JS)** | 10,707 lines |
| **Main App Component** | 2,927 lines |
| **UI Components** | 3,161 lines |
| **Python Scripts** | 13,642 lines |
| **Total Code** | ~24,349 lines |

### Raw Data (`data/`)
| Metric | Value |
|--------|-------|
| **Total Size** | 4.7 GB |
| **Transcript Files (plain)** | 37,974 files |
| **Transcript Files (timestamped)** | 37,426 files |
| **Average File Size (plain)** | 57.0 KB |
| **Total Raw Text (plain)** | 2.0 GB |
| **Churches/Folders** | 60 |
| **Largest Transcript (plain)** | 286 KB (multi-hour service) |

### Processed Data (`site_api/`)
| Metric | Value |
|--------|-------|
| **Total Size** | 2.0 GB |
| **JSON Chunk Files** | 392 files |
| **Chunk Size Limit** | ~5 MB each |
| **Indexed Sermons** | 41,417 |

### Production Build (`docs/`)
| Metric | Value |
|--------|-------|
| **Total Size** | 8.4 GB |
| **Includes** | Frontend + Data + API |

### What the Browser Must Handle
When a user loads the dashboard:
1. **Initial metadata**: ~32 MB (sermon index, no full text)
2. **On search**: Downloads 392 JSON chunks totaling **2.0 GB** of searchable text
3. **In memory**: Processes **41,000+ sermon records** with filtering, sorting, charting
4. **Rendering**: Virtualized table handles **33,000+ transcript rows** without crashing

This is why the project required extensive optimization—most web apps never need to handle datasets this large in the browser.

---

## 🏗️ Architecture

### Server Architecture (Static Site / Jamstack)

This project uses a **Serverless / Static Site Architecture** (often referred to as Jamstack). It does **not** rely on a traditional backend server (like Node.js, Django, or Rails) running 24/7.

1.  **No Active Server**: The live site is hosted on **GitHub Pages**, which acts as a simple file server (CDN). There is no database engine running in the background.
2.  **Build-Time Backend**: The Python scripts (`update_sermons.py`, `generate_site_data.py`) act as a "build-time backend." They run locally to ingest data, process transcripts, and generate static JSON files.
3.  **API is just Files**: When the frontend "fetches data," it is simply downloading static JSON files generated during the build process.
    *   **Pros:** Zero hosting costs, high performance (CDN), unhackable (no DB to injection).
    *   **Cons:** Data is only as fresh as the last deployment; no real-time user-to-user features.

### Data Pipeline (Python)

#### `update_sermons.py` (5,888 lines)
The main scraper that fetches sermon transcripts from YouTube channels.

**Key Features:**
- **Multi-source scraping**: Uses `pytubefix`, `scrapetube`, and YouTube APIs
- **Transcript extraction**: Downloads auto-generated captions and builds local transcript files
- **Timestamped transcript generation**: Creates `*.timestamped.txt` companions when timestamp data is available (and includes backfill tools to add timestamps to older transcripts)
- **NLP-powered speaker detection**: Uses `spaCy` plus hardened heuristics to identify speakers from titles when metadata is missing
- **Smart caching**: Tracks video ages to avoid re-processing old content
- **Healing logic**: Auto-corrects common metadata issues (wrong speakers, song detection)
- **Metadata backfills**: Utilities to backfill missing metadata (e.g., descriptions) into existing transcript files
- **Rate limiting**: Randomized delays to avoid API throttling

**Speaker Detection Logic:**
```python
# Uses NLP to extract person names from titles
doc = nlp(title)
for ent in doc.ents:
    if ent.label_ == "PERSON":
        speaker_candidates.append(ent.text)
```

#### `generate_site_data.py` (523 lines)
Processes raw transcript files into optimized JSON chunks for the frontend.

**Key Features:**
- **Chunked output**: Splits 31K+ sermons into ~5MB JSON chunks for efficient loading
- **Pre-computed regex matches**: Runs the default "Brother Branham" regex at build time
- **Metadata extraction**: Parses date, speaker, title, type, language from each file
- **Duration support**: attaches `durationMinutes` when duration metadata is available (from the per-church `*_Summary.csv` outputs)
- **Timestamp feature support**: emits `hasTimestamped` and `timestampedPath` when a `*.timestamped.txt` companion exists
- **CSV generation**: Creates master spreadsheet of all sermons

#### `generate_wmb_site_data.py`
Generates a dedicated static API for the **Branham Archive** in `wmb_api/` (its own `metadata.json` + `text_chunk_*.json` files), using the same chunking strategy as the main archive.

#### `validate_filenames.py` (194 lines)
Prevents GitHub Pages deployment failures caused by problematic filenames.

**Challenges Solved:**
- GitHub Pages has a **255-byte filename limit**
- Unicode "fancy" characters (𝐀, 𝒜, etc.) from YouTube titles inflate byte count
- Automatically converts Unicode to ASCII equivalents
- Pre-commit hook integration

#### `heal_spoken_word_dates.py`
Fixes date inconsistencies in the Spoken Word Tabernacle archive.

#### `cleanup_duplicate_dates.py`
Removes duplicate sermon entries with conflicting dates.

#### `woljc_speaker_scraper.py`
Specialized scraper for Word of Life Church speaker metadata.

---

### Frontend (React + Vite)

#### Core Application (`src/App.jsx` - 2,609 lines)
The main dashboard combining search, filtering, and visualization.

The frontend now supports two experiences:
- **Main Archive** (church YouTube channels)
- **Branham Archive** (William Branham sermons), lazy-loaded and routed via URL hash (`#branham`)

**State Management:**
- Uses React hooks with `useTransition` for non-blocking filter updates
- Pre-computes filter Sets for O(1) lookups (critical for 26K+ items)
- Memoized computations with `useMemo` to prevent expensive recalculations

**Dataset transparency / auditing:**
- The **Sources** view exposes channel coverage and scrape recency, plus searchable metadata tables with direct YouTube linking.

#### Components

| Component | Purpose |
|-----------|---------|
| `TopicAnalyzerDefault.jsx` | Search interface with term input, variations, and regex support |
| `VirtualizedTable.jsx` | Renders 31K rows using `react-window` for smooth scrolling |
| `StatCard.jsx` | Dashboard metric tiles with truncation for long text |
| `SermonModal.jsx` | Full transcript viewer with search highlighting, including **timestamped transcript toggle** and clickable **jump-to-YouTube timestamp** links when available |
| `BranhamApp.jsx` | Dedicated William Branham sermon archive UI (WMB-only searching, browsing, and analysis) |
| `ChartModal.jsx` | Expanded chart view with date picker |
| `ChannelChart.jsx` | Per-church activity visualization |
| `LazyChannelChart.jsx` | Lazy-loaded charts for performance |
| `MultiSelect.jsx` | Filter dropdowns with search and select-all |
| `HeatmapDetails.jsx` | Speaker/church heatmap visualization |
| `Icon.jsx` | SVG icon library |

#### Utilities

**`src/utils/chunkCache.js`** - IndexedDB caching system
- Stores downloaded transcript chunks locally
- Auto-invalidates when data version changes
- Dramatically speeds up repeat searches

**`src/utils/resample.js`** - Chart data aggregation
- Buckets 31K+ data points into weekly/monthly aggregates
- Prevents chart performance issues
- Caches results for repeated renders

**`src/utils/regexExpander.js`** - Search term expansion
- Converts simple terms into regex patterns
- Handles common misspellings of "Branham"

---

## ⚙️ Configuration Module

The `config/` folder contains centralized configuration shared between the Python backend and JavaScript frontend.

### Key Files

| File | Purpose |
|------|---------|
| `speakers_config.json` | Speaker normalization rules, invalid speakers list |
| `search_config.json` | Search regex patterns, chunk sizes, UI constants |
| `shared_config.py` | Python module to load configs with fallbacks |
| `generate_frontend_constants.py` | Script to generate JS constants from config |

### Developer Usage

**Python Scripts:**
```python
from config import shared_config

# Get invalid speakers (returns set)
invalid = shared_config.get_invalid_speakers()

# Normalize a speaker name
speaker = shared_config.normalize_speaker_name("Dan Evans")  # Returns "Daniel Evans"
```

**Adding a New Invalid Speaker:**
1. Edit `config/speakers_config.json`
2. Add the name to the `invalid_speakers` array
3. Run `python3 generate_site_data.py` to regenerate site data

**Syncing Frontend Constants:**
After modifying `search_config.json`, run:
```bash
python3 config/generate_frontend_constants.py
npm run build
```

---

## 📊 Visualizations

### 1. Activity & Trend Chart
- **Type**: ComposedChart (Area + Line)
- **Data**: Mention counts over time with rolling average
- **Interaction**: Click to browse sermons from that time period
- **Challenge**: Rendering 31K+ points required resampling to ~800 points

### 2. Church Coverage Bar Chart
- **Type**: Horizontal bar chart
- **Data**: Transcript count per church
- **Toggle**: Absolute count vs. percentage view

### 3. Speaker Heatmap
- **Type**: Grid heatmap
- **Data**: Speaker activity by time period
- **Challenge**: Thousands of unique speakers required virtualization

### 4. Per-Church Sparklines
- **Type**: Mini area charts
- **Data**: Individual church activity trends
- **Lazy loading**: Only renders visible charts

### 5. Dashboard Stat Cards
- Total transcripts, mentions, averages
- Peak sermon identification
- Videos without transcripts count

---

## ⚡ Performance Challenges & Solutions

### Challenge 1: Loading 31K+ Records
**Problem**: Initial page load would timeout or freeze browsers.

**Solution**: 
- Split data into ~5MB JSON chunks loaded in parallel
- Progressive loading with percentage indicator
- IndexedDB caching for repeat visits

### Challenge 2: Real-time Search Across All Transcripts
**Problem**: Searching full text of 31K transcripts took 30+ seconds.

**Solution**:
- Pre-computed default regex matches at build time
- Chunked search with progress indicator
- Web Worker offloading (optional)
- IndexedDB chunk caching

### Challenge 3: Filter Performance
**Problem**: Filtering 26K items with `Array.includes()` caused 150ms+ delays.

**Solution**:
```javascript
// Pre-compute Sets for O(1) lookups
const filterSets = useMemo(() => ({
  churches: new Set(selChurches),
  speakers: new Set(selSpeakers),
  // ...
}), [selChurches, selSpeakers, ...])
```
Result: Filter time reduced from ~150ms to ~5ms.

### Challenge 4: Table Rendering
**Problem**: Rendering 31K table rows crashed browsers.

**Solution**: 
- `react-window` for virtualized scrolling
- Only renders ~20 visible rows at a time
- Maintains scroll position and selection state

### Challenge 5: Chart Performance
**Problem**: Recharts couldn't handle 31K data points.

**Solution**:
- `resample.js` aggregates to weekly/monthly buckets
- Caches aggregated results
- Animation disabled for large datasets

### Challenge 6: Mobile Memory Constraints
**Problem**: Mobile devices ran out of memory during searches.

**Solution**:
- Disabled chunk caching on mobile
- Reduced concurrent fetch count
- Aggressive garbage collection hints

### Challenge 8: Verification (jump to the exact spoken moment)
**Problem**: Full-text search results are useful, but research and reporting require fast verification in the original source.

**Solution**:
- Add timestamped transcripts (`*.timestamped.txt`) containing inline tokens like `[12:34]`
- In the sermon viewer, render timestamps as clickable links that open YouTube at `t=<seconds>`

This turns transcript search into a direct **quote → source audio/video** workflow.

### Challenge 7: GitHub Pages Filename Limits
**Problem**: YouTube titles with Unicode caused deployment failures.

**Solution**:
- `validate_filenames.py` pre-commit hook
- Automatic Unicode → ASCII conversion
- 255-byte limit enforcement

---

## 🔧 Configuration

### `config/shared_config.py`
Centralized configuration for both Python scripts:
- Mention regex patterns
- Invalid speaker detection
- Chunk size limits

### `config/search_config.json`
Search engine settings:
- Default search terms
- Regex patterns
- Word boundary settings

### `channels.json`
YouTube channel definitions:
- Channel names and URLs
- Playlist IDs
- Custom scraping rules

### `speakers.json`
Known speaker database for metadata healing.

---

## 🚀 Deployment

### Development
```bash
npm install
npm run dev
```

### Production Build
```bash
npm run build  # Compiles to docs/
```

### Data Update Pipeline
```bash
python3 update_sermons.py      # Fetch new transcripts
python3 generate_site_data.py  # Generate JSON chunks
npm run build                  # Rebuild frontend
python3 validate_filenames.py  # Check for issues
git add -A && git commit && git push
```

### GitHub Pages
The `docs/` folder is served directly by GitHub Pages. The build process copies all assets there.

---

## ⚠️ Critical Deployment Rules

**NEVER commit without testing:**
1. Run `npm run dev` and verify changes
2. Run `npm run build` to compile
3. Run `python3 validate_filenames.py`
4. Only then commit and push

Untested commits have caused production outages due to:
- Filename encoding issues
- Missing build step (src/ changes not reflected in docs/)
- Large file commits exceeding GitHub limits

---

## 📁 Directory Structure

```
├── data/                    # Raw transcript files by church
│   ├── Church_Name/
│   │   └── YYYY-MM-DD - Title - Speaker.txt
├── docs/                    # Production build (GitHub Pages)
├── site_api/               # Generated JSON chunks
│   ├── metadata.json
│   ├── text_chunk_*.json
│   └── master_sermons.csv
├── wmb_api/                # Branham Archive JSON chunks
│   ├── metadata.json
│   └── text_chunk_*.json
├── src/                    # React frontend source
│   ├── components/
│   ├── utils/
│   └── App.jsx
├── config/                 # Shared configuration
├── update_sermons.py       # Main scraper
├── generate_site_data.py   # Data processor
└── validate_filenames.py   # Pre-commit validator
```

---

## 🛠️ Tech Stack

**Backend/Data:**
- Python 3.x
- spaCy (NLP)
- pytubefix (YouTube)
- scrapetube

**Frontend:**
- React 18
- Vite
- Tailwind CSS
- Recharts
- react-window
- IndexedDB

**Deployment:**
- GitHub Pages
- Static JSON API

---

## About This Project

This site is part of an independent, non-commercial journalistic and research project that analyzes publicly available sermon content published on Message Church YouTube channels.

The purpose of the project is to support research, reporting, and public understanding by identifying patterns, recurring references, and trends in religious messaging over time.

The project transforms publicly available speech into analytical datasets that help readers understand what is being said, how often, and across which organizations.

## Source Attribution

This project emphasizes transparency and verification. Analytical outputs include direct links to the original YouTube videos so that readers can review the full source material in its original context.

The project is not intended to replace original content and encourages readers to consult primary sources when evaluating findings or claims.

## Fair Use & Purpose

This project asserts fair use under U.S. copyright law (17 U.S.C. § 107).

Any copyrighted material referenced or analyzed is used solely for purposes of news reporting, criticism, commentary, research, and scholarship. The use is transformative, focusing on analysis and aggregation rather than simple reproduction of original works.

The project is non-commercial and does not monetize content. Full transcripts are provided for research and verification purposes, but the project does not host or stream original videos and is not intended to serve as a substitute for viewing content at the original source.

## Methodology

* Analysis is based on publicly available video metadata and transcripts
* Content is categorized by church, speaker, topic, date, language, and type
* Outputs emphasize aggregated statistics and timelines
* The project is independent and unaffiliated with any church or platform

## Transparency & Good-Faith Compliance

Requests for review, correction, or removal of specific material will be evaluated promptly and in good faith.

All content remains the property of its respective owners.

## Contact

For inquiries, corrections, or feedback, please contact:
messageanalyticsproject@gmail.com