# Copilot Instructions

## ⚠️ CRITICAL: Git Deployment Rules

**NEVER commit or push to git without explicit user permission.** Always ask the user before running any of these commands:
- `git commit`
- `git push`
- `git add` followed by commit
- Any deployment scripts

This is a production website. Untested commits have caused outages. Always:
1. Test changes thoroughly in the dev server (`npm run dev`)
2. Ask the user to verify the changes work
3. **Run `npm run build`** to compile frontend changes into `docs/`
4. Only then, ASK FOR PERMISSION before committing/deploying

## ⚠️ CRITICAL: No Removing Features Without Permission

**NEVER remove, disable, or significantly alter existing functionality without explicit user permission.** This includes:
- Removing UI components, buttons, or features
- Commenting out or deleting working code
- Changing default behaviors that users rely on
- Removing columns from tables
- Disabling search features or filters
- Removing download capabilities

If a refactor or fix requires removing something, **ASK FIRST** and explain:
1. What will be removed
2. Why it needs to be removed
3. What (if anything) will replace it

## ⚠️ CRITICAL: Testing Requirements

**All code changes MUST be tested before considering them complete:**

### Frontend changes (`src/`):
1. User runs `npm run dev` to start dev server
2. Manually verify the change works as expected
3. Test on both desktop AND mobile viewports (< 768px)
4. Check browser console for errors
5. User confirms changes work before proceeding

### Python script changes:
1. Run the script with a small test case first
2. Verify output files are correct
3. Check for errors in terminal output

### Do NOT assume changes work. Verify them.

## ⚠️ Dev Server Rules

**DO NOT run `npm run dev` or start the dev server.** The user will run the dev server themselves. You can:
- Make code changes
- Run `npm run build` when requested
- Run Python scripts for data processing

But leave dev server management to the user.

### Deployment Checklist
Before pushing any changes that include frontend (`src/`) modifications:
- [ ] `npm run dev` - Test in dev server
- [ ] User confirms changes work
- [ ] `npm run build` - Build production bundle to `docs/`
- [ ] `python3 validate_filenames.py` - Check for problematic filenames
- [ ] `git add -A && git commit` - Stage and commit all changes
- [ ] `git push` - Push to GitHub

### Filename Validation
The `validate_filenames.py` script checks for filenames that could cause GitHub Pages deployment failures:
- Filenames exceeding 255 bytes (GitHub limit)
- Unicode fancy characters (mathematical italic/bold letters) that inflate byte length
- To auto-fix issues: `python3 validate_filenames.py --fix`

**Note:** The `docs/` folder is served by GitHub Pages. If you only commit `src/` changes without running `npm run build`, the live site will NOT update!

## ⚠️ CRITICAL: File Path Encoding Rules

When working with file paths that contain special characters (sermon filenames have spaces, commas, apostrophes, etc.):

### DO NOT use `encodeURIComponent()` for file paths
`encodeURIComponent()` encodes ALL special characters, including commas (`,` → `%2C`) and apostrophes. This breaks file fetches because the actual files have literal commas in their names.

### DO use selective encoding
Only encode characters that actually break URLs:
```javascript
// CORRECT: Selective encoding
const encodedPath = path.split('/').map(part => 
  part.replace(/ /g, '%20').replace(/#/g, '%23')
).join('/')

// WRONG: Over-encoding breaks file fetches
const encodedPath = path.split('/').map(part => encodeURIComponent(part)).join('/')
```

### Characters to encode:
- Spaces → `%20`
- Hash `#` → `%23`

### Characters to leave alone:
- Commas `,`
- Apostrophes `'`
- Parentheses `(` `)`
- Ampersands `&`
- These are valid in URLs and filenames

### Always decode first
If a path might already be URL-encoded, decode it first to avoid double-encoding:
```javascript
const decodedPath = decodeURIComponent(path)
const encodedPath = decodedPath.split('/').map(part => 
  part.replace(/ /g, '%20').replace(/#/g, '%23')
).join('/')
```

## ⚠️ CRITICAL: VirtualizedTable Rules

The `src/components/VirtualizedTable.jsx` component uses `react-window` for performance. **Do not modify without testing on both desktop and mobile.**

### Width Handling - CRITICAL
The table uses a specific width pattern that MUST be maintained:

```javascript
// Row component - ALWAYS use width: '100%'
<div style={{ ...style, width: '100%' }}>
  <div style={{ display: 'grid', gridTemplateColumns: gridTemplate, ..., width: '100%' }}>

// List component - ALWAYS use width="100%" (string, not number)
<List
  width="100%"  // ✅ CORRECT - string "100%"
  // width={totalWidth}  // ❌ WRONG - causes horizontal scroll issues
  style={{ overflowX: 'hidden', overflowY: 'auto' }}
>
```

### DO NOT change these patterns:
1. **Row widths**: Both outer div and inner grid div must have `width: '100%'`
2. **List width**: Must be `width="100%"` (string), NOT a pixel value
3. **List overflow**: Must have `overflowX: 'hidden'` to prevent double scrollbars
4. **Container structure**: Outer `overflow-x-auto` container handles horizontal scroll, List handles vertical

### The totalWidth calculation
`totalWidth` is used for:
- `minWidth` on containers (ensures content doesn't collapse)
- `InnerElement` width override (for react-window internal element)

It should **NOT** be used for:
- `width` on the List component
- `width` on Row components

### Mobile responsiveness
- Columns with `hideOnMobile: true` are filtered out on screens < 768px
- Mobile uses smaller row heights (48px vs 64px)
- Mobile uses tighter gaps (8px vs 12px)
- Test any table changes on mobile viewport

### Column definitions
When adding columns in `App.jsx` or `BranhamApp.jsx`:
```javascript
{
  key: 'columnKey',           // Unique key
  label: 'Display Label',     // Header text
  width: '100px',             // Fixed width (px) or '1fr' for flexible
  hideOnMobile: true,         // Optional: hide on small screens
  centered: true,             // Optional: center content
  noTruncate: true,           // Optional: don't truncate text
  render: (row) => <jsx />    // Custom render function
}
```

## ⚠️ CRITICAL: BASE_URL Handling for GitHub Pages

This site is deployed to GitHub Pages. All fetch URLs and asset paths must use `import.meta.env.BASE_URL`:

```javascript
// CORRECT: Always use BASE_URL prefix
const basePath = import.meta.env.BASE_URL || '/'
fetch(`${basePath}site_api/sermons.json`)
fetch(`${basePath}${encodedPath}`)

// WRONG: Hardcoded paths break on GitHub Pages
fetch('/site_api/sermons.json')
fetch(encodedPath)
```

### When to use BASE_URL:
- Fetching JSON data from `site_api/`
- Fetching transcript files from `data/`
- Any `href` or `src` attribute pointing to local assets
- Download links for transcripts

## ⚠️ CRITICAL: Data File Conventions

### Deduplication Strategy - KEEP LATEST METADATA
When the same video URL appears in multiple church folders or with multiple entries:
- **ALWAYS deduplicate by video URL**
- **KEEP the entry with the NEWEST timestamp** (most recent metadata)
- Remove older/archived copies that may have stale information
- This is critical because old copies may be in read-only archives or contain outdated speaker names/titles

Implementation: Compare timestamps when duplicates are found and keep the one with the highest timestamp value.

### Transcript file types
- `.txt` - Plain transcript (no timestamps)
- `.timestamped.txt` - Transcript with YouTube timestamps

### When filtering files in Python:
```python
# CORRECT: Exclude timestamped files when processing plain transcripts
txt_files = [f for f in os.listdir(path) if f.endswith('.txt') and not f.endswith('.timestamped.txt')]

# WRONG: This includes both versions, causing duplicates
txt_files = [f for f in os.listdir(path) if f.endswith('.txt')]
```

### Extracting base filename (speaker names, metadata)
When parsing filenames to extract speaker names or other metadata, handle `.timestamped.txt` properly:

```python
# CORRECT: Handle double extension .timestamped.txt
def get_base_filename(filename):
    if filename.endswith('.timestamped.txt'):
        return filename[:-15]  # Remove '.timestamped.txt'
    elif filename.endswith('.txt'):
        return filename[:-4]   # Remove '.txt'
    return filename

# WRONG: os.path.splitext only removes the last extension
base = os.path.splitext('sermon - Speaker.timestamped.txt')[0]
# Result: 'sermon - Speaker.timestamped' ❌ (corrupts speaker name!)
```

This bug caused speaker names like `"John Smith.timestamped"` to appear in CSV files because `os.path.splitext()` only strips `.txt`, leaving `.timestamped` attached to the speaker name.

### Data pipeline order
Always run scripts in this order:
1. `python update_sermons.py` - Scrape new transcripts
2. `python generate_site_data.py` - Generate JSON for frontend

Never run `generate_site_data.py` alone if you need new transcripts—it only processes existing files.

### William Branham Sermon Files - THREE LOCATIONS
The WMB sermon transcript files exist in **three locations** that must stay in sync:

1. `data/William_Branham_Sermons/` - Source of truth
2. `docs/data/William_Branham_Sermons/` - For GitHub Pages production
3. `public/data/William_Branham_Sermons/` - For Vite dev server

**When modifying WMB transcript files**, sync all three:
```bash
# After fixing/modifying files in data/William_Branham_Sermons/:
cp -r data/William_Branham_Sermons/* docs/data/William_Branham_Sermons/
cp -r data/William_Branham_Sermons/* public/data/William_Branham_Sermons/
```

The `generate_wmb_site_data.py` script automatically syncs to `docs/`, but NOT to `public/`.

**Why this matters:** The dev server serves from `public/`, so if you only update `data/` and `docs/`, the dev server will still show old content.

### WMB Data Workflow
When fixing or modifying William Branham sermon transcript files:

```bash
# 1. Make changes to source files in data/William_Branham_Sermons/
# 2. Sync to all three locations AND regenerate JSON:
cp -r data/William_Branham_Sermons/* docs/data/William_Branham_Sermons/
cp -r data/William_Branham_Sermons/* public/data/William_Branham_Sermons/
python3 generate_wmb_site_data.py
```

**Key files:**
- `generate_wmb_site_data.py` - Generates `wmb_api/metadata.json` and text chunks
- `wmb_api/` - Contains WMB-specific JSON data (separate from `site_api/`)
- `BranhamApp.jsx` - Frontend for Branham Archive (uses `wmb_api/`)

**Common WMB transcript issues to watch for:**
- Venue field containing sermon title fragments instead of actual venue
- Location field containing just state code (e.g., "AZ") instead of "City, AZ"
- City in Venue field, state in Location field (should be combined in Location)
- Lowercase state codes (e.g., "Ca" instead of "CA")

## ⚠️ Mobile Testing Requirements

**Always test frontend changes on mobile viewport (< 768px):**

1. Use browser DevTools to simulate mobile
2. Check that tables scroll horizontally without breaking
3. Verify `hideOnMobile` columns are properly hidden
4. Test touch interactions (tap to open modals, scroll)

### Common mobile breakages:
- Fixed pixel widths that overflow the screen
- Missing `hideOnMobile: true` on non-essential columns
- Scroll containers not having `WebkitOverflowScrolling: 'touch'`

## Project Overview

This project is a static website that provides a searchable archive of sermon transcripts. The site is built using a Python-based data processing pipeline and a modern frontend framework.

## Core Technologies

-   **Data Processing**: Python scripts are used to fetch, parse, and structure the sermon data.
-   **Frontend**: The website is built with a modern web stack (details in `package.json`).

## Data Pipeline

The data pipeline is a multi-step process:

1.  **Update Sermons**: The `update_sermons.py` script fetches the latest sermon transcripts from the YouTube channels defined in `channels.json`. It uses advanced logic, including the `spaCy` NLP library, to identify speakers and categorize video types.

    To update the sermon data, run:
    ```bash
    python update_sermons.py
    ```
    *Note: The script may prompt you to download the `en_core_web_sm` model on first run if you don't have it.*

2.  **Heal Archive**: The script also includes a `heal_archive` function that automatically runs to clean up existing data. It corrects speaker names, identifies song/worship services, and ensures metadata consistency.

3.  **Generate Site Data**: The `generate_site_data.py` script processes the raw text files in the `data/` directory. It extracts metadata (title, speaker, date), counts mentions of "Brother Branham", and generates structured JSON files in the `site_api/` directory. This data is then used by the frontend to display the sermon archive.

    To generate the site data, run:
    ```bash
    python generate_site_data.py
    ```

## Key Files and Directories

-   `channels.json`: Contains the list of YouTube channels to scrape for new sermons.
-   `speakers.json`: A list of speakers for metadata purposes.
-   `update_sermons.py`: Fetches new sermon transcripts from YouTube.
-   `generate_site_data.py`: Processes raw sermon data and generates the site's API data.
-   `data/`: Contains the raw sermon transcript files, organized by church.
-   `site_api/`: Contains the generated JSON data used by the frontend.
-   `src/`: Contains the frontend source code.

## Frontend Development

The frontend is a modern web application. To run the local development server and work on the frontend, use the following commands:

```bash
npm install
npm run dev
```

This will start a local server and you can view the site in your browser.
