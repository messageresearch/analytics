#!/bin/bash
#
# daily_update.sh - Automated daily workflow for sermon processing
#
# This script runs the complete update pipeline:
# 1. Scrape new videos (last N days)
# 2. Retry videos with missing transcripts/metadata
# 3. Generate site data
# 4. Upload to R2
# 5. Build site
# 6. Validate filenames
# 7. Commit and push to git
#
# Usage:
#   ./daily_update.sh              # Normal run with defaults
#   ./daily_update.sh --dry-run    # Preview what would happen
#   ./daily_update.sh --days 14    # Use 14-day window instead of 7
#   ./daily_update.sh --skip-scrape # Skip scraping step
#   ./daily_update.sh --skip-git   # Skip git commit/push
#

set -euo pipefail

# --- Configuration ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/daily_update_$TIMESTAMP.log"

# Default values
DAYS=7
DRY_RUN=false
SKIP_SCRAPE=false
SKIP_GENERATE=false
SKIP_R2=false
SKIP_BUILD=false
SKIP_VALIDATE=false
SKIP_GIT=false

# --- Color output ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# --- Helper functions ---
log() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${BLUE}[$timestamp]${NC} $1" | tee -a "$LOG_FILE"
}

log_success() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${GREEN}[$timestamp] $1${NC}" | tee -a "$LOG_FILE"
}

log_warning() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${YELLOW}[$timestamp] WARNING: $1${NC}" | tee -a "$LOG_FILE"
}

log_error() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${RED}[$timestamp] ERROR: $1${NC}" | tee -a "$LOG_FILE"
}

print_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Automated daily workflow for sermon processing."
    echo ""
    echo "Options:"
    echo "  --days N        Number of days to look back for new videos (default: 7)"
    echo "  --dry-run       Preview what would happen without making changes"
    echo "  --skip-scrape   Skip the scraping step"
    echo "  --skip-generate Skip the site data generation step"
    echo "  --skip-r2       Skip the R2 upload step"
    echo "  --skip-build    Skip the site build step"
    echo "  --skip-validate Skip the filename validation step"
    echo "  --skip-git      Skip the git commit/push step"
    echo "  -h, --help      Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                    # Normal daily run"
    echo "  $0 --dry-run          # Preview without changes"
    echo "  $0 --days 14          # Use 14-day window"
    echo "  $0 --skip-git         # Test without committing"
}

# --- Parse arguments ---
while [[ $# -gt 0 ]]; do
    case $1 in
        --days)
            DAYS="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --skip-scrape)
            SKIP_SCRAPE=true
            shift
            ;;
        --skip-generate)
            SKIP_GENERATE=true
            shift
            ;;
        --skip-r2)
            SKIP_R2=true
            shift
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --skip-validate)
            SKIP_VALIDATE=true
            shift
            ;;
        --skip-git)
            SKIP_GIT=true
            shift
            ;;
        -h|--help)
            print_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done

# --- Create logs directory ---
mkdir -p "$LOG_DIR"

# --- Start ---
echo ""
log "=============================================="
log "  Daily Update Started"
log "=============================================="
log "Working directory: $SCRIPT_DIR"
log "Log file: $LOG_FILE"
log "Days to look back: $DAYS"
log "Dry run: $DRY_RUN"
echo ""

cd "$SCRIPT_DIR"

START_TIME=$(date +%s)

# --- Step 1a: Scrape new videos ---
if [ "$SKIP_SCRAPE" = true ]; then
    log "Step 1a/7: Skipping scrape (--skip-scrape)"
else
    log "Step 1a/7: Scraping new videos from last $DAYS days..."
    if [ "$DRY_RUN" = true ]; then
        log "  [DRY RUN] Would run: python3 update_sermons.py --days $DAYS"
    else
        if python3 update_sermons.py --days "$DAYS" 2>&1 | tee -a "$LOG_FILE"; then
            log_success "Step 1a complete: Scraping finished"
        else
            log_error "Step 1a failed: Scraping encountered an error"
            exit 1
        fi
    fi
fi
echo ""

# --- Step 1b: Retry incomplete entries ---
if [ "$SKIP_SCRAPE" = true ]; then
    log "Step 1b/7: Skipping retry (--skip-scrape)"
else
    log "Step 1b/7: Retrying videos with missing transcripts..."
    if [ "$DRY_RUN" = true ]; then
        log "  [DRY RUN] Would run: python3 update_sermons.py --retry-no-transcript"
    else
        if python3 update_sermons.py --retry-no-transcript 2>&1 | tee -a "$LOG_FILE"; then
            log_success "Step 1b.1 complete: Transcript retry finished"
        else
            log_warning "Step 1b.1: Transcript retry had issues (non-fatal)"
        fi
    fi

    log "Step 1b/7: Fixing corrupt/missing metadata..."
    if [ "$DRY_RUN" = true ]; then
        log "  [DRY RUN] Would run: python3 update_sermons.py --fix-corrupt"
    else
        if python3 update_sermons.py --fix-corrupt 2>&1 | tee -a "$LOG_FILE"; then
            log_success "Step 1b.2 complete: Metadata fix finished"
        else
            log_warning "Step 1b.2: Metadata fix had issues (non-fatal)"
        fi
    fi
fi
echo ""

# --- Step 2: Generate site data ---
if [ "$SKIP_GENERATE" = true ]; then
    log "Step 2/7: Skipping site data generation (--skip-generate)"
else
    log "Step 2/7: Generating site data..."
    if [ "$DRY_RUN" = true ]; then
        log "  [DRY RUN] Would run: python3 generate_site_data.py"
    else
        if python3 generate_site_data.py 2>&1 | tee -a "$LOG_FILE"; then
            log_success "Step 2 complete: Site data generated"
        else
            log_error "Step 2 failed: Site data generation failed"
            exit 1
        fi
    fi
fi
echo ""

# --- Step 3: Upload to R2 ---
if [ "$SKIP_R2" = true ]; then
    log "Step 3/7: Skipping R2 upload (--skip-r2)"
else
    log "Step 3/7: Uploading to R2..."
    if [ "$DRY_RUN" = true ]; then
        log "  [DRY RUN] Would run: ./sync_to_r2.sh"
    else
        if [ -f "$SCRIPT_DIR/sync_to_r2.sh" ]; then
            if ./sync_to_r2.sh 2>&1 | tee -a "$LOG_FILE"; then
                log_success "Step 3 complete: R2 upload finished"
            else
                log_error "Step 3 failed: R2 upload failed"
                exit 1
            fi
        else
            log_warning "Step 3: sync_to_r2.sh not found, skipping R2 upload"
        fi
    fi
fi
echo ""

# --- Step 4: Build site ---
if [ "$SKIP_BUILD" = true ]; then
    log "Step 4/7: Skipping site build (--skip-build)"
else
    log "Step 4/7: Building site..."
    if [ "$DRY_RUN" = true ]; then
        log "  [DRY RUN] Would run: npm run build:gh"
    else
        if npm run build:gh 2>&1 | tee -a "$LOG_FILE"; then
            log_success "Step 4 complete: Site built"
        else
            log_error "Step 4 failed: Site build failed"
            exit 1
        fi
    fi
fi
echo ""

# --- Step 5: Validate filenames ---
if [ "$SKIP_VALIDATE" = true ]; then
    log "Step 5/7: Skipping filename validation (--skip-validate)"
else
    log "Step 5/7: Validating filenames..."
    if [ "$DRY_RUN" = true ]; then
        log "  [DRY RUN] Would run: python3 validate_filenames.py"
    else
        if [ -f "$SCRIPT_DIR/validate_filenames.py" ]; then
            if python3 validate_filenames.py 2>&1 | tee -a "$LOG_FILE"; then
                log_success "Step 5 complete: Filenames validated"
            else
                log_warning "Step 5: Filename validation found issues (non-fatal)"
            fi
        else
            log_warning "Step 5: validate_filenames.py not found, skipping"
        fi
    fi
fi
echo ""

# --- Step 6: Git commit and push ---
if [ "$SKIP_GIT" = true ]; then
    log "Step 6/7: Skipping git commit/push (--skip-git)"
else
    log "Step 6/7: Checking for changes to commit..."
    if [ "$DRY_RUN" = true ]; then
        log "  [DRY RUN] Would check git status and commit if changes exist"
    else
        # Check if there are any changes
        if git diff --quiet && git diff --staged --quiet && [ -z "$(git ls-files --others --exclude-standard)" ]; then
            log "Step 6: No changes to commit"
        else
            log "Step 6: Changes detected, committing..."
            git add -A

            COMMIT_DATE=$(date +%Y-%m-%d)
            git commit -m "Daily update: $COMMIT_DATE

Automated daily scrape and site rebuild.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>" 2>&1 | tee -a "$LOG_FILE" || {
                log_warning "Step 6: Commit may have failed or nothing to commit"
            }

            log "Step 6: Pushing to remote..."
            if git push 2>&1 | tee -a "$LOG_FILE"; then
                log_success "Step 6 complete: Changes committed and pushed"
            else
                log_warning "Step 6: Push failed (changes saved locally)"
            fi
        fi
    fi
fi
echo ""

# --- Summary ---
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

log "=============================================="
log_success "  Daily Update Complete!"
log "=============================================="
log "Total time: ${MINUTES}m ${SECONDS}s"
log "Log saved to: $LOG_FILE"
echo ""
