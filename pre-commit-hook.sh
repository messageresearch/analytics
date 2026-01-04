#!/bin/bash
#
# Pre-commit hook to validate filenames before committing
# This prevents GitHub Pages deployment failures due to long filenames
#
# To install, copy this file to .git/hooks/pre-commit and make it executable:
#   cp pre-commit-hook.sh .git/hooks/pre-commit
#   chmod +x .git/hooks/pre-commit

echo "🔍 Validating filenames..."

# Run the filename validator
python3 validate_filenames.py

if [ $? -ne 0 ]; then
    echo ""
    echo "❌ Commit blocked: Filename issues detected!"
    echo ""
    echo "Fix the issues above, or run:"
    echo "  python3 validate_filenames.py --fix"
    echo ""
    echo "Then try committing again."
    exit 1
fi

echo "✅ Filename validation passed!"
exit 0
