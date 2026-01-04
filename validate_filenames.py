#!/usr/bin/env python3
"""
Validate filenames in the data directory to prevent GitHub Pages deployment failures.

This script checks for:
1. Filenames that exceed the GitHub Pages byte limit (255 bytes)
2. Unicode characters that inflate byte length
3. Illegal characters for cross-platform compatibility

Run this before committing to catch problematic filenames early.

Usage:
    python validate_filenames.py           # Check for issues
    python validate_filenames.py --fix     # Automatically fix issues
"""

import os
import sys
import re
import argparse
import unicodedata

DATA_DIR = "data"
MAX_FILENAME_BYTES = 255  # GitHub Pages limit

# Unicode fancy character mappings (same as in update_sermons.py)
UNICODE_TO_ASCII = {
    # Mathematical italic letters
    '𝐴': 'A', '𝐵': 'B', '𝐶': 'C', '𝐷': 'D', '𝐸': 'E', '𝐹': 'F', '𝐺': 'G', '𝐻': 'H', '𝐼': 'I',
    '𝐽': 'J', '𝐾': 'K', '𝐿': 'L', '𝑀': 'M', '𝑁': 'N', '𝑂': 'O', '𝑃': 'P', '𝑄': 'Q', '𝑅': 'R',
    '𝑆': 'S', '𝑇': 'T', '𝑈': 'U', '𝑉': 'V', '𝑊': 'W', '𝑋': 'X', '𝑌': 'Y', '𝑍': 'Z',
    '𝑎': 'a', '𝑏': 'b', '𝑐': 'c', '𝑑': 'd', '𝑒': 'e', '𝑓': 'f', '𝑔': 'g', '𝘩': 'h', '𝑖': 'i',
    '𝑗': 'j', '𝑘': 'k', '𝑙': 'l', '𝑚': 'm', '𝑛': 'n', '𝑜': 'o', '𝑝': 'p', '𝑞': 'q', '𝑟': 'r',
    '𝑠': 's', '𝑡': 't', '𝑢': 'u', '𝑣': 'v', '𝑤': 'w', '𝑥': 'x', '𝑦': 'y', '𝑧': 'z',
    # Mathematical bold letters
    '𝐀': 'A', '𝐁': 'B', '𝐂': 'C', '𝐃': 'D', '𝐄': 'E', '𝐅': 'F', '𝐆': 'G', '𝐇': 'H', '𝐈': 'I',
    '𝐉': 'J', '𝐊': 'K', '𝐋': 'L', '𝐌': 'M', '𝐍': 'N', '𝐎': 'O', '𝐏': 'P', '𝐐': 'Q', '𝐑': 'R',
    '𝐒': 'S', '𝐓': 'T', '𝐔': 'U', '𝐕': 'V', '𝐖': 'W', '𝐗': 'X', '𝐘': 'Y', '𝐙': 'Z',
    '𝐚': 'a', '𝐛': 'b', '𝐜': 'c', '𝐝': 'd', '𝐞': 'e', '𝐟': 'f', '𝐠': 'g', '𝐡': 'h', '𝐢': 'i',
    '𝐣': 'j', '𝐤': 'k', '𝐥': 'l', '𝐦': 'm', '𝐧': 'n', '𝐨': 'o', '𝐩': 'p', '𝐪': 'q', '𝐫': 'r',
    '𝐬': 's', '𝐭': 't', '𝐮': 'u', '𝐯': 'v', '𝐰': 'w', '𝐱': 'x', '𝐲': 'y', '𝐳': 'z',
    # Common decorative characters
    'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u', 'ñ': 'n',
    'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U', 'Ñ': 'N',
    'ü': 'u', 'Ü': 'U', 'ö': 'o', 'Ö': 'O', 'ä': 'a', 'Ä': 'A',
    '–': '-', '—': '-', ''': "'", ''': "'", '"': '"', '"': '"',
    '…': '...', '•': '-', '·': '-',
}


def normalize_unicode_to_ascii(text):
    """Convert Unicode fancy characters to ASCII equivalents."""
    for unicode_char, ascii_char in UNICODE_TO_ASCII.items():
        text = text.replace(unicode_char, ascii_char)
    
    normalized = unicodedata.normalize('NFKD', text)
    ascii_text = normalized.encode('ascii', 'ignore').decode('ascii')
    return ascii_text


def sanitize_filename(filename):
    """Generate a safe filename from a potentially problematic one."""
    # Preserve the extension
    base, ext = os.path.splitext(filename)
    
    # Extract date prefix if present (YYYY-MM-DD - )
    date_match = re.match(r'^(\d{4}-\d{2}-\d{2})\s*-\s*(.+)$', base)
    if date_match:
        date_prefix = date_match.group(1)
        rest = date_match.group(2)
    else:
        date_prefix = None
        rest = base
    
    # Normalize Unicode to ASCII
    rest = normalize_unicode_to_ascii(rest)
    
    # Remove illegal characters
    rest = re.sub(r'[\\/*?:"<>|#]', "", rest)
    rest = re.sub(r'\s+', ' ', rest)
    rest = re.sub(r'-+', '-', rest)
    rest = rest.strip(' -')
    
    # Reconstruct filename
    if date_prefix:
        new_base = f"{date_prefix} - {rest}"
    else:
        new_base = rest
    
    # Truncate if too long
    max_base_bytes = MAX_FILENAME_BYTES - len(ext.encode('utf-8'))
    while len(new_base.encode('utf-8')) > max_base_bytes:
        if ' ' in new_base:
            new_base = new_base.rsplit(' ', 1)[0]
        else:
            new_base = new_base[:-1]
        new_base = new_base.rstrip(' -')
    
    return new_base + ext


def check_filename(filepath):
    """
    Check if a filename has issues.
    Returns tuple: (has_issue, issue_type, byte_length, suggested_fix)
    
    Only flags files that exceed the MAX_FILENAME_BYTES limit.
    Non-ASCII characters are fine as long as the total byte length is under the limit.
    """
    filename = os.path.basename(filepath)
    byte_length = len(filename.encode('utf-8'))
    
    issues = []
    
    # Only check byte length - non-ASCII characters are fine if under limit
    if byte_length > MAX_FILENAME_BYTES:
        issues.append(f"exceeds {MAX_FILENAME_BYTES} bytes ({byte_length} bytes)")
    
    if issues:
        suggested = sanitize_filename(filename)
        return (True, ", ".join(issues), byte_length, suggested)
    
    return (False, None, byte_length, None)


def scan_directory(data_dir, fix=False):
    """Scan all files in data directory for filename issues."""
    issues_found = 0
    files_fixed = 0
    
    for root, dirs, files in os.walk(data_dir):
        for filename in files:
            if not filename.endswith('.txt'):
                continue
                
            filepath = os.path.join(root, filename)
            has_issue, issue_type, byte_length, suggested = check_filename(filepath)
            
            if has_issue:
                issues_found += 1
                rel_path = os.path.relpath(filepath, data_dir)
                print(f"\n❌ {rel_path}")
                print(f"   Issue: {issue_type}")
                print(f"   Bytes: {byte_length}")
                
                if suggested and suggested != filename:
                    print(f"   Suggested: {suggested}")
                    
                    if fix:
                        new_filepath = os.path.join(root, suggested)
                        if os.path.exists(new_filepath):
                            print(f"   ⚠️  Cannot fix: target file already exists")
                        else:
                            try:
                                os.rename(filepath, new_filepath)
                                print(f"   ✅ Fixed!")
                                files_fixed += 1
                            except Exception as e:
                                print(f"   ⚠️  Error fixing: {e}")
    
    return issues_found, files_fixed


def main():
    parser = argparse.ArgumentParser(description="Validate filenames for GitHub Pages compatibility")
    parser.add_argument('--fix', action='store_true', help="Automatically fix problematic filenames")
    parser.add_argument('--dir', default=DATA_DIR, help="Directory to scan (default: data)")
    args = parser.parse_args()
    
    print(f"Scanning {args.dir} for filename issues...")
    print(f"Max filename bytes: {MAX_FILENAME_BYTES}")
    print("=" * 60)
    
    issues_found, files_fixed = scan_directory(args.dir, fix=args.fix)
    
    print("\n" + "=" * 60)
    if issues_found == 0:
        print("✅ No filename issues found!")
        sys.exit(0)
    else:
        print(f"Found {issues_found} files with issues")
        if args.fix:
            print(f"Fixed {files_fixed} files")
            if issues_found > files_fixed:
                print(f"⚠️  {issues_found - files_fixed} files could not be fixed automatically")
                sys.exit(1)
        else:
            print("\nRun with --fix to automatically rename problematic files")
            sys.exit(1)


if __name__ == "__main__":
    main()
