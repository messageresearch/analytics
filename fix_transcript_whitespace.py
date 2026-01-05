#!/usr/bin/env python3
"""Fix whitespace issues in William Branham sermon transcripts."""
import os
import re

DATA_DIR = "data/William_Branham_Sermons"

def fix_transcript(content):
    """Clean up whitespace in transcript text."""
    lines = content.split('\n')
    
    # Find the transcript separator
    transcript_start = -1
    for i, line in enumerate(lines):
        if line.strip() == '--- TRANSCRIPT ---':
            transcript_start = i
            break
    
    if transcript_start == -1:
        return content
    
    # Keep header as-is
    header_lines = lines[:transcript_start + 1]
    transcript_lines = lines[transcript_start + 1:]
    
    # Process transcript: join ALL lines within L-number paragraphs
    # Only start new paragraph at L-number markers
    result = []
    current_para = []
    
    for line in transcript_lines:
        stripped = line.strip()
        
        # L-number starts a new paragraph
        if re.match(r'^L-\d+', stripped):
            # Save previous paragraph
            if current_para:
                para_text = ' '.join(current_para)
                para_text = re.sub(r'  +', ' ', para_text)  # collapse spaces
                result.append(para_text)
            current_para = [stripped]
        elif stripped:
            # Non-empty line - add to current paragraph (ignore blank lines within paragraphs)
            current_para.append(stripped)
    
    # Don't forget last paragraph
    if current_para:
        para_text = ' '.join(current_para)
        para_text = re.sub(r'  +', ' ', para_text)
        result.append(para_text)
    
    # Join paragraphs with double newlines
    return '\n'.join(header_lines) + '\n\n' + '\n\n'.join(result) + '\n'

def main():
    count = 0
    for filename in sorted(os.listdir(DATA_DIR)):
        if not filename.endswith('.txt'):
            continue
        
        filepath = os.path.join(DATA_DIR, filename)
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
        
        fixed = fix_transcript(content)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(fixed)
        
        count += 1
        if count % 200 == 0:
            print(f"   Processed {count} files...")
    
    print(f"✅ Fixed whitespace in {count} transcript files")

if __name__ == '__main__':
    main()
