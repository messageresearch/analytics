import csv
import glob
import os

# Path to all summary CSVs
glob_pattern = os.path.join('data', '*_Summary.csv')
summary_files = glob.glob(glob_pattern)

def clean_speaker(speaker):
    if speaker is None:
        return ''
    return speaker.replace('.timestamped', '').strip()

for csv_path in summary_files:
    print(f'Processing: {csv_path}')
    rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        for row in reader:
            row['speaker'] = clean_speaker(row.get('speaker', ''))
            rows.append(row)
    with open(csv_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
print('All summary CSVs cleaned of .timestamped in speaker columns.')
