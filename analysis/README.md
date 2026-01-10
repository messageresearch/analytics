# Analysis Scripts

Scripts for auditing and analyzing sermon data quality.

## Scripts

- **audit_speaker_accuracy.py** - Main audit script that scans for suspicious speaker names and potential missed detections
- **audit_speakers_final.py** - Final audit review tool
- **audit_speakers_json.py** - Audits the speakers.json file for errors
- **perform_deep_audit.py** - Comprehensive deep audit of the entire dataset

## Usage

Run from this directory:
```bash
cd analysis
python3 audit_speaker_accuracy.py
```

Outputs will be written to `../logs/audit/`
