import csv
import json
import re
from collections import defaultdict
from pathlib import Path

SPEAKERS_JSON = Path('speakers.json')
OUT_GROUPS = Path('speakers_prefix_contamination_groups.csv')
OUT_SUGGESTIONS = Path('speakers_prefix_contamination_suggestions.csv')

MONTHS = {
    'january','february','march','april','may','june','july','august','september','october','november','december',
    'jan','feb','mar','apr','jun','jul','aug','sep','sept','oct','nov','dec'
}
WEEKDAYS = {'monday','tuesday','wednesday','thursday','friday','saturday','sunday','mon','tue','tues','wed','thu','thur','thurs','fri','sat','sun'}

# Bible book abbreviations (common)
BIBLE = {
    'gen','ex','exo','lev','num','deut','dt','josh','jos','judg','jdg','ruth','ru',
    'sam','kings','kg','chr','chron','ezra','neh','est','job','ps','psa','prov','prv','ecc','eccl','song','ss',
    'isa','jer','lam','ezek','ezk','dan','hos','joel','amos','obad','jonah','mic','nah','hab','zeph','hag','zech','mal',
    'matt','mt','mark','mk','luke','lk','john','jn','acts','ac','rom','rm','cor','co','gal','eph','phil','php','col','thess','thes','tim','tit','titus','phile','philem',
    'heb','james','jm','pet','peter','jude','rev','revelation'
}

TOPIC_TOKENS = {
    'access','clothed','decisions','discernment','emmanuel','expectations','focused','food','growth','honoring','immersed',
    'light','overcoming','peniel','perfect','press','redeemable','secure','victory','walking','wisdom',
    'service','meeting','convention','seminar','youth','banquet','retreat','concert','camp'
}

NAME_TOKEN_RE = re.compile(r"^[A-Za-zÀ-ÖØ-öø-ÿ][A-Za-zÀ-ÖØ-öø-ÿ'\-\.]*$")


def normalize_spaces(s: str) -> str:
    return re.sub(r'\s+', ' ', (s or '').strip())


def is_personish_base2(base2: str) -> bool:
    parts = base2.split()
    if len(parts) != 2:
        return False
    for p in parts:
        p_clean = p.strip(".,:;!()[]{}\"'")
        if not p_clean:
            return False
        if not NAME_TOKEN_RE.match(p_clean):
            return False
        if not p_clean[0].isupper():
            return False
    return True


def classify_suffix_token(tok: str) -> str:
    t = tok.lower().strip(".,:;!()[]{}\"'")
    if not t:
        return 'empty'
    if t in BIBLE:
        return 'bible'
    if t in MONTHS:
        return 'month'
    if t in WEEKDAYS:
        return 'weekday'
    if t in TOPIC_TOKENS:
        return 'topic'
    # short alpha tokens (often abbreviations)
    if t.isalpha() and len(t) <= 4:
        return 'short'
    return 'other'


def main() -> None:
    speakers = json.loads(SPEAKERS_JSON.read_text(encoding='utf-8'))
    speakers = [normalize_spaces(s) for s in speakers if normalize_spaces(s)]

    # Case-insensitive exact set for base existence checks
    exact_lower = {s.lower(): s for s in speakers}

    groups: dict[str, list[str]] = defaultdict(list)
    for s in speakers:
        parts = s.split()
        if len(parts) >= 3:
            base2 = ' '.join(parts[:2])
            groups[base2].append(s)

    rows = []
    for base2, items in groups.items():
        if len(items) < 4:
            continue
        if not is_personish_base2(base2):
            continue

        base_present = base2.lower() in exact_lower

        # Score: how many are likely contamination (3rd token looks like bible/month/weekday/topic/short)
        counts = defaultdict(int)
        examples = []
        for s in sorted(items)[:12]:
            examples.append(s)
        for s in items:
            parts = s.split()
            tok = parts[2] if len(parts) >= 3 else ''
            counts[classify_suffix_token(tok)] += 1

        total = len(items)
        strong = counts['bible'] + counts['month'] + counts['weekday'] + counts['topic'] + counts['short']
        strength = strong / total if total else 0.0

        # Prefer clusters where base is present OR contamination signal is strong.
        if not base_present and strength < 0.75:
            continue

        rows.append({
            'base2': base2,
            'cluster_count': total,
            'base_present_in_speakers_json': 'yes' if base_present else 'no',
            'strength': f"{strength:.2f}",
            'bible': counts['bible'],
            'month': counts['month'],
            'weekday': counts['weekday'],
            'topic': counts['topic'],
            'short': counts['short'],
            'other': counts['other'],
            'examples': ' | '.join(examples),
        })

    rows.sort(key=lambda r: (-int(r['cluster_count']), -float(r['strength']), r['base2'].lower()))

    with OUT_GROUPS.open('w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=[
            'base2','cluster_count','base_present_in_speakers_json','strength',
            'bible','month','weekday','topic','short','other','examples'
        ])
        w.writeheader()
        w.writerows(rows)

    # Generate suggested REPLACE rows (FROM -> base2) for every item in each group
    suggestions = []
    for r in rows:
        base2 = r['base2']
        for s in groups[base2]:
            # only rewrite if it actually has extra words
            if normalize_spaces(s).lower() != base2.lower():
                suggestions.append({
                    'from': s,
                    'to': base2,
                    'action': 'REPLACE',
                    'reasons': 'collapse_to_base_prefix_scan'
                })

    suggestions.sort(key=lambda x: (x['to'].lower(), x['from'].lower()))
    with OUT_SUGGESTIONS.open('w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['from','to','action','reasons'])
        w.writeheader()
        w.writerows(suggestions)

    print(f"Scanned speakers.json: {len(speakers)} entries")
    print(f"Candidate groups written: {OUT_GROUPS} ({len(rows)} groups)")
    print(f"Suggested REPLACE rows written: {OUT_SUGGESTIONS} ({len(suggestions)} rows)")


if __name__ == '__main__':
    main()
