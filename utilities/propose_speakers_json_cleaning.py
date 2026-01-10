import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path

SPEAKERS_JSON = Path('speakers.json')
OUT_CORRECTIONS_CSV = Path('speakers_proposed_corrections.csv')
OUT_PROPOSED_JSON = Path('speakers.proposed.cleaned.json')

# --- Heuristics ---
HONORIFIC_PREFIXES = [
    'bro', 'bro.', 'sis', 'sis.', 'dr', 'dr.', 'apostle', 'pastor', 'past', 'past.', 'pstr', 'min', 'min.'
]
HONORIFIC_SUFFIXES = set(HONORIFIC_PREFIXES)

# Words that strongly indicate this is NOT a person name (token-based; case-insensitive)
TITLE_TOKENS = {
    'service', 'meeting', 'prayer', 'worship', 'communion', 'convention', 'conference', 'camp', 'youth',
    'banquet', 'wedding', 'funeral', 'memorial', 'tribute', 'report', 'broadcast', 'livestream', 'live',
    'school', 'seminar', 'retreat', 'concert', 'anniversary', 'graduation', 'christmas', 'easter',
    'watch', 'night', 'episode', 'series', 'review', 'slideshow', 'presentation', 'combined'
}

# Allow some particles and common suffixes that are valid parts of names
NAME_PARTICLES = {'de', 'del', 'la', 'van', 'von', 'da', 'di', 'du', 'st', 'st.'}
NAME_SUFFIXES = {'jr', 'jr.', 'sr', 'sr.', 'ii', 'iii', 'iv'}
MONTHS = {
    'january','february','march','april','may','june','july','august','september','october','november','december',
    'jan','feb','mar','apr','jun','jul','aug','sep','sept','oct','nov','dec'
}

re_date1 = re.compile(r'\b\d{4}-\d{2}-\d{2}\b')
re_date2 = re.compile(r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b')


def normalize_spaces(s: str) -> str:
    return re.sub(r'\s+', ' ', (s or '').strip())


def split_tokens(s: str) -> list[str]:
    return [t for t in normalize_spaces(s).split(' ') if t]


def strip_honorific_prefix(s: str) -> tuple[str, bool]:
    changed = False
    out = normalize_spaces(s)
    while True:
        low = out.lower()
        matched = False
        for h in HONORIFIC_PREFIXES:
            prefix = h + ' '
            if low.startswith(prefix):
                out = normalize_spaces(out[len(prefix):])
                changed = True
                matched = True
                break
        if not matched:
            break
    return out, changed


def strip_honorific_suffix(s: str) -> tuple[str, bool]:
    t = split_tokens(s)
    if not t:
        return '', False
    last = t[-1].lower().strip('.,:;!')
    if last in HONORIFIC_SUFFIXES:
        return normalize_spaces(' '.join(t[:-1])), True
    return s, False


def contains_date_or_digits(s: str) -> bool:
    return bool(re_date1.search(s) or re_date2.search(s) or any(ch.isdigit() for ch in s))


def is_initial_token(tok: str) -> bool:
    # e.g. "A." or "J."
    return bool(re.fullmatch(r"[A-Za-z]\.", tok))


def looks_like_person_name(s: str) -> bool:
    # Accept 2-4 tokens; allow particles/initials/suffixes.
    t = split_tokens(s)
    if len(t) < 2 or len(t) > 4:
        return False

    # Reject if any token is clearly an event/title token (except allowed particles)
    low_tokens = [x.lower().strip('.,:;!()[]{}') for x in t]
    for lt in low_tokens:
        if lt in TITLE_TOKENS and lt not in NAME_PARTICLES:
            return False

    # Require at least 2 tokens that look like name words (start with letter)
    nameish = 0
    for tok in t:
        clean = tok.strip('.,:;!()[]{}')
        if is_initial_token(clean):
            nameish += 1
            continue
        if re.match(r"^[A-Za-zÀ-ÖØ-öø-ÿ]", clean):
            nameish += 1
    return nameish >= 2


def has_title_tokens(s: str) -> bool:
    low_tokens = [x.lower().strip('.,:;!()[]{}') for x in split_tokens(s)]
    return any(t in TITLE_TOKENS for t in low_tokens)


@dataclass
class Proposal:
    from_name: str
    to_name: str
    action: str  # REPLACE | REMOVE | KEEP
    reasons: str


def build_base_names(names: list[str]) -> set[str]:
    # Base names are person-like, 2-3 tokens.
    out = set()
    for n in names:
        t = split_tokens(n)
        if 2 <= len(t) <= 3 and looks_like_person_name(n):
            out.add(n)
    return out


def find_longest_base_prefix(name: str, base_names: set[str]) -> str | None:
    # If "Chad Lamb Access" and "Chad Lamb" exists, return "Chad Lamb".
    # Try longest prefix first.
    t = split_tokens(name)
    for k in range(min(4, len(t)-1), 1, -1):
        cand = ' '.join(t[:k])
        if cand in base_names:
            return cand
    return None


def propose_for_name(name: str, base_names: set[str]) -> Proposal:
    original = normalize_spaces(name)
    current = original
    reasons = []

    # 1) Remove embedded .timestamped anywhere (should be gone already, but safe)
    if '.timestamped' in current:
        current = normalize_spaces(current.replace('.timestamped', ''))
        reasons.append('remove_timestamped')

    # 2) Strip honorific prefix/suffix
    current, changed_prefix = strip_honorific_prefix(current)
    if changed_prefix:
        reasons.append('strip_honorific_prefix')

    current, changed_suffix = strip_honorific_suffix(current)
    if changed_suffix:
        reasons.append('strip_honorific_suffix')

    # 3) Prefix contamination (title appended)
    prefix = find_longest_base_prefix(current, base_names)
    if prefix and prefix != current:
        # If remainder looks like a title keyword/month/etc, prefer collapsing to prefix
        remainder_tokens = split_tokens(current)[len(split_tokens(prefix)):]
        remainder_low = [t.lower().strip('.,:;!()[]{}') for t in remainder_tokens]
        if any(t in TITLE_TOKENS or t in MONTHS for t in remainder_low) or len(remainder_tokens) >= 1:
            current = prefix
            reasons.append('collapse_to_base_prefix')

    # Decide action
    if not current:
        return Proposal(original, '', 'REMOVE', ';'.join(reasons + ['empty_after_clean']))

    # If it still looks like an event/title and not a person, remove
    if has_title_tokens(current) and not looks_like_person_name(current):
        return Proposal(original, '', 'REMOVE', ';'.join(reasons + ['contains_title_tokens']))

    # If it contains digits/dates and doesn't look like a person, remove
    if contains_date_or_digits(current) and not looks_like_person_name(current):
        return Proposal(original, '', 'REMOVE', ';'.join(reasons + ['contains_digits_or_date']))

    # If it is a single token after cleaning, it's usually ambiguous (e.g., "Bro Barry" -> "Barry")
    if len(split_tokens(current)) == 1 and current not in base_names:
        # Keep it only if the original was already single-token (don’t introduce new ambiguous names)
        if len(split_tokens(original)) > 1:
            return Proposal(original, '', 'REMOVE', ';'.join(reasons + ['single_token_after_clean']))

    if current != original:
        return Proposal(original, current, 'REPLACE', ';'.join(reasons) or 'normalized')

    return Proposal(original, original, 'KEEP', '')


def main() -> None:
    speakers = json.loads(SPEAKERS_JSON.read_text(encoding='utf-8'))
    speakers = [normalize_spaces(s) for s in speakers]

    base_names = build_base_names(speakers)

    proposals: list[Proposal] = []
    for name in speakers:
        proposals.append(propose_for_name(name, base_names))

    corrections = [p for p in proposals if p.action in {'REPLACE', 'REMOVE'}]

    # Build proposed cleaned list
    cleaned: list[str] = []
    for p in proposals:
        if p.action == 'REMOVE':
            continue
        cleaned.append(p.to_name)

    # Dedupe + sort (keep stable output)
    cleaned_unique = sorted(set(cleaned), key=lambda x: x.lower())

    # Write FROM/TO corrections CSV
    with OUT_CORRECTIONS_CSV.open('w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['from', 'to', 'action', 'reasons'])
        w.writeheader()
        for p in sorted(corrections, key=lambda x: (x.action, x.from_name.lower())):
            w.writerow({'from': p.from_name, 'to': p.to_name, 'action': p.action, 'reasons': p.reasons})

    # Write proposed cleaned JSON
    OUT_PROPOSED_JSON.write_text(
        json.dumps(cleaned_unique, ensure_ascii=False, indent=2) + '\n',
        encoding='utf-8'
    )

    removed = sum(1 for p in proposals if p.action == 'REMOVE')
    replaced = sum(1 for p in proposals if p.action == 'REPLACE')

    print(f"Input speakers: {len(speakers)}")
    print(f"Proposed removals: {removed}")
    print(f"Proposed replacements: {replaced}")
    print(f"Output speakers (unique): {len(cleaned_unique)}")
    print(f"Wrote: {OUT_CORRECTIONS_CSV}")
    print(f"Wrote: {OUT_PROPOSED_JSON}")


if __name__ == '__main__':
    main()
