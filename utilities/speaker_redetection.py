#!/usr/bin/env python3
"""Generate speaker review report with improved redetection heuristics."""
from __future__ import annotations

import csv
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
MASTER_PATH = DATA_DIR / "master_sermons_with_speaker_detected.csv"
OUTPUT_PATH = DATA_DIR / "speaker_detection_review.csv"
SUMMARY_GLOB = "*_Summary.csv"

BLOCK_TOKENS: Set[str] = {
    "the",
    "of",
    "and",
    "a",
    "an",
    "by",
    "to",
    "for",
    "with",
    "from",
    "into",
    "unto",
    "in",
    "on",
    "at",
    "is",
    "are",
    "be",
    "his",
    "her",
    "their",
    "our",
    "your",
    "my",
    "mine",
    "yours",
    "part",
    "pt",
    "episode",
    "lesson",
    "series",
    "sermon",
    "service",
    "meeting",
    "conference",
    "camp",
    "campmeeting",
    "retreat",
    "revival",
    "night",
        "when",
    "am",
    "pm",
    "session",
    "segment",
    "special",
    "intro",
    "opening",
    "closing",
    "ending",
    "conclusion",
    "recap",
    "highlight",
    "tribute",
    "memorial",
    "funeral",
    "wedding",
    "anniversary",
    "youth",
    "choir",
    "worship",
    "praise",
    "music",
    "song",
    "songs",
    "instrumental",
    "panel",
    "qa",
    "devotional",
    "communion",
    "baptism",
    "testimony",
    "update",
    "preview",
    "teaser",
    "announcement",
    "announcements",
    "greeting",
    "lunch",
    "dinner",
    "fellowship",
    "banquet",
    "candlelight",
    "production",
    "play",
    "scene",
    "report",
    "study",
    "bible",
    "livestream",
    "live",
    "stream",
    "streaming",
    "broadcast",
    "webcast",
    "webinar",
    "school",
    "class",
    "discussion",
    "roundtable",
    "panelist",
    "message",
    "topic",
    "theme",
    "subject",
    "continuation",
    "continuing",
    "installment",
    "bonus",
    "replay",
    "rebroadcast",
    "encore",
    "sunday",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sabbath",
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
    "evening",
    "morning",
    "afternoon",
    "online",
    "virtual",
    "hybrid",
    "podcast",
    "zoom",
    "teams",
    "meet",
    "information",
    "details",
    "overview",
    "summary",
    "outline",
    "notes",
    "devotion",
    "hymn",
    "hymns",
    "ensemble",
    "band",
    "orchestra",
    "acoustic",
    "electric",
    "rally",
    "crusade",
    "mission",
    "missions",
    "outreach",
    "guest",
    "speakers",
    "panelists",
    "series",
    "pt1",
    "pt2",
    "pt3",
    "pt4",
    "pt5",
    "part1",
    "part2",
    "part3",
    "part4",
    "part5",
    "headstone",
    "spirit",
    "word",
    "presence",
    "anointing",
    "glory",
    "faith",
    "hope",
    "love",
    "tabernacle",
    "church",
    "fellowship",
    "assembly",
    "ministries",
    "ministry",
    "center",
    "centre",
    "chapel",
    "cathedral",
    "london",
    "india",
    "africa",
    "canada",
    "usa",
    "mexico",
    "phoenix",
    "flagstaff",
    "arizona",
    "california",
    "ohio",
    "kentucky",
    "australia",
    "new",
    "zealand",
        "was",
        "met",
}

SUFFIX_REPLACEMENTS = {
    "jr": "Jr",
    "sr": "Sr",
    "ii": "II",
    "iii": "III",
    "iv": "IV",
    "v": "V",
    "vi": "VI",
    "vii": "VII",
    "viii": "VIII",
    "ix": "IX",
    "x": "X",
}

LOWERCASE_PARTICLES = {
    "de",
    "del",
    "la",
    "le",
    "van",
    "von",
    "da",
    "dos",
    "das",
    "di",
    "du",
    "st",
    "san",
    "santa",
    "al",
    "ibn",
    "bin",
    "abu",
    "el",
}

PREFIX_PATTERN = re.compile(
    r"(?i)(?:bro(?:ther)?|br\.?|sis(?:ter)?|sr|pastor|pst\.?|rev(?:erend)?|dr\.?|"  # prefixes
    r"elder|evangelist|minister|prophet|apostle|bishop|mr\.?|mrs\.?|ms\.?|prop\.?|"
    r"min\.?|evg\.?|fr\.?|father)\s+([A-Z][A-Za-z']+(?:\s+[A-Z][A-Za-z']+){0,3})"
)

BY_PATTERN = re.compile(
    r"(?i)(?:message\s+by|sermon\s+by|word\s+by|service\s+with|meeting\s+with|with)\s+"
    r"([A-Z][A-Za-z']+(?:\s+[A-Z][A-Za-z']+){0,3})"
)

DASH_PATTERN = re.compile(
    r"([A-Z][A-Za-z']+(?:\s+[A-Z][A-Za-z']+){1,3})\s*(?:-|–|—|:)"
)

MULTI_SPLIT_PATTERN = re.compile(r"\s*(?:,|&| and |/|\+|\|)\s*", re.IGNORECASE)


class NameContext:
    """Tracks known speaker tokens and casing."""

    def __init__(self) -> None:
        self.case_reference: Dict[str, str] = {}
        self.master_tokens: Set[str] = set()
        self.allowed_single: Set[str] = set()

    def add_master_tokens(self, tokens: Sequence[str]) -> None:
        if not tokens:
            return
        self.master_tokens.update(tokens)
        if len(tokens) == 1:
            self.allowed_single.add(tokens[0])
        self._ensure_case_entry(tokens)

    def add_summary_tokens(self, tokens: Sequence[str]) -> None:
        if not tokens:
            return
        self._ensure_case_entry(tokens)

    def _ensure_case_entry(self, tokens: Sequence[str]) -> None:
        if not tokens:
            return
        key = " ".join(tokens)
        if key not in self.case_reference:
            self.case_reference[key] = " ".join(format_token(tok) for tok in tokens)

    def canonicalize_tokens(self, tokens: Sequence[str]) -> str:
        if not tokens:
            return ""
        self._ensure_case_entry(tokens)
        return self.case_reference[" ".join(tokens)]

    def canonicalize_text(self, text: str) -> str:
        tokens = clean_person_tokens(text)
        if not tokens or not self.is_likely_person(tokens):
            return ""
        return self.canonicalize_tokens(tokens)

    def is_likely_person(self, tokens: Sequence[str]) -> bool:
        if not tokens:
            return False
        if len(tokens) > 4:
            return False
        if len(tokens) == 1:
            return tokens[0] in self.allowed_single
        return any(tok in self.master_tokens for tok in tokens)


def clean_person_tokens(text: str) -> List[str]:
    if not text:
        return []
    text = re.sub(r"(?i)unknown speaker", " ", text)
    text = re.sub(r"\([^)]*\)", " ", text)
    text = re.sub(r"\[[^]]*\]", " ", text)
    text = re.sub(r"[\-–—_/|]", " ", text)
    text = re.sub(r"[0-9]+", " ", text)
    text = re.sub(
        r"(?i)\b(?:bro(?:ther)?|br\.?|sis(?:ter)?|sr|pastor|pst\.?|rev(?:erend)?|dr\.?|"
        r"elder|evangelist|minister|prophet|apostle|bishop|mr\.?|mrs\.?|ms\.?|prop\.?|"
        r"min\.?|evg\.?|fr\.?|father)\b",
        " ",
        text,
    )
    text = re.sub(r"[^A-Za-z' ]", " ", text)
    tokens = [token.lower() for token in text.split()]
    tokens = [tok for tok in tokens if tok not in BLOCK_TOKENS]
    while tokens and tokens[-1] in BLOCK_TOKENS:
        tokens.pop()
    while tokens and tokens and tokens[0] in BLOCK_TOKENS:
        tokens.pop(0)
    return tokens


def format_token(token: str) -> str:
    if token in SUFFIX_REPLACEMENTS:
        return SUFFIX_REPLACEMENTS[token]
    if token.startswith("o'") and len(token) > 2:
        return "O'" + token[2:].capitalize()
    if token.startswith("mc") and len(token) > 2:
        return "Mc" + token[2:].capitalize()
    if token.startswith("mac") and len(token) > 3:
        return "Mac" + token[3:].capitalize()
    if token in LOWERCASE_PARTICLES:
        return token.capitalize()
    if "'" in token:
        return "'".join(part.capitalize() for part in token.split("'") if part)
    return token.capitalize()


def summarize_tokens_from_speaker(text: str) -> Iterable[List[str]]:
    if not text:
        return []
    parts = MULTI_SPLIT_PATTERN.split(text)
    return [clean_person_tokens(part) for part in parts if part.strip()]


def extract_candidates(text: str, context: NameContext) -> List[str]:
    if not text:
        return []
    candidates: List[str] = []
    for pattern in (PREFIX_PATTERN, BY_PATTERN, DASH_PATTERN):
        for match in pattern.findall(text):
            candidate_text = match if isinstance(match, str) else match[0]
            tokens = clean_person_tokens(candidate_text)
            if not tokens or not context.is_likely_person(tokens):
                continue
            candidates.append(context.canonicalize_tokens(tokens))
    return unique_preserve_order(candidates)


def unique_preserve_order(items: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    ordered: List[str] = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def load_master(context: NameContext) -> Dict[str, List[dict]]:
    master_by_url: Dict[str, List[dict]] = defaultdict(list)
    with MASTER_PATH.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = (row.get("videoUrl") or "").strip()
            if not url:
                continue
            master_by_url[url].append(row)
            tokens = clean_person_tokens(row.get("speaker_detected", ""))
            if tokens:
                context.add_master_tokens(tokens)
    return master_by_url


def load_summary_rows() -> List[dict]:
    rows: List[dict] = []
    for path in sorted(DATA_DIR.glob(SUMMARY_GLOB)):
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row["_source_file"] = path.name
                rows.append(row)
    return rows


def enrich_context_with_summary(rows: Sequence[dict], context: NameContext) -> None:
    for row in rows:
        for tokens in summarize_tokens_from_speaker(row.get("speaker") or ""):
            if not tokens:
                continue
            if context.is_likely_person(tokens):
                context.add_summary_tokens(tokens)


def choose_redetected(row: dict, detected_name: str, context: NameContext) -> str:
    detected = context.canonicalize_text(detected_name)
    summary_candidates = unique_preserve_order(
        context.canonicalize_tokens(tokens)
        for tokens in summarize_tokens_from_speaker(row.get("speaker") or "")
        if tokens and context.is_likely_person(tokens)
    )
    for candidate in summary_candidates:
        if candidate and candidate != detected:
            return candidate
    title_candidates = extract_candidates(row.get("title") or "", context)
    for candidate in title_candidates:
        if candidate and candidate != detected:
            return candidate
    description_candidates = extract_candidates(row.get("description") or "", context)
    for candidate in description_candidates:
        if candidate and candidate != detected:
            return candidate
    if detected:
        return detected
    tokens = clean_person_tokens(detected_name)
    if tokens:
        return context.canonicalize_tokens(tokens)
    return ""


def build_review(rows: Sequence[dict], master_by_url: Dict[str, List[dict]], context: NameContext) -> List[dict]:
    output_rows: List[dict] = []
    for row in rows:
        url = (row.get("url") or "").strip()
        if not url:
            continue
        master_rows = master_by_url.get(url)
        if not master_rows:
            continue
        detected = (master_rows[-1].get("speaker_detected") or "").strip()
        if not detected or detected == "Unknown Speaker":
            continue
        summary_speaker = (row.get("speaker") or "").strip()
        status = None
        if summary_speaker == "Unknown Speaker":
            status = "UnknownNeedsName"
        elif summary_speaker and summary_speaker != detected:
            status = "ErroneousLabel"
        if not status:
            continue
        redetected = choose_redetected(row, detected, context)
        output_rows.append(
            {
                "status": status,
                "summary_file": row.get("_source_file", ""),
                "date": row.get("date", ""),
                "title": row.get("title", ""),
                "church": row.get("church", ""),
                "summary_speaker": summary_speaker,
                "detected_speaker": detected,
                "redetected_try2": redetected,
                "video_url": url,
            }
        )
    return output_rows


def write_review(rows: Sequence[dict]) -> None:
    fieldnames = [
        "status",
        "summary_file",
        "date",
        "title",
        "church",
        "summary_speaker",
        "detected_speaker",
        "redetected_try2",
        "video_url",
    ]
    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    if not MASTER_PATH.exists():
        print(f"Missing master detection file: {MASTER_PATH}", file=sys.stderr)
        return 1
    context = NameContext()
    master_by_url = load_master(context)
    summary_rows = load_summary_rows()
    enrich_context_with_summary(summary_rows, context)
    review_rows = build_review(summary_rows, master_by_url, context)
    write_review(review_rows)
    print(f"Wrote {len(review_rows)} rows to {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
