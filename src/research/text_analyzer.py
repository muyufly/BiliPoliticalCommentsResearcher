"""
Single-text semantic cleaning, fuzzy lexicon matching, and stance analysis.

This module is intentionally independent from crawler and batch summary flows.
It reads the existing manual lexicons, analyzes user-provided text, and writes
standalone outputs under output/text_analysis_*.
"""
from __future__ import annotations

import csv
import json
import re
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from uuid import uuid4
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from config.config import CSV_ENCODING
from src.research.analysis import normalize_text, tokenize
from src.research.ai_analyzer import run_structured_ai_json
from src.research.deep_cleaning import (
    build_effective_term_map,
    build_stance_labels,
    load_manual_lexicon_overrides,
)
from src.research.political_axis import score_comment
from src.research.reports import markdown_to_html

try:
    from pypinyin import lazy_pinyin
except Exception:  # pragma: no cover - optional dependency fallback
    lazy_pinyin = None


TEXT_ANALYSIS_ROOT = Path("output")
PROJECT_ROOT = Path(__file__).resolve().parents[2]
MANUAL_LEXICON_DIR = PROJECT_ROOT / "config" / "manual_lexicons"
SEMANTIC_OVERRIDES_PATH = MANUAL_LEXICON_DIR / "semantic_overrides.json"
STANCE_OVERRIDES_PATH = MANUAL_LEXICON_DIR / "stance_overrides.json"
COMPOSITE_OVERRIDES_PATH = MANUAL_LEXICON_DIR / "composite_overrides.json"
LEXICON_CANDIDATES_PATH = MANUAL_LEXICON_DIR / "lexicon_candidates.json"
FUZZY_MAX_TEXT_CHARS = 8000
MAX_FUZZY_MATCHES_PER_SEGMENT = 80
STANCE_ORDER_TEXT = ["神", "左", "兔", "皇", "乐子人"]

DEFAULT_COMPOSITE_OVERRIDES = {
    "rules": [
        {
            "rule_id": "left_world_people_grand_unity",
            "terms": ["世界", "人民", "大团结"],
            "window": "same_segment",
            "semantic_category": "政治路线",
            "meaning_label": "国际主义动员",
            "semantic_weight": 4.2,
            "stance": "左",
            "stance_score": 4.0,
            "confidence": 0.98,
            "reason": "世界/人民/大团结共现时按左派国际主义口号处理",
            "enabled": True,
        },
        {
            "rule_id": "left_world_people_unite",
            "terms": ["世界", "人民", "联合起来"],
            "window": "adjacent_segments",
            "semantic_category": "政治路线",
            "meaning_label": "国际主义动员",
            "semantic_weight": 4.3,
            "stance": "左",
            "stance_score": 4.1,
            "confidence": 0.98,
            "reason": "世界/人民/联合起来共现时按左派国际主义口号处理",
            "enabled": True,
        },
        {
            "rule_id": "left_world_proletarian_unity",
            "terms": ["世界", "无产者", "联合起来"],
            "window": "adjacent_segments",
            "semantic_category": "政治路线",
            "meaning_label": "国际主义动员",
            "semantic_weight": 4.5,
            "stance": "左",
            "stance_score": 4.2,
            "confidence": 0.99,
            "reason": "世界/无产者/联合起来共现时按国际主义动员处理",
            "enabled": True,
        },
        {
            "rule_id": "mao_poem_four_seas_five_continents",
            "terms": ["四海", "五洲", "风雷"],
            "window": "adjacent_segments",
            "semantic_category": "政治路线",
            "meaning_label": "毛泽东诗词政治隐喻",
            "semantic_weight": 4.0,
            "stance": "左",
            "stance_score": 4.0,
            "confidence": 0.97,
            "reason": "四海/五洲/风雷共现时按毛泽东《满江红·和郭沫若同志》拆分互文处理",
            "enabled": True,
        }
    ]
}

DEFAULT_LEXICON_CANDIDATES = {"candidates": []}

AMBIGUOUS_ALIAS_CONTEXTS: Dict[str, List[str]] = {
    "猫": ["教员", "导师", "主席", "毛", "红太阳", "老人家", "太祖", "换了人间", "人民万岁", "文革", "左"],
    "毛": ["教员", "导师", "主席", "红太阳", "老人家", "太祖", "换了人间", "人民万岁", "文革", "左"],
    "稻": ["邓", "邓公", "改革", "改开", "市场", "南巡", "黑猫白猫", "摸着石头", "司马懿"],
    "小王": ["王洪文", "四人帮", "接班人", "上海", "工总司", "文革", "张春桥", "江青", "姚文元"],
    "年轻人": ["王洪文", "四人帮", "接班人", "上海", "工总司", "文革", "张春桥", "江青", "姚文元"],
    "小崔": ["王洪文", "四人帮", "接班人", "上海", "工总司", "文革", "张春桥", "江青", "姚文元"],
    "发展": ["中国", "国家", "大国", "强国", "复兴", "特色", "改开", "改革开放", "现代化", "中国制造", "稳定"],
    "崛起": ["中国", "国家", "大国", "强国", "复兴", "民族复兴", "中华", "东升西降", "国运"],
    "民族主义": ["皇汉", "华夷", "汉家", "汉统", "满清", "反清复明", "驱逐鞑虏", "尊王攘夷", "大一统", "正统", "华夏"],
    "加速": ["神友", "浪人", "冲浪里", "神奈冲", "神奈川冲浪里", "何意味", "总加速师", "加速主义"],
}


def create_text_analysis_output_dir(base_dir: Path = TEXT_ANALYSIS_ROOT) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = base_dir / f"text_analysis_{timestamp}_{uuid4().hex[:6]}"
    (output_dir / "data").mkdir(parents=True, exist_ok=True)
    return output_dir


def _read_json(path: Path, default: Dict[str, object]) -> Dict[str, object]:
    if not path.exists():
        return json.loads(json.dumps(default, ensure_ascii=False))
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return json.loads(json.dumps(default, ensure_ascii=False))
    return data if isinstance(data, dict) else json.loads(json.dumps(default, ensure_ascii=False))


def _write_json(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _ensure_lexicon_extension_files() -> None:
    MANUAL_LEXICON_DIR.mkdir(parents=True, exist_ok=True)
    if not COMPOSITE_OVERRIDES_PATH.exists():
        _write_json(COMPOSITE_OVERRIDES_PATH, DEFAULT_COMPOSITE_OVERRIDES)
    if not LEXICON_CANDIDATES_PATH.exists():
        _write_json(LEXICON_CANDIDATES_PATH, DEFAULT_LEXICON_CANDIDATES)


def load_composite_overrides() -> Dict[str, object]:
    _ensure_lexicon_extension_files()
    data = _read_json(COMPOSITE_OVERRIDES_PATH, DEFAULT_COMPOSITE_OVERRIDES)
    data.setdefault("rules", [])
    return data


def load_lexicon_candidates() -> Dict[str, object]:
    _ensure_lexicon_extension_files()
    data = _read_json(LEXICON_CANDIDATES_PATH, DEFAULT_LEXICON_CANDIDATES)
    data.setdefault("candidates", [])
    return data


def _compact_text(text: str) -> str:
    text = unicodedata.normalize("NFKC", str(text or "")).lower()
    return re.sub(r"[^\w\u4e00-\u9fff]+", "", text)


def _split_text(text: str, max_segment_chars: int = 260) -> List[str]:
    cleaned = normalize_text(text)
    raw_parts = re.split(r"[\n\r]+|(?<=[。！？!?；;])", cleaned)
    segments: List[str] = []
    for part in raw_parts:
        part = part.strip()
        if not part:
            continue
        while len(part) > max_segment_chars:
            segments.append(part[:max_segment_chars].strip())
            part = part[max_segment_chars:].strip()
        if part:
            segments.append(part)
    return segments or ([cleaned] if cleaned else [])


def _pinyin_key(text: str) -> str:
    if not lazy_pinyin:
        return ""
    return " ".join(lazy_pinyin(text, errors="ignore"))


def _term_metadata(
    effective_term_map: Dict[str, List[Dict[str, object]]],
    manual_stance: Dict[str, object],
) -> Dict[str, Dict[str, object]]:
    terms: Dict[str, Dict[str, object]] = {}
    for term, meanings in effective_term_map.items():
        if not term or not meanings:
            continue
        best = max(meanings, key=lambda item: float(item.get("weight", 1.0) or 1.0))
        terms[term] = {
            "term": term,
            "semantic_category": best.get("category", ""),
            "meaning_label": best.get("meaning", ""),
            "semantic_weight": float(best.get("weight", 1.0) or 1.0),
            "context_required": all(bool(item.get("context_required", False)) for item in meanings),
            "context_terms": sorted({
                str(context).strip()
                for item in meanings
                for context in item.get("context_terms", []) or []
                if str(context).strip()
            }),
            "sources": sorted({str(item.get("source", "seed")) for item in meanings}),
        }

    for item in manual_stance.get("term_overrides", []):
        if not isinstance(item, dict):
            continue
        term = str(item.get("term", "")).strip()
        if not term:
            continue
        terms.setdefault(term, {
            "term": term,
            "semantic_category": "",
            "meaning_label": "",
            "semantic_weight": 1.5,
            "sources": [],
        })
        terms[term]["stance"] = str(item.get("stance", "")).strip()
        terms[term]["stance_score"] = float(item.get("score", 2.0) or 2.0)
        if bool(item.get("context_required", False)):
            terms[term]["context_required"] = True
        item_contexts = [
            str(context).strip()
            for context in item.get("context_terms", []) or []
            if str(context).strip()
        ]
        if item_contexts:
            merged_contexts = set(terms[term].get("context_terms", []) or [])
            merged_contexts.update(item_contexts)
            terms[term]["context_terms"] = sorted(merged_contexts)
    return terms


def _iter_windows(text: str, size: int) -> Iterable[str]:
    if size <= 0 or len(text) < size:
        return []
    seen = set()
    values = []
    for idx in range(0, len(text) - size + 1):
        window = text[idx:idx + size]
        if window and window not in seen:
            seen.add(window)
            values.append(window)
    return values


def _match_term_in_segment(
    segment: str,
    term: str,
    enable_fuzzy: bool,
    pinyin_cache: Dict[str, str],
    force_review: bool = False,
    context_terms: Optional[List[str]] = None,
) -> List[Dict[str, object]]:
    compact_segment = _compact_text(segment)
    compact_term = _compact_text(term)
    if not compact_segment or not compact_term:
        return []

    matches: List[Dict[str, object]] = []
    if compact_term in compact_segment:
        ambiguous = _ambiguous_without_context(
            segment,
            term,
            force_review=force_review,
            context_terms=context_terms,
        )
        matches.append({
            "matched_text": term,
            "match_method": "exact",
            "confidence": 0.62 if ambiguous else 1.0,
            "needs_review": ambiguous,
            "reason": "context-required term needs composite evidence" if force_review else (
                "ambiguous alias without context" if ambiguous else "exact substring"
            ),
        })
        return matches

    if not enable_fuzzy or len(compact_term) < 2:
        return []

    term_len = len(compact_term)
    term_pinyin = pinyin_cache.setdefault(compact_term, _pinyin_key(compact_term))
    best_by_method: Dict[str, Dict[str, object]] = {}

    for window in _iter_windows(compact_segment[:FUZZY_MAX_TEXT_CHARS], term_len):
        if window == compact_term:
            continue
        ratio = SequenceMatcher(None, compact_term, window).ratio()

        if lazy_pinyin and term_pinyin:
            window_pinyin = pinyin_cache.setdefault(window, _pinyin_key(window))
            if window_pinyin == term_pinyin:
                confidence = min(0.96, max(0.84, 0.84 + ratio * 0.12))
                best_by_method["homophone"] = max(
                    [best_by_method.get("homophone", {}), {
                        "matched_text": window,
                        "match_method": "homophone",
                        "confidence": round(confidence, 4),
                        "needs_review": term_len <= 2 or confidence < 0.9,
                        "reason": f"same pinyin; char_similarity={ratio:.2f}",
                    }],
                    key=lambda item: float(item.get("confidence", 0) or 0),
                )

        edit_threshold = 0.86 if term_len >= 4 else 0.9
        if term_len >= 3 and ratio >= edit_threshold:
            confidence = min(0.93, ratio)
            best_by_method["near_shape_or_typo"] = max(
                [best_by_method.get("near_shape_or_typo", {}), {
                    "matched_text": window,
                    "match_method": "near_shape_or_typo",
                    "confidence": round(confidence, 4),
                    "needs_review": confidence < 0.9,
                    "reason": f"edit similarity={ratio:.2f}",
                }],
                key=lambda item: float(item.get("confidence", 0) or 0),
            )

    matches.extend(best_by_method.values())
    return sorted(matches, key=lambda item: float(item["confidence"]), reverse=True)[:2]


def _ambiguous_without_context(
    segment: str,
    term: str,
    force_review: bool = False,
    context_terms: Optional[List[str]] = None,
) -> bool:
    contexts = list(AMBIGUOUS_ALIAS_CONTEXTS.get(term, []))
    contexts.extend(
        str(context).strip()
        for context in (context_terms or [])
        if str(context).strip()
    )
    if not contexts:
        return bool(force_review)
    compact_segment = _compact_text(segment)
    has_context = any(_compact_text(context) in compact_segment for context in contexts if context != term)
    return bool(force_review and not has_context) or not has_context


def fuzzy_match_text(
    text: str,
    term_map: Dict[str, Dict[str, object]],
    enable_fuzzy: bool = True,
) -> List[Dict[str, object]]:
    segments = _split_text(text)
    pinyin_cache: Dict[str, str] = {}
    rows: List[Dict[str, object]] = []
    terms = sorted(term_map.keys(), key=len, reverse=True)

    for idx, segment in enumerate(segments, start=1):
        segment_rows: List[Dict[str, object]] = []
        for term in terms:
            if len(_compact_text(term)) > max(2, len(_compact_text(segment))):
                continue
            meta = term_map[term]
            for match in _match_term_in_segment(
                segment,
                term,
                enable_fuzzy,
                pinyin_cache,
                force_review=bool(meta.get("context_required", False)),
                context_terms=meta.get("context_terms", []),
            ):
                row = {
                    "segment_id": f"text_{idx:03d}",
                    "segment": segment,
                    "canonical_term": term,
                    "matched_text": match["matched_text"],
                    "match_method": match["match_method"],
                    "confidence": match["confidence"],
                    "needs_review": match["needs_review"],
                    "semantic_category": meta.get("semantic_category", ""),
                    "meaning_label": meta.get("meaning_label", ""),
                    "semantic_weight": meta.get("semantic_weight", 1.0),
                    "stance": meta.get("stance", ""),
                    "stance_score": meta.get("stance_score", 0.0),
                    "reason": match["reason"],
                }
                segment_rows.append(row)

        segment_rows.sort(key=lambda row: (
            str(row["match_method"]) != "exact",
            -float(row["confidence"]),
            -float(row["semantic_weight"]),
            str(row["canonical_term"]),
        ))
        rows.extend(segment_rows[:MAX_FUZZY_MATCHES_PER_SEGMENT])
    return rows


def _composite_match_text(text: str, segments: List[str], composite_overrides: Dict[str, object]) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    seen = set()
    full_compact = _compact_text(text)
    rules = composite_overrides.get("rules", [])
    if not isinstance(rules, list):
        return rows

    for rule in rules:
        if not isinstance(rule, dict) or rule.get("enabled") is False:
            continue
        terms = [str(term).strip() for term in rule.get("terms", []) if str(term).strip()]
        if len(terms) < 2:
            continue

        window = str(rule.get("window", "adjacent_segments")).strip() or "adjacent_segments"
        compact_terms = [_compact_text(term) for term in terms if _compact_text(term)]
        if len(compact_terms) != len(terms):
            continue

        windows: List[Tuple[str, str, str]] = []
        if window == "full_text":
            windows = [("text_all", text, full_compact)]
        else:
            for idx, segment in enumerate(segments, start=1):
                if window == "same_segment":
                    candidate = segment
                else:
                    neighbor = segments[idx] if idx < len(segments) else ""
                    candidate = f"{segment}\n{neighbor}".strip()
                windows.append((f"text_{idx:03d}", candidate, _compact_text(candidate)))

        for segment_id, raw_window, compact_window in windows:
            if not compact_window or not all(term in compact_window for term in compact_terms):
                continue
            rule_id = str(rule.get("rule_id") or "+".join(terms))
            key = (rule_id, segment_id)
            if key in seen:
                continue
            seen.add(key)
            rows.append({
                "segment_id": segment_id,
                "segment": raw_window,
                "canonical_term": " + ".join(terms),
                "matched_text": " / ".join(terms),
                "match_method": f"composite_{window}",
                "confidence": round(float(rule.get("confidence", 0.95) or 0.95), 4),
                "needs_review": bool(rule.get("needs_review", False)),
                "semantic_category": str(rule.get("semantic_category", "")),
                "meaning_label": str(rule.get("meaning_label", "")),
                "semantic_weight": float(rule.get("semantic_weight", 3.0) or 3.0),
                "stance": str(rule.get("stance", "")),
                "stance_score": float(rule.get("stance_score", 2.5) or 2.5),
                "reason": str(rule.get("reason", "composite rule")),
                "composite_rule_id": rule_id,
            })
    return rows


def _build_axis_row(comment_id: str, content: str) -> Tuple[Dict[str, object], List[Dict[str, object]]]:
    scores, hits = score_comment(content)
    total_score = round(sum(poles["left"] + poles["right"] for poles in scores.values()), 4)
    row: Dict[str, object] = {
        "comment_id": comment_id,
        "song_key": "uploaded_text",
        "song_name": "uploaded_text",
        "bvid": "",
        "content": content,
        "political_total_score": total_score,
    }
    for dimension, poles in scores.items():
        left_score = float(poles.get("left", 0) or 0)
        right_score = float(poles.get("right", 0) or 0)
        row[f"{dimension}_left_score"] = round(left_score, 4)
        row[f"{dimension}_right_score"] = round(right_score, 4)
        row[f"{dimension}_net_score"] = round(left_score - right_score, 4)
    return row, hits


def _meaning_candidates_from_matches(matches: List[Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    candidates: Dict[str, Dict[str, object]] = {}
    for match in matches:
        if bool(match.get("needs_review")) and float(match.get("confidence", 0) or 0) < 0.75:
            continue
        label = str(match.get("meaning_label", "")).strip()
        if not label:
            continue
        item = candidates.setdefault(label, {
            "meaning_category": str(match.get("semantic_category", "")).strip(),
            "score": 0.0,
            "matched_terms": set(),
        })
        item["score"] += (
            float(match.get("semantic_weight", 1.0) or 1.0)
            * float(match.get("confidence", 0.0) or 0.0)
        )
        item["matched_terms"].add(str(match.get("canonical_term", "")).strip())
    return candidates


def _build_meaning_rows(
    segments: List[str],
    matches: List[Dict[str, object]],
    axis_rows: List[Dict[str, object]],
) -> pd.DataFrame:
    matches_by_segment: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for match in matches:
        matches_by_segment[str(match["segment_id"])].append(match)

    rows = []
    axis_by_id = {str(row["comment_id"]): row for row in axis_rows}
    for idx, segment in enumerate(segments, start=1):
        segment_id = f"text_{idx:03d}"
        candidates = _meaning_candidates_from_matches(matches_by_segment.get(segment_id, []))
        candidate_rows = [
            {
                "meaning_label": label,
                "meaning_category": info["meaning_category"],
                "score": round(float(info["score"]), 4),
                "matched_terms": ",".join(sorted(info["matched_terms"])),
            }
            for label, info in candidates.items()
            if float(info["score"]) > 0
        ]
        candidate_rows.sort(key=lambda item: item["score"], reverse=True)
        primary = candidate_rows[0] if candidate_rows else None
        secondary = candidate_rows[1:3] if len(candidate_rows) > 1 else []
        political_total_score = float(axis_by_id.get(segment_id, {}).get("political_total_score", 0) or 0)
        fuzzy_score = sum(float(item.get("semantic_weight", 0) or 0) * float(item.get("confidence", 0) or 0)
                          for item in matches_by_segment.get(segment_id, []))
        valid_political = bool(primary) or political_total_score + fuzzy_score >= 2
        rows.append({
            "comment_id": segment_id,
            "song_key": "uploaded_text",
            "song_name": "uploaded_text",
            "bvid": "",
            "content": segment,
            "is_political_historical": valid_political,
            "primary_meaning_category": primary["meaning_category"] if primary else "",
            "primary_meaning_label": primary["meaning_label"] if primary else "",
            "primary_meaning_score": primary["score"] if primary else 0,
            "primary_matched_terms": primary["matched_terms"] if primary else "",
            "secondary_meaning_labels": ",".join(item["meaning_label"] for item in secondary),
            "secondary_matched_terms": ",".join(item["matched_terms"] for item in secondary if item["matched_terms"]),
            "candidate_count": len(candidate_rows),
            "political_total_score": round(political_total_score + fuzzy_score, 4),
        })
    return pd.DataFrame(rows)


def _apply_fuzzy_stance_boost(stance_df: pd.DataFrame, matches: List[Dict[str, object]]) -> pd.DataFrame:
    if stance_df.empty:
        return stance_df
    matches_by_segment: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for match in matches:
        matches_by_segment[str(match["segment_id"])].append(match)

    rows = []
    for _, row in stance_df.iterrows():
        record = row.to_dict()
        segment_matches = matches_by_segment.get(str(record.get("comment_id", "")), [])
        stance_scores = Counter()
        for match in segment_matches:
            if bool(match.get("needs_review")) and float(match.get("confidence", 0) or 0) < 0.75:
                continue
            stance = str(match.get("stance", "")).strip()
            if not stance:
                continue
            stance_scores[stance] += (
                float(match.get("stance_score", 0) or 0)
                * float(match.get("confidence", 0) or 0)
            )
        if stance_scores:
            best_stance, best_score = stance_scores.most_common(1)[0]
            if best_stance and (
                record.get("stance") == "乐子人"
                or float(best_score) >= max(1.8, float(record.get("stance_confidence", 0) or 0) * 3)
            ):
                record["stance"] = best_stance
                record["stance_confidence"] = round(min(0.98, float(best_score) / (float(best_score) + 1.5)), 4)
                record["stance_reason"] = f"fuzzy_boost:{best_stance}={best_score:.2f}; {record.get('stance_reason', '')}"
        rows.append(record)
    return pd.DataFrame(rows)


def _remove_uncertain_manual_alias_stance(stance_df: pd.DataFrame, matches: List[Dict[str, object]]) -> pd.DataFrame:
    if stance_df.empty:
        return stance_df
    uncertain_by_segment = defaultdict(set)
    for match in matches:
        term = str(match.get("canonical_term", "")).strip()
        if term in AMBIGUOUS_ALIAS_CONTEXTS and bool(match.get("needs_review")):
            uncertain_by_segment[str(match.get("segment_id", ""))].add(term)

    rows = []
    for _, row in stance_df.iterrows():
        record = row.to_dict()
        uncertain_terms = uncertain_by_segment.get(str(record.get("comment_id", "")), set())
        reason = str(record.get("stance_reason", ""))
        matched_keywords = str(record.get("matched_keywords", ""))
        if uncertain_terms and any(
            f"manual:{term}" in reason or f"manual:{term}" in matched_keywords
            for term in uncertain_terms
        ):
            record["stance"] = "乐子人"
            record["stance_confidence"] = 0.0
            record["stance_reason"] = f"ambiguous_alias_review:{','.join(sorted(uncertain_terms))}; {reason}"
        rows.append(record)
    return pd.DataFrame(rows)


def _distribution(values: Iterable[str], order: Optional[List[str]] = None) -> List[Dict[str, object]]:
    counter = Counter(value for value in values if str(value).strip())
    if order:
        rank = {key: idx for idx, key in enumerate(order)}
        keys = sorted(counter, key=lambda key: (-counter[key], rank.get(key, len(order))))
    else:
        keys = [key for key, _ in counter.most_common()]
    total = sum(counter.values())
    return [
        {
            "label": key,
            "count": int(counter[key]),
            "share": round(counter[key] / total, 6) if total else 0,
            "share_pct": round(counter[key] / total * 100, 2) if total else 0,
        }
        for key in keys
    ]


def _accuracy_summary(matches_df: pd.DataFrame) -> Dict[str, object]:
    if matches_df.empty:
        return {
            "match_count": 0,
            "exact_count": 0,
            "fuzzy_count": 0,
            "needs_review_count": 0,
            "estimated_precision": 0.0,
            "note": "未命中词库。",
        }
    exact_count = int((matches_df["match_method"] == "exact").sum())
    fuzzy = matches_df[matches_df["match_method"] != "exact"].copy()
    needs_review_count = int(matches_df["needs_review"].fillna(False).sum())
    estimated_precision = float(matches_df["confidence"].astype(float).mean())
    return {
        "match_count": int(len(matches_df)),
        "exact_count": exact_count,
        "fuzzy_count": int(len(fuzzy)),
        "needs_review_count": needs_review_count,
        "estimated_precision": round(estimated_precision, 4),
        "note": "estimated_precision 是规则置信度均值，不等同人工标注准确率；needs_review 应人工抽查。",
    }


def _ai_full_text_review(
    text: str,
    matches_df: pd.DataFrame,
    meaning_df: pd.DataFrame,
    stance_df: pd.DataFrame,
    enabled: bool,
) -> Dict[str, object]:
    if not enabled:
        return {}

    evidence_matches = []
    if not matches_df.empty:
        evidence_matches = matches_df.sort_values(
            ["confidence", "semantic_weight"], ascending=False
        ).head(60).to_dict("records")

    prompt = {
        "task": "对一段古风/DJ歌词或评论文本做全文政治历史隐喻理解，并给出单主立场加权建议。",
        "stance_definitions": {
            "神": "自由派/反威权/奥威尔式批判语境",
            "左": "传统左、新左、毛时代符号、群众动员、国际主义、社会主义/苏联意象语境",
            "兔": "建制支持、改开、特色、复兴、国家发展正当性语境",
            "皇": "民族主义、皇汉、保守秩序、王朝正统语境",
            "乐子人": "无明显政治立场、玩梗、审美表达或证据不足",
        },
        "coding_hints": [
            "重点识别整首文本的互文结构，而不是只看单个词。",
            "四海、五洲、翻腾、风雷若组合出现，应优先考虑毛泽东《满江红·和郭沫若同志》“四海翻腾云水怒，五洲震荡风雷激”的拆分引用。",
            "克里姆林宫、集体活动、褪色的口号、群众/标语/文艺汇演等可作为左翼/社会主义阵营语境辅助证据。",
            "短词代称如猫、毛、稻、小王若缺上下文，不要单独作为强证据。",
            "输出严格 JSON 对象，不要 Markdown。",
        ],
        "local_rule_summary": {
            "matches": evidence_matches,
            "meaning_rows": meaning_df.head(20).to_dict("records") if not meaning_df.empty else [],
            "stance_rows": stance_df.head(20).to_dict("records") if not stance_df.empty else [],
        },
        "text": str(text or "")[:6000],
        "output_schema": {
            "stance": "神|左|兔|皇|乐子人",
            "confidence": "0-1 number",
            "meaning_labels": ["1-5 个真正含义标签"],
            "evidence_terms": ["关键证据词或短句"],
            "reason": "简短中文说明，说明全文互文关系",
        },
    }
    response = run_structured_ai_json(prompt, enabled=True)
    if not isinstance(response, dict):
        return {}
    stance = str(response.get("stance", "")).strip()
    if stance not in STANCE_ORDER_TEXT:
        stance = "乐子人"
    try:
        confidence = float(response.get("confidence", 0) or 0)
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    return {
        "stance": stance,
        "confidence": round(confidence, 4),
        "meaning_labels": response.get("meaning_labels", []) if isinstance(response.get("meaning_labels", []), list) else [],
        "evidence_terms": response.get("evidence_terms", []) if isinstance(response.get("evidence_terms", []), list) else [],
        "reason": str(response.get("reason", "")).strip(),
    }


def _apply_ai_stance_weight(
    rule_distribution: List[Dict[str, object]],
    ai_review: Dict[str, object],
    segment_count: int,
) -> List[Dict[str, object]]:
    scores = Counter()
    for item in rule_distribution:
        scores[str(item.get("label", ""))] += float(item.get("count", 0) or 0)

    stance = str(ai_review.get("stance", "")).strip()
    confidence = float(ai_review.get("confidence", 0) or 0)
    if stance in STANCE_ORDER_TEXT and confidence >= 0.55:
        scores[stance] += max(2.0, float(segment_count) * 1.5) * confidence

    total = sum(scores.values())
    rank = {key: idx for idx, key in enumerate(STANCE_ORDER_TEXT)}
    rows = []
    for key in sorted(scores, key=lambda value: (-scores[value], rank.get(value, len(rank)))):
        rows.append({
            "label": key,
            "count": round(float(scores[key]), 4),
            "share": round(float(scores[key]) / total, 6) if total else 0,
            "share_pct": round(float(scores[key]) / total * 100, 2) if total else 0,
        })
    return rows


def _default_candidate_from_analysis(
    text: str,
    expected_stance: str,
    analysis_summary: Dict[str, object],
) -> List[Dict[str, object]]:
    evidence_terms = []
    ai_review = analysis_summary.get("ai_fulltext_review", {}) if isinstance(analysis_summary, dict) else {}
    if isinstance(ai_review, dict):
        evidence_terms = [str(item).strip() for item in ai_review.get("evidence_terms", []) if str(item).strip()]
    if not evidence_terms:
        tokens = [token for token in tokenize(text) if len(token) >= 2]
        evidence_terms = [term for term, _ in Counter(tokens).most_common(8)]

    candidate_terms = evidence_terms[:6] or [str(text or "")[:20].strip()]
    return [{
        "candidate_type": "composite" if len(candidate_terms) >= 2 else "term",
        "terms": candidate_terms,
        "composite_rule": {
            "terms": candidate_terms,
            "window": "full_text" if len(candidate_terms) > 3 else "adjacent_segments",
        } if len(candidate_terms) >= 2 else {},
        "stance": expected_stance,
        "semantic_category": "政治路线" if expected_stance in ("神", "左", "兔") else "国家民族叙事",
        "meaning_label": (
            "人工纠偏补充线索" if not isinstance(ai_review, dict)
            else (ai_review.get("meaning_labels") or ["人工纠偏补充线索"])[0]
        ),
        "confidence": 0.62,
        "evidence_spans": [str(text or "")[:500]],
        "reason": "AI 不可用时基于当前文本高频/证据词生成的待审核候选。",
    }]


def _normalize_candidate(raw: Dict[str, object], expected_stance: str, text: str) -> Dict[str, object]:
    terms = [str(term).strip() for term in raw.get("terms", []) if str(term).strip()]
    phrase = str(raw.get("phrase", "") or raw.get("term", "")).strip()
    if phrase and phrase not in terms:
        terms.insert(0, phrase)
    candidate_type = str(raw.get("candidate_type", "") or raw.get("type", "")).strip()
    if candidate_type not in ("term", "composite", "ambiguous_alias"):
        candidate_type = "composite" if len(terms) >= 2 else "term"
    stance = str(raw.get("stance", expected_stance)).strip()
    if stance not in STANCE_ORDER_TEXT:
        stance = expected_stance if expected_stance in STANCE_ORDER_TEXT else "乐子人"
    try:
        confidence = float(raw.get("confidence", 0.6) or 0.6)
    except (TypeError, ValueError):
        confidence = 0.6
    composite_rule = raw.get("composite_rule", {}) if isinstance(raw.get("composite_rule", {}), dict) else {}
    if candidate_type == "composite" and not composite_rule:
        composite_rule = {"terms": terms, "window": "adjacent_segments"}

    return {
        "candidate_id": f"cand_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "candidate_type": candidate_type,
        "terms": terms,
        "composite_rule": composite_rule,
        "stance": stance,
        "semantic_category": str(raw.get("semantic_category", "政治路线") or "政治路线"),
        "meaning_label": str(raw.get("meaning_label", "人工纠偏补充线索") or "人工纠偏补充线索"),
        "confidence": round(max(0.0, min(1.0, confidence)), 4),
        "evidence_spans": raw.get("evidence_spans", [text[:500]]) if isinstance(raw.get("evidence_spans", []), list) else [text[:500]],
        "reason": str(raw.get("reason", "")).strip() or "人工纠偏后由 AI/规则建议加入词库。",
        "status": "pending",
    }


def suggest_lexicon_updates(
    text: str,
    expected_stance: str,
    analysis_result: Dict[str, object],
) -> Dict[str, object]:
    expected_stance = expected_stance if expected_stance in STANCE_ORDER_TEXT else "乐子人"
    prompt = {
        "task": "根据用户纠偏结果，为政治历史隐喻词库生成候选增量。只生成候选，不直接写正式词库。",
        "expected_stance": expected_stance,
        "current_analysis": analysis_result,
        "instructions": [
            "优先提出能解释全文互文结构的连续短语或非连续组合规则。",
            "若多个词拆开含义弱、组合后含义强，请生成 candidate_type=composite。",
            "若旧词可能误导，例如字面猫被当作毛泽东代称，请生成 candidate_type=ambiguous_alias 并说明上下文条件。",
            "输出严格 JSON 数组，不要 Markdown。",
        ],
        "output_schema": {
            "candidate_type": "term|composite|ambiguous_alias",
            "terms": ["词或短语"],
            "composite_rule": {"terms": ["组合词"], "window": "same_segment|adjacent_segments|full_text"},
            "stance": "神|左|兔|皇|乐子人",
            "semantic_category": "一级类",
            "meaning_label": "真正含义标签",
            "confidence": "0-1 number",
            "evidence_spans": ["原文证据片段"],
            "reason": "中文说明",
        },
        "text": str(text or "")[:6000],
    }
    response = run_structured_ai_json(prompt, enabled=True)
    if not isinstance(response, list) or not response:
        response = _default_candidate_from_analysis(text, expected_stance, analysis_result)

    candidates_payload = load_lexicon_candidates()
    existing = candidates_payload.setdefault("candidates", [])
    created = []
    for item in response:
        if not isinstance(item, dict):
            continue
        candidate = _normalize_candidate(item, expected_stance, str(text or ""))
        if not candidate["terms"]:
            continue
        existing.append(candidate)
        created.append(candidate)
    _write_json(LEXICON_CANDIDATES_PATH, candidates_payload)
    return {
        "created_count": len(created),
        "candidates": created,
        "candidates_path": str(LEXICON_CANDIDATES_PATH),
    }


def _append_unique(items: List[Dict[str, object]], new_item: Dict[str, object], unique_keys: Tuple[str, ...]) -> bool:
    marker = tuple(str(new_item.get(key, "")) for key in unique_keys)
    for item in items:
        if tuple(str(item.get(key, "")) for key in unique_keys) == marker:
            item.update(new_item)
            return False
    items.append(new_item)
    return True


def apply_lexicon_candidates(candidate_ids: List[str]) -> Dict[str, object]:
    candidates_payload = load_lexicon_candidates()
    candidates = candidates_payload.setdefault("candidates", [])
    target_ids = set(candidate_ids)
    if not target_ids:
        target_ids = {str(item.get("candidate_id", "")) for item in candidates if item.get("status") == "pending"}

    semantic = _read_json(SEMANTIC_OVERRIDES_PATH, {"include_terms": [], "exclude_terms": [], "meaning_label_overrides": []})
    stance = _read_json(STANCE_OVERRIDES_PATH, {"term_overrides": [], "meaning_overrides": []})
    composite = load_composite_overrides()
    semantic.setdefault("include_terms", [])
    stance.setdefault("term_overrides", [])
    composite.setdefault("rules", [])

    applied = []
    for candidate in candidates:
        candidate_id = str(candidate.get("candidate_id", ""))
        if candidate_id not in target_ids or candidate.get("status") != "pending":
            continue
        terms = [str(term).strip() for term in candidate.get("terms", []) if str(term).strip()]
        candidate_type = str(candidate.get("candidate_type", "term"))
        semantic_category = str(candidate.get("semantic_category", "政治路线") or "政治路线")
        meaning_label = str(candidate.get("meaning_label", "人工纠偏补充线索") or "人工纠偏补充线索")
        stance_label = str(candidate.get("stance", "乐子人") or "乐子人")
        confidence = float(candidate.get("confidence", 0.6) or 0.6)
        reason = f"candidate:{candidate_id}; {candidate.get('reason', '')}"

        if candidate_type == "composite" and len(terms) >= 2:
            rule = candidate.get("composite_rule", {}) if isinstance(candidate.get("composite_rule", {}), dict) else {}
            rule_terms = [str(term).strip() for term in rule.get("terms", terms) if str(term).strip()]
            _append_unique(composite["rules"], {
                "rule_id": f"candidate_{candidate_id}",
                "terms": rule_terms or terms,
                "window": str(rule.get("window", "adjacent_segments") or "adjacent_segments"),
                "semantic_category": semantic_category,
                "meaning_label": meaning_label,
                "semantic_weight": round(2.6 + confidence, 4),
                "stance": stance_label,
                "stance_score": round(2.4 + confidence, 4),
                "confidence": confidence,
                "reason": reason,
                "enabled": True,
            }, ("rule_id",))
        else:
            for term in terms[:8]:
                _append_unique(semantic["include_terms"], {
                    "term": term,
                    "semantic_category": semantic_category,
                    "meaning_label": meaning_label,
                    "weight": round(2.5 + confidence, 4),
                    "reason": reason,
                }, ("term", "meaning_label"))
                if stance_label in STANCE_ORDER_TEXT and stance_label != "乐子人":
                    _append_unique(stance["term_overrides"], {
                        "term": term,
                        "stance": stance_label,
                        "score": round(2.2 + confidence, 4),
                        "reason": reason,
                    }, ("term", "stance"))

        candidate["status"] = "accepted"
        candidate["applied_at"] = datetime.now().isoformat(timespec="seconds")
        applied.append(candidate_id)

    _write_json(SEMANTIC_OVERRIDES_PATH, semantic)
    _write_json(STANCE_OVERRIDES_PATH, stance)
    _write_json(COMPOSITE_OVERRIDES_PATH, composite)
    _write_json(LEXICON_CANDIDATES_PATH, candidates_payload)
    return {"applied_count": len(applied), "applied_ids": applied}


def reject_lexicon_candidates(candidate_ids: List[str]) -> Dict[str, object]:
    candidates_payload = load_lexicon_candidates()
    candidates = candidates_payload.setdefault("candidates", [])
    target_ids = set(candidate_ids)
    rejected = []
    for candidate in candidates:
        candidate_id = str(candidate.get("candidate_id", ""))
        if candidate_id in target_ids and candidate.get("status") == "pending":
            candidate["status"] = "rejected"
            candidate["rejected_at"] = datetime.now().isoformat(timespec="seconds")
            rejected.append(candidate_id)
    _write_json(LEXICON_CANDIDATES_PATH, candidates_payload)
    return {"rejected_count": len(rejected), "rejected_ids": rejected}


def analyze_text(
    text: str,
    output_dir: Optional[Path] = None,
    enable_fuzzy: bool = True,
    enable_ai: bool = False,
    expected_stance: Optional[str] = None,
    enable_correction_suggestion: bool = False,
) -> Dict[str, object]:
    output_dir = output_dir or create_text_analysis_output_dir()
    data_dir = output_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    manual = load_manual_lexicon_overrides()
    effective_map = build_effective_term_map(manual["semantic"])
    term_map = _term_metadata(effective_map, manual["stance"])
    segments = _split_text(text)
    composite_overrides = load_composite_overrides()
    matches = fuzzy_match_text(text, term_map, enable_fuzzy=enable_fuzzy)
    matches.extend(_composite_match_text(text, segments, composite_overrides))

    axis_rows = []
    axis_hits = []
    for idx, segment in enumerate(segments, start=1):
        segment_id = f"text_{idx:03d}"
        row, hits = _build_axis_row(segment_id, segment)
        axis_rows.append(row)
        for hit in hits:
            hit["segment_id"] = segment_id
            axis_hits.append(hit)

    meaning_df = _build_meaning_rows(segments, matches, axis_rows)
    stance_df = build_stance_labels(meaning_df, enable_ai=False, manual_stance=manual["stance"])
    stance_df = _remove_uncertain_manual_alias_stance(stance_df, matches)
    stance_df = _apply_fuzzy_stance_boost(stance_df, matches)

    matches_df = pd.DataFrame(matches)
    axis_df = pd.DataFrame(axis_rows)
    axis_hits_df = pd.DataFrame(axis_hits)
    segments_df = pd.DataFrame([
        {
            "segment_id": f"text_{idx:03d}",
            "content": segment,
            "normalized_text": normalize_text(segment),
            "tokens": ",".join(tokenize(segment)),
        }
        for idx, segment in enumerate(segments, start=1)
    ])

    ai_review = _ai_full_text_review(
        text=text,
        matches_df=matches_df,
        meaning_df=meaning_df,
        stance_df=stance_df,
        enabled=enable_ai,
    )

    for name, df in [
        ("segments.csv", segments_df),
        ("matches.csv", matches_df),
        ("axis_scores.csv", axis_df),
        ("axis_hits.csv", axis_hits_df),
        ("meaning_labels.csv", meaning_df),
        ("stance_labels.csv", stance_df),
    ]:
        df.to_csv(data_dir / name, index=False, encoding=CSV_ENCODING, quoting=csv.QUOTE_MINIMAL)

    valid_meanings = meaning_df[meaning_df["is_political_historical"]] if not meaning_df.empty else pd.DataFrame()
    meaning_distribution = _distribution(valid_meanings["primary_meaning_label"].tolist()) if not valid_meanings.empty else []
    stance_distribution = _distribution(
        stance_df["stance"].tolist() if not stance_df.empty else [],
        order=STANCE_ORDER_TEXT,
    )
    weighted_stance_distribution = _apply_ai_stance_weight(
        stance_distribution,
        ai_review,
        len(segments),
    ) if ai_review else []
    displayed_stance_distribution = weighted_stance_distribution or stance_distribution
    top_stance = displayed_stance_distribution[0]["label"] if displayed_stance_distribution else "乐子人"
    accuracy = _accuracy_summary(matches_df)
    expected_stance = expected_stance if expected_stance in STANCE_ORDER_TEXT else ""
    is_expected_match = bool(expected_stance and top_stance == expected_stance)

    summary = {
        "output_dir": str(output_dir),
        "segment_count": len(segments),
        "match_accuracy": accuracy,
        "stance": top_stance,
        "stance_distribution": displayed_stance_distribution,
        "rule_stance_distribution": stance_distribution,
        "ai_weighted_stance_distribution": weighted_stance_distribution,
        "ai_fulltext_review": ai_review,
        "meaning_distribution": meaning_distribution[:20],
        "enable_fuzzy": bool(enable_fuzzy),
        "enable_ai": bool(enable_ai),
        "pinyin_available": bool(lazy_pinyin),
        "expected_stance": expected_stance,
        "is_expected_match": is_expected_match if expected_stance else None,
        "correction_suggestion": {},
    }
    if expected_stance and enable_correction_suggestion and not is_expected_match:
        summary["correction_suggestion"] = suggest_lexicon_updates(text, expected_stance, summary)
    (data_dir / "ai_fulltext_review.json").write_text(
        json.dumps(ai_review, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (data_dir / "correction_suggestion.json").write_text(
        json.dumps(summary["correction_suggestion"], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (data_dir / "analysis_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    report_md = _render_report(text, summary, matches_df, meaning_df, stance_df)
    (output_dir / "report.md").write_text(report_md, encoding="utf-8")
    (output_dir / "report.html").write_text(markdown_to_html(report_md, title="上传文本立场分析"), encoding="utf-8")
    return {
        **summary,
        "reports": {
            "markdown": str(output_dir / "report.md"),
            "html": str(output_dir / "report.html"),
        },
    }


def _render_report(
    text: str,
    summary: Dict[str, object],
    matches_df: pd.DataFrame,
    meaning_df: pd.DataFrame,
    stance_df: pd.DataFrame,
) -> str:
    lines = [
        "# 上传文本立场分析",
        "",
        "## 样本说明",
        "",
        f"- 分段数：{summary['segment_count']}",
        f"- 模糊匹配：{'启用' if summary['enable_fuzzy'] else '关闭'}",
        f"- AI 全文理解：{'启用' if summary.get('enable_ai') else '关闭'}",
        f"- 同音匹配：{'可用' if summary['pinyin_available'] else '不可用（未安装 pypinyin）'}",
        f"- 综合立场：{summary['stance']}",
    ]
    if summary.get("expected_stance"):
        result_text = "符合" if summary.get("is_expected_match") else "不符合"
        lines.append(f"- 预期立场：{summary.get('expected_stance')}（{result_text}）")
    if summary.get("correction_suggestion"):
        suggestion = summary["correction_suggestion"]
        lines.append(
            f"- 已生成纠偏候选：{suggestion.get('created_count', 0)} 条，"
            f"候选文件：{suggestion.get('candidates_path', '')}"
        )
    lines.extend(["", "## 匹配准确度", ""])
    acc = summary["match_accuracy"]
    for key in ["match_count", "exact_count", "fuzzy_count", "needs_review_count", "estimated_precision"]:
        lines.append(f"- {key}: {acc.get(key)}")
    lines.append(f"- 说明：{acc.get('note', '')}")
    lines.extend(["", "## 立场占比", ""])
    unit = "加权分" if summary.get("ai_weighted_stance_distribution") else "段"
    for item in summary["stance_distribution"]:
        lines.append(f"- {item['label']}：{item['count']} {unit}，{item['share_pct']}%")

    if summary.get("ai_fulltext_review"):
        ai_review = summary["ai_fulltext_review"]
        lines.extend(["", "## AI 全文理解加权", ""])
        lines.append(f"- AI 立场：{ai_review.get('stance')}（置信度 {ai_review.get('confidence')}）")
        if ai_review.get("meaning_labels"):
            lines.append(f"- AI 含义标签：{', '.join(map(str, ai_review.get('meaning_labels', [])))}")
        if ai_review.get("evidence_terms"):
            lines.append(f"- AI 证据词：{', '.join(map(str, ai_review.get('evidence_terms', [])))}")
        if ai_review.get("reason"):
            lines.append(f"- AI 说明：{ai_review.get('reason')}")
        if summary.get("rule_stance_distribution"):
            lines.append("- 本地规则原始立场占比：" + "；".join(
                f"{item['label']} {item['share_pct']}%" for item in summary["rule_stance_distribution"]
            ))

    lines.extend(["", "## 真正含义 Top", ""])
    for item in summary["meaning_distribution"][:10]:
        lines.append(f"- {item['label']}：{item['count']} 段，{item['share_pct']}%")

    lines.extend(["", "## 高置信命中词", ""])
    if matches_df.empty:
        lines.append("未命中词库。")
    else:
        top = matches_df.sort_values(["confidence", "semantic_weight"], ascending=False).head(30)
        for _, row in top.iterrows():
            review = "，需复核" if bool(row.get("needs_review", False)) else ""
            lines.append(
                f"- {row.get('canonical_term')} <= {row.get('matched_text')} "
                f"({row.get('match_method')}, {float(row.get('confidence', 0)):.2f}{review})："
                f"{row.get('meaning_label') or row.get('stance')}"
            )

    review_df = matches_df[matches_df["needs_review"].fillna(False)] if not matches_df.empty else pd.DataFrame()
    lines.extend(["", "## 需人工复核", ""])
    if review_df.empty:
        lines.append("暂无。")
    else:
        for _, row in review_df.head(30).iterrows():
            lines.append(
                f"- {row.get('segment_id')}：{row.get('canonical_term')} <= {row.get('matched_text')}，"
                f"{row.get('reason')}"
            )

    lines.extend(["", "## 原文摘录", "", "```text", str(text or "")[:3000], "```", ""])
    return "\n".join(lines)
