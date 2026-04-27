"""
Cross-song deep cleaning and summary reporting for existing research outputs.
"""
from __future__ import annotations

import json
import logging
import math
import re
import shutil
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from config.config import CSV_ENCODING
from src.research.ai_analyzer import run_structured_ai_json
from src.research.analysis import _setup_plot, tokenize
from src.research.reports import markdown_to_html

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MANUAL_LEXICON_DIR = PROJECT_ROOT / "config" / "manual_lexicons"
MANUAL_SEMANTIC_OVERRIDES_PATH = MANUAL_LEXICON_DIR / "semantic_overrides.json"
MANUAL_STANCE_OVERRIDES_PATH = MANUAL_LEXICON_DIR / "stance_overrides.json"
MANUAL_COMPOSITE_OVERRIDES_PATH = MANUAL_LEXICON_DIR / "composite_overrides.json"


SEMANTIC_CATEGORY_ORDER = [
    "历史事件",
    "政治路线",
    "制度治理",
    "国家民族叙事",
    "阶层与经济秩序",
    "革命与改革",
    "文化保守与礼法",
]

STANCE_ORDER = ["神", "左", "兔", "皇", "乐子人"]

GENERIC_EXCLUDED_TERMS = {
    "好听", "难听", "上头", "循环", "单曲", "神曲", "宝藏", "好歌", "前奏", "副歌", "歌词",
    "编曲", "旋律", "嗓音", "戏腔", "唱腔", "翻唱", "原唱", "版本", "节奏", "氛围", "感觉",
    "泪目", "好哭", "爷青回", "童年", "青春", "哈哈", "哈哈哈", "呜呜", "呜呜呜", "绝了",
    "好美", "好帅", "仙气", "宿命感", "鸡皮疙瘩", "高音", "低音", "和声", "入坑", "推荐",
    "古风", "歌曲", "音乐", "评论", "视频", "up", "UP", "b站", "弹幕", "作者", "up主",
    "姐姐", "哥哥", "老师", "姐妹", "兄弟", "小姐姐", "小哥哥", "角色", "画面", "剧情",
}


AMBIGUOUS_SEED_TERMS = {
    "世界",
    "天下",
    "四海",
    "五洲",
    "万邦",
    "万国",
    "大同",
    "丝路",
    "民族主义",
}

CONTEXTUAL_MEANING_RULES = [
    {
        "rule_id": "left_world_people_unity",
        "all_of": ["世界", "人民"],
        "any_of": ["大团结", "联合起来", "万岁"],
        "meaning_category": "政治路线",
        "meaning_label": "国际主义动员",
        "score": 4.4,
    },
    {
        "rule_id": "left_world_proletarian_unity",
        "all_of": ["世界", "无产者"],
        "any_of": ["联合起来", "国际歌", "英特纳雄耐尔"],
        "meaning_category": "政治路线",
        "meaning_label": "国际主义动员",
        "score": 4.6,
    },
    {
        "rule_id": "mao_four_seas_five_continents",
        "all_of": ["四海", "五洲"],
        "any_of": ["风雷", "翻腾", "震荡", "换了人间"],
        "meaning_category": "政治路线",
        "meaning_label": "毛泽东诗词政治隐喻",
        "score": 4.8,
    },
    {
        "rule_id": "nation_tianxia_state_context",
        "all_of": ["天下"],
        "any_of": ["家国", "社稷", "江山", "兴亡", "苍生", "九州", "华夏", "中原", "山河", "故国"],
        "meaning_category": "国家民族叙事",
        "meaning_label": "家国叙事",
        "score": 3.3,
    },
]


SEMANTIC_ONTOLOGY_SEED: Dict[str, Dict[str, Dict[str, Iterable[str]]]] = {
    "历史事件": {
        "改革开放": {
            "terms": ["改革开放", "改开", "南巡", "经济特区", "拨乱反正", "深圳", "市场化", "下海"],
            "weight": 3.1,
        },
        "文化大革命": {
            "terms": ["文革", "文化大革命", "红卫兵", "造反派", "批斗", "上山下乡", "破四旧"],
            "weight": 3.4,
        },
        "王朝更替": {
            "terms": ["改朝换代", "王朝", "朝代", "乱世", "盛世", "亡国", "兴亡", "鼎革"],
            "weight": 2.8,
        },
        "晚清民国转型": {
            "terms": ["晚清", "民国", "辛亥", "北洋", "洋务", "维新", "革命党"],
            "weight": 2.8,
        },
    },
    "政治路线": {
        "国家复兴": {
            "terms": ["复兴", "崛起", "强国", "盛世", "民族复兴", "中国梦"],
            "weight": 2.9,
        },
        "自由化改革": {
            "terms": ["自由化", "启蒙", "普世", "法治", "民主", "人权", "宪政"],
            "weight": 3.0,
        },
        "新左与再分配": {
            "terms": ["新左", "左派", "公社", "平均主义", "再分配", "共同富裕", "国企"],
            "weight": 3.0,
        },
    },
    "制度治理": {
        "中央集权": {
            "terms": ["中央集权", "皇权", "君权", "大一统", "朝廷", "王权", "科层"],
            "weight": 3.1,
        },
        "秩序与威权": {
            "terms": ["威权", "秩序", "铁律", "禁令", "整肃", "服从", "忠君"],
            "weight": 3.0,
        },
        "市场治理": {
            "terms": ["市场", "私有化", "资本", "商业", "通商", "贸易", "企业家"],
            "weight": 2.7,
        },
    },
    "国家民族叙事": {
        "家国叙事": {
            "terms": ["家国", "山河", "故国", "故土", "九州", "华夏", "社稷", "中原"],
            "weight": 3.2,
        },
        "天下世界": {
            "terms": ["天下", "四海", "万邦", "世界", "大同", "万国", "丝路"],
            "weight": 2.6,
        },
        "民族主义": {
            "terms": ["民族主义", "汉人", "汉家", "国族", "边疆", "正统", "国祚"],
            "weight": 2.9,
        },
    },
    "阶层与经济秩序": {
        "阶层分化": {
            "terms": ["阶层", "贫富", "底层", "工人", "资本家", "剥削", "内卷"],
            "weight": 2.9,
        },
        "民生与分配": {
            "terms": ["民生", "分配", "赋税", "赈灾", "均贫", "田亩", "仓廪"],
            "weight": 2.8,
        },
    },
    "革命与改革": {
        "革命动员": {
            "terms": ["革命", "起义", "造反", "反抗", "觉醒", "破局", "斗争"],
            "weight": 3.1,
        },
        "制度改革": {
            "terms": ["改革", "变法", "革新", "改良", "新政", "图强"],
            "weight": 2.9,
        },
    },
    "文化保守与礼法": {
        "传统礼教": {
            "terms": ["礼教", "宗法", "纲常", "祖训", "家法", "礼法", "忠孝"],
            "weight": 3.1,
        },
        "文明正统": {
            "terms": ["正统", "华夷", "礼制", "祖宗之法", "血统", "门阀"],
            "weight": 2.8,
        },
    },
}


STANCE_RULES = {
    "神": {
        "terms": ["自由", "民主", "人权", "宪政", "启蒙", "普世", "西化", "反贼", "神友"],
        "meanings": {"自由化改革"},
        "axis_bonus": {
            "liberty_authority_net_score": 1.2,
            "world_nation_net_score": 0.5,
            "progress_conservative_net_score": 0.6,
        },
    },
    "左": {
        "terms": ["左派", "新左", "公社", "工农", "平均主义", "再分配", "计划", "文革", "毛左"],
        "meanings": {"文化大革命", "新左与再分配", "革命动员", "民生与分配", "国际主义动员", "毛泽东诗词政治隐喻"},
        "axis_bonus": {
            "plan_market_net_score": 1.0,
            "progress_conservative_net_score": 0.5,
        },
    },
    "兔": {
        "terms": ["复兴", "强国", "特色", "改开", "改革开放", "建制", "稳定", "兔友"],
        "meanings": {"改革开放", "国家复兴", "家国叙事"},
        "axis_bonus": {
            "world_nation_net_score": -0.8,
            "liberty_authority_net_score": -0.5,
        },
    },
    "皇": {
        "terms": ["皇汉", "保守", "大一统", "正统", "祖宗之法", "汉家", "秩序", "威权"],
        "meanings": {"民族主义", "中央集权", "传统礼教", "文明正统"},
        "axis_bonus": {
            "world_nation_net_score": -1.0,
            "liberty_authority_net_score": -1.0,
            "progress_conservative_net_score": -1.2,
        },
    },
}

DEFAULT_MANUAL_SEMANTIC_OVERRIDES = {
    "include_terms": [
        {
            "term": "改革开放",
            "semantic_category": "历史事件",
            "meaning_label": "改革开放",
            "weight": 3.1,
            "reason": "manual seed example",
        }
    ],
    "exclude_terms": [
        {
            "term": "好听",
            "reason": "music appreciation term",
        }
    ],
    "meaning_label_overrides": [
        {
            "term": "文革",
            "semantic_category": "历史事件",
            "meaning_label": "文化大革命",
            "weight": 3.4,
            "reason": "normalize short form",
        }
    ],
}

DEFAULT_MANUAL_STANCE_OVERRIDES = {
    "term_overrides": [
        {
            "term": "改开",
            "stance": "兔",
            "score": 2.4,
            "reason": "manual stance example",
        }
    ],
    "meaning_overrides": [
        {
            "meaning_label": "改革开放",
            "stance": "兔",
            "score": 2.0,
            "reason": "manual meaning-to-stance mapping",
        }
    ],
}


def safe_slug(value: str) -> str:
    value = re.sub(r'[\\/:*?"<>|\s]+', "_", str(value).strip())
    value = re.sub(r"_+", "_", value).strip("_")
    return value[:80] or "summary"


def _compact_text(text: str) -> str:
    return re.sub(r"[^\w\u4e00-\u9fff]+", "", str(text or "")).lower()


def _has_context_terms(text: str, term: str, contexts: Optional[List[str]] = None) -> bool:
    candidates = [
        str(context).strip()
        for context in (contexts or [])
        if str(context).strip()
    ]
    if not candidates:
        return term not in AMBIGUOUS_SEED_TERMS
    compact = _compact_text(text)
    return any(_compact_text(context) in compact for context in candidates if context != term)


def _ensure_manual_lexicon_files():
    MANUAL_LEXICON_DIR.mkdir(parents=True, exist_ok=True)
    if not MANUAL_SEMANTIC_OVERRIDES_PATH.exists():
        MANUAL_SEMANTIC_OVERRIDES_PATH.write_text(
            json.dumps(DEFAULT_MANUAL_SEMANTIC_OVERRIDES, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    if not MANUAL_STANCE_OVERRIDES_PATH.exists():
        MANUAL_STANCE_OVERRIDES_PATH.write_text(
            json.dumps(DEFAULT_MANUAL_STANCE_OVERRIDES, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def _load_json_or_default(path: Path, default: Dict[str, object]) -> Dict[str, object]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else json.loads(json.dumps(default))
    except Exception:
        logger.warning("Failed to load manual lexicon file: %s", path)
        return json.loads(json.dumps(default))


def load_manual_composite_rules() -> List[Dict[str, object]]:
    payload = _load_json_or_default(MANUAL_COMPOSITE_OVERRIDES_PATH, {"rules": []})
    rules = payload.get("rules", [])
    return [rule for rule in rules if isinstance(rule, dict)]


def load_manual_lexicon_overrides() -> Dict[str, Dict[str, object]]:
    _ensure_manual_lexicon_files()
    return {
        "semantic": _load_json_or_default(MANUAL_SEMANTIC_OVERRIDES_PATH, DEFAULT_MANUAL_SEMANTIC_OVERRIDES),
        "stance": _load_json_or_default(MANUAL_STANCE_OVERRIDES_PATH, DEFAULT_MANUAL_STANCE_OVERRIDES),
    }


def _flatten_seed_terms() -> Dict[str, List[Dict[str, object]]]:
    term_map: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for category, meanings in SEMANTIC_ONTOLOGY_SEED.items():
        for meaning, meta in meanings.items():
            weight = float(meta.get("weight", 2.5))
            for term in meta.get("terms", []):
                term_map[str(term)].append({
                    "category": category,
                    "meaning": meaning,
                    "weight": weight,
                    "source": "seed",
                    "context_required": str(term) in AMBIGUOUS_SEED_TERMS,
                })
    return term_map


SEED_TERM_MAP = _flatten_seed_terms()


def build_effective_term_map(manual_semantic: Optional[Dict[str, object]] = None) -> Dict[str, List[Dict[str, object]]]:
    term_map: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for term, values in SEED_TERM_MAP.items():
        term_map[term].extend([dict(item) for item in values])

    manual_semantic = manual_semantic or {}
    for item in manual_semantic.get("include_terms", []):
        if not isinstance(item, dict):
            continue
        term = str(item.get("term", "")).strip()
        category = str(item.get("semantic_category", "")).strip()
        meaning = str(item.get("meaning_label", "")).strip()
        if not term or category not in SEMANTIC_CATEGORY_ORDER or not meaning:
            continue
        term_map[term].append({
            "category": category,
            "meaning": meaning,
            "weight": float(item.get("weight", 2.8) or 2.8),
            "source": "manual_include",
            "context_required": bool(item.get("context_required", False)),
            "context_terms": [
                str(context).strip()
                for context in item.get("context_terms", []) or []
                if str(context).strip()
            ],
        })

    for item in manual_semantic.get("meaning_label_overrides", []):
        if not isinstance(item, dict):
            continue
        term = str(item.get("term", "")).strip()
        category = str(item.get("semantic_category", "")).strip()
        meaning = str(item.get("meaning_label", "")).strip()
        if not term or category not in SEMANTIC_CATEGORY_ORDER or not meaning:
            continue
        term_map[term] = [{
            "category": category,
            "meaning": meaning,
            "weight": float(item.get("weight", 3.0) or 3.0),
            "source": "manual_override",
            "context_required": bool(item.get("context_required", False)),
            "context_terms": [
                str(context).strip()
                for context in item.get("context_terms", []) or []
                if str(context).strip()
            ],
        }]
    return term_map


def _iter_song_dirs(result_root: Path) -> List[Path]:
    return sorted(
        [
            path for path in result_root.iterdir()
            if path.is_dir() and (path / "data" / "comments_anonymized.csv").exists()
        ]
    )


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as exc:
        logger.warning("Failed to read %s: %s", path, exc)
        return pd.DataFrame()


def load_result_bundle(result_root: Path) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    comment_frames = []
    term_frames = []
    axis_frames = []
    theme_frames = []
    song_rows = []

    for song_dir in _iter_song_dirs(result_root):
        song_key = song_dir.name
        song_name = re.sub(r"_\d{8}_\d{6}$", "", song_key)
        comments = _read_csv(song_dir / "data" / "comments_anonymized.csv")
        word_frequency = _read_csv(song_dir / "data" / "word_frequency.csv")
        political_comments = _read_csv(song_dir / "data" / "political_axis_comments.csv")
        political_terms = _read_csv(song_dir / "data" / "political_axis_terms.csv")
        ai_themes = _read_csv(song_dir / "data" / "ai_themes.csv")

        if comments.empty:
            continue

        comments = comments.copy()
        comments["song_key"] = song_key
        comments["song_name"] = song_name
        if not political_comments.empty:
            candidate_cols = [
                col for col in political_comments.columns
                if col not in {"content"} and (col == "comment_id" or col not in comments.columns)
            ]
            political_subset = political_comments.drop_duplicates(subset=["comment_id"])[candidate_cols]
            comments = comments.merge(political_subset, on="comment_id", how="left")
        comment_frames.append(comments)

        if not word_frequency.empty:
            wf = word_frequency.copy()
            wf["song_key"] = song_key
            wf["song_name"] = song_name
            term_frames.append(wf)

        if not political_terms.empty:
            pt = political_terms.copy()
            pt["song_key"] = song_key
            pt["song_name"] = song_name
            axis_frames.append(pt)

        if not ai_themes.empty:
            themes = ai_themes.copy()
            themes["song_key"] = song_key
            themes["song_name"] = song_name
            theme_frames.append(themes)

        song_rows.append({
            "song_key": song_key,
            "song_name": song_name,
            "comment_count": int(len(comments)),
            "term_count": int(len(word_frequency)),
            "political_term_count": int(len(political_terms)),
            "theme_count": int(len(ai_themes)),
            "source_dir": str(song_dir),
        })

    combined_comments = pd.concat(comment_frames, ignore_index=True) if comment_frames else pd.DataFrame()
    bundle = {
        "songs": pd.DataFrame(song_rows),
        "word_frequency": pd.concat(term_frames, ignore_index=True) if term_frames else pd.DataFrame(),
        "political_terms": pd.concat(axis_frames, ignore_index=True) if axis_frames else pd.DataFrame(),
        "ai_themes": pd.concat(theme_frames, ignore_index=True) if theme_frames else pd.DataFrame(),
    }
    return combined_comments, bundle


def load_song_result_bundle(song_dir: Path) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    song_dir = Path(song_dir)
    data_dir = song_dir / "data"
    if not data_dir.exists():
        return pd.DataFrame(), {}

    song_key = song_dir.name
    song_name = re.sub(r"_\d{8}_\d{6}$", "", song_key)
    comments = _read_csv(data_dir / "comments_anonymized.csv")
    word_frequency = _read_csv(data_dir / "word_frequency.csv")
    political_comments = _read_csv(data_dir / "political_axis_comments.csv")
    political_terms = _read_csv(data_dir / "political_axis_terms.csv")
    ai_themes = _read_csv(data_dir / "ai_themes.csv")
    tfidf = _read_csv(data_dir / "tfidf_keywords.csv")

    if comments.empty:
        return pd.DataFrame(), {}

    comments = comments.copy()
    comments["song_key"] = song_key
    comments["song_name"] = song_name
    if not political_comments.empty:
        candidate_cols = [
            col for col in political_comments.columns
            if col not in {"content"} and (col == "comment_id" or col not in comments.columns)
        ]
        political_subset = political_comments.drop_duplicates(subset=["comment_id"])[candidate_cols]
        comments = comments.merge(political_subset, on="comment_id", how="left")

    bundle = {
        "songs": pd.DataFrame([{
            "song_key": song_key,
            "song_name": song_name,
            "comment_count": int(len(comments)),
            "term_count": int(len(word_frequency)),
            "political_term_count": int(len(political_terms)),
            "theme_count": int(len(ai_themes)),
            "source_dir": str(song_dir),
        }]),
        "word_frequency": word_frequency.assign(song_key=song_key, song_name=song_name) if not word_frequency.empty else pd.DataFrame(),
        "political_terms": political_terms.assign(song_key=song_key, song_name=song_name) if not political_terms.empty else pd.DataFrame(),
        "ai_themes": ai_themes.assign(song_key=song_key, song_name=song_name) if not ai_themes.empty else pd.DataFrame(),
        "tfidf": tfidf,
    }
    return comments, bundle


def _build_term_evidence(bundle: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: Dict[str, Dict[str, object]] = {}

    for _, row in bundle.get("word_frequency", pd.DataFrame()).iterrows():
        term = str(row.get("keyword", "")).strip()
        if not term:
            continue
        item = rows.setdefault(term, {
            "term": term,
            "evidence_count": 0,
            "songs_count": set(),
            "sources": set(),
            "raw_frequency": 0,
            "weighted_frequency": 0.0,
        })
        item["raw_frequency"] += int(row.get("frequency", 0) or 0)
        item["evidence_count"] += int(row.get("frequency", 0) or 0)
        item["songs_count"].add(str(row.get("song_key", "")))
        item["sources"].add("word_frequency")

    for _, row in bundle.get("political_terms", pd.DataFrame()).iterrows():
        term = str(row.get("keyword", "")).strip()
        if not term:
            continue
        item = rows.setdefault(term, {
            "term": term,
            "evidence_count": 0,
            "songs_count": set(),
            "sources": set(),
            "raw_frequency": 0,
            "weighted_frequency": 0.0,
        })
        item["raw_frequency"] += int(row.get("raw_frequency", 0) or 0)
        item["evidence_count"] += int(row.get("raw_frequency", 0) or 0)
        item["weighted_frequency"] += float(row.get("weighted_frequency", 0) or 0)
        item["songs_count"].add(str(row.get("song_key", "")))
        item["sources"].add("political_terms")

    records = []
    for item in rows.values():
        records.append({
            "term": item["term"],
            "evidence_count": int(item["evidence_count"]),
            "songs_count": len(item["songs_count"]),
            "sources": ",".join(sorted(item["sources"])),
            "raw_frequency": int(item["raw_frequency"]),
            "weighted_frequency": round(float(item["weighted_frequency"]), 4),
        })
    df = pd.DataFrame(records)
    if df.empty:
        return pd.DataFrame(columns=[
            "term", "evidence_count", "songs_count", "sources", "raw_frequency", "weighted_frequency"
        ])
    return df.sort_values(["weighted_frequency", "evidence_count"], ascending=False).reset_index(drop=True)


def _review_terms_with_ai(term_rows: List[Dict[str, object]], enabled: bool) -> List[Dict[str, object]]:
    if not enabled or not term_rows:
        return []

    prompt = {
        "task": "你是社会科学研究助理。请判断下列中文词条在B站古风歌曲评论研究里，是否具备明确的政治/历史属性。",
        "instructions": [
            "输出严格 JSON 数组。",
            "仅使用三个 semantic_status: keep / exclude / review。",
            "semantic_category 只能从这些值中选择：历史事件、政治路线、制度治理、国家民族叙事、阶层与经济秩序、革命与改革、文化保守与礼法、none。",
            "如果词条只是审美、情绪、音乐评价、泛意象、人物称呼或平台语，应排除。",
            "confidence 使用 0 到 1 的小数。",
        ],
        "items": term_rows,
        "output_schema": {
            "term": "词条",
            "semantic_status": "keep | exclude | review",
            "semantic_category": "七个一级类之一或 none",
            "confidence": "0-1 number",
            "reason": "简短中文说明",
        },
    }
    response = run_structured_ai_json(prompt, enabled=True)
    if not isinstance(response, list):
        return []
    normalized = []
    for item in response:
        if not isinstance(item, dict):
            continue
        normalized.append({
            "term": str(item.get("term", "")).strip(),
            "semantic_status": str(item.get("semantic_status", "review")).strip(),
            "semantic_category": str(item.get("semantic_category", "none")).strip(),
            "confidence": float(item.get("confidence", 0) or 0),
            "reason": str(item.get("reason", "")).strip(),
        })
    return normalized


def semantic_clean_terms(
    term_df: pd.DataFrame,
    enable_ai: bool = False,
    manual_semantic: Optional[Dict[str, object]] = None,
    effective_term_map: Optional[Dict[str, List[Dict[str, object]]]] = None,
) -> Dict[str, pd.DataFrame]:
    if term_df.empty:
        empty = pd.DataFrame(columns=[
            "term", "semantic_status", "semantic_category", "evidence_count",
            "decision_source", "confidence", "reason", "raw_frequency", "weighted_frequency",
        ])
        return {
            "clean_terms": empty.copy(),
            "excluded_terms": empty.copy(),
            "review_queue": empty.copy(),
        }

    manual_semantic = manual_semantic or {}
    effective_term_map = effective_term_map or SEED_TERM_MAP
    manual_excludes = {
        str(item.get("term", "")).strip()
        for item in manual_semantic.get("exclude_terms", [])
        if isinstance(item, dict) and str(item.get("term", "")).strip()
    }
    ai_candidates = []
    records = []
    for _, row in term_df.iterrows():
        term = str(row.get("term", "")).strip()
        if not term:
            continue
        base = {
            "term": term,
            "evidence_count": int(row.get("evidence_count", 0) or 0),
            "raw_frequency": int(row.get("raw_frequency", 0) or 0),
            "weighted_frequency": round(float(row.get("weighted_frequency", 0) or 0), 4),
            "semantic_status": "review",
            "semantic_category": "none",
            "decision_source": "rule",
            "confidence": 0.5,
            "reason": "",
        }
        if term in manual_excludes:
            base.update({
                "semantic_status": "exclude",
                "semantic_category": "none",
                "decision_source": "manual_override",
                "confidence": 1.0,
                "reason": "manual_exclude",
            })
        elif term in GENERIC_EXCLUDED_TERMS:
            base.update({
                "semantic_status": "exclude",
                "semantic_category": "none",
                "confidence": 0.98,
                "reason": "generic_music_or_emotion_term",
            })
        elif term in effective_term_map:
            matches = effective_term_map[term]
            if all(bool(item.get("context_required", False)) for item in matches):
                base.update({
                    "semantic_status": "review",
                    "semantic_category": "none",
                    "decision_source": "rule",
                    "confidence": 0.52,
                    "reason": "ambiguous_seed_term_requires_context",
                })
            else:
                categories = [item["category"] for item in matches]
                base.update({
                    "semantic_status": "keep",
                    "semantic_category": categories[0],
                    "decision_source": "manual_override" if any(
                        str(item.get("source", "")).startswith("manual") for item in matches
                    ) else "seed_ontology",
                    "confidence": 0.99,
                    "reason": "term_map_match",
                })
        elif float(row.get("weighted_frequency", 0) or 0) > 0:
            base.update({
                "semantic_status": "keep",
                "semantic_category": "制度治理",
                "decision_source": "rule",
                "confidence": 0.72,
                "reason": "political_axis_overlap",
            })
        elif len(term) <= 2 and int(row.get("evidence_count", 0) or 0) < 8:
            base.update({
                "semantic_status": "exclude",
                "semantic_category": "none",
                "confidence": 0.82,
                "reason": "short_low_evidence",
            })
        else:
            ai_candidates.append({
                "term": term,
                "evidence_count": int(row.get("evidence_count", 0) or 0),
                "songs_count": int(row.get("songs_count", 0) or 0),
                "weighted_frequency": round(float(row.get("weighted_frequency", 0) or 0), 4),
            })
        records.append(base)

    ai_map = {}
    if ai_candidates:
        selected = ai_candidates[: min(20, len(ai_candidates))]
        for start in range(0, len(selected), 20):
            batch = selected[start:start + 20]
            for item in _review_terms_with_ai(batch, enabled=enable_ai):
                if item["term"]:
                    ai_map[item["term"]] = item

    for row in records:
        reviewed = ai_map.get(row["term"])
        if reviewed:
            row["semantic_status"] = reviewed["semantic_status"]
            row["semantic_category"] = reviewed["semantic_category"]
            row["decision_source"] = "ai_review"
            row["confidence"] = round(float(reviewed["confidence"]), 4)
            row["reason"] = reviewed["reason"]

    df = pd.DataFrame(records)
    clean = df[df["semantic_status"] == "keep"].copy().sort_values(
        ["weighted_frequency", "evidence_count"], ascending=False
    )
    excluded = df[df["semantic_status"] == "exclude"].copy().sort_values(
        ["evidence_count", "term"], ascending=[False, True]
    )
    review = df[df["semantic_status"] == "review"].copy().sort_values(
        ["evidence_count", "term"], ascending=[False, True]
    )
    return {
        "clean_terms": clean.reset_index(drop=True),
        "excluded_terms": excluded.reset_index(drop=True),
        "review_queue": review.reset_index(drop=True),
    }


def _meaning_candidates_from_comment(
    content: str,
    effective_term_map: Optional[Dict[str, List[Dict[str, object]]]] = None,
) -> Dict[str, Dict[str, object]]:
    tokens = tokenize(content)
    text = str(content or "")
    effective_term_map = effective_term_map or SEED_TERM_MAP
    compact_text = _compact_text(text)
    scores: Dict[str, Dict[str, object]] = {}

    for rule in CONTEXTUAL_MEANING_RULES:
        all_of = [term for term in rule.get("all_of", []) if _compact_text(term)]
        any_of = [term for term in rule.get("any_of", []) if _compact_text(term)]
        if all_of and not all(_compact_text(term) in compact_text for term in all_of):
            continue
        if any_of and not any(_compact_text(term) in compact_text for term in any_of):
            continue
        item = scores.setdefault(rule["meaning_label"], {
            "meaning_category": rule["meaning_category"],
            "score": 0.0,
            "matched_terms": set(),
        })
        item["score"] += float(rule["score"])
        item["matched_terms"].update(all_of)
        item["matched_terms"].update(term for term in any_of if _compact_text(term) in compact_text)

    for token in set(tokens):
        for match in effective_term_map.get(token, []):
            if bool(match.get("context_required", False)) and not _has_context_terms(
                text,
                token,
                match.get("context_terms", []),
            ):
                continue
            item = scores.setdefault(match["meaning"], {
                "meaning_category": match["category"],
                "score": 0.0,
                "matched_terms": set(),
            })
            item["score"] += float(match["weight"])
            item["matched_terms"].add(token)
    for term, matches in effective_term_map.items():
        if term in text and term not in tokens:
            for match in matches:
                if bool(match.get("context_required", False)) and not _has_context_terms(
                    text,
                    term,
                    match.get("context_terms", []),
                ):
                    continue
                item = scores.setdefault(match["meaning"], {
                    "meaning_category": match["category"],
                    "score": 0.0,
                    "matched_terms": set(),
                })
                item["score"] += float(match["weight"])
                item["matched_terms"].add(term)
    return scores


def _ai_expand_meanings(clean_terms: pd.DataFrame, enabled: bool) -> Dict[str, Dict[str, object]]:
    if not enabled or clean_terms.empty:
        return {}
    candidates = clean_terms[
        ~clean_terms["term"].isin(SEED_TERM_MAP.keys()) &
        (clean_terms["evidence_count"] >= 10)
    ].head(12)
    if candidates.empty:
        return {}

    prompt = {
        "task": "根据高频词条，为古风歌曲评论研究补充二级‘真正含义’标签。",
        "instructions": [
            "输出严格 JSON 数组。",
            "仅在词条具备明确政治/历史语义时给出 keep=true。",
            "一级类只能从既定七类中选择。",
            "meaning_label 应是可统计的二级标签，避免与输入词条完全同义重复。",
        ],
        "categories": SEMANTIC_CATEGORY_ORDER,
        "items": candidates[["term", "evidence_count", "weighted_frequency"]].to_dict("records"),
        "output_schema": {
            "term": "高频词条",
            "keep": "boolean",
            "meaning_category": "一级类",
            "meaning_label": "二级标签",
            "confidence": "0-1 number",
            "reason": "简短中文说明",
        },
    }
    response = run_structured_ai_json(prompt, enabled=True)
    if not isinstance(response, list):
        return {}

    delta: Dict[str, Dict[str, object]] = {}
    for item in response:
        if not isinstance(item, dict) or not item.get("keep"):
            continue
        term = str(item.get("term", "")).strip()
        category = str(item.get("meaning_category", "")).strip()
        label = str(item.get("meaning_label", "")).strip()
        if not term or category not in SEMANTIC_CATEGORY_ORDER or not label:
            continue
        delta[term] = {
            "meaning_category": category,
            "meaning_label": label,
            "confidence": round(float(item.get("confidence", 0) or 0), 4),
            "reason": str(item.get("reason", "")).strip(),
        }
    return delta


def build_meaning_labels(
    comments_df: pd.DataFrame,
    clean_terms_df: pd.DataFrame,
    enable_ai: bool = False,
    effective_term_map: Optional[Dict[str, List[Dict[str, object]]]] = None,
) -> Tuple[pd.DataFrame, Dict[str, object]]:
    if comments_df.empty:
        return pd.DataFrame(), {}

    effective_term_map = effective_term_map or SEED_TERM_MAP
    ontology_delta = _ai_expand_meanings(clean_terms_df, enabled=enable_ai)
    rows = []
    for _, row in comments_df.iterrows():
        content = str(row.get("content", "") or "")
        candidates = _meaning_candidates_from_comment(content, effective_term_map=effective_term_map)
        for term, extra in ontology_delta.items():
            if term in content:
                item = candidates.setdefault(extra["meaning_label"], {
                    "meaning_category": extra["meaning_category"],
                    "score": 0.0,
                    "matched_terms": set(),
                })
                item["score"] += 2.6 + float(extra.get("confidence", 0))
                item["matched_terms"].add(term)

        candidate_rows = [
            {
                "meaning_label": meaning,
                "meaning_category": info["meaning_category"],
                "score": round(float(info["score"]), 4),
                "matched_terms": ",".join(sorted(info["matched_terms"])),
            }
            for meaning, info in candidates.items()
            if info["score"] > 0
        ]
        candidate_rows.sort(key=lambda item: item["score"], reverse=True)

        valid_political = bool(candidate_rows) or float(row.get("political_total_score", 0) or 0) >= 2
        primary = candidate_rows[0] if candidate_rows else None
        secondary = candidate_rows[1:3] if len(candidate_rows) > 1 else []
        rows.append({
            "comment_id": row.get("comment_id", ""),
            "song_key": row.get("song_key", ""),
            "song_name": row.get("song_name", ""),
            "bvid": row.get("bvid", ""),
            "content": content,
            "is_political_historical": valid_political,
            "primary_meaning_category": primary["meaning_category"] if primary else "",
            "primary_meaning_label": primary["meaning_label"] if primary else "",
            "primary_meaning_score": primary["score"] if primary else 0,
            "primary_matched_terms": primary["matched_terms"] if primary else "",
            "secondary_meaning_labels": ",".join(item["meaning_label"] for item in secondary),
            "secondary_matched_terms": ",".join(item["matched_terms"] for item in secondary if item["matched_terms"]),
            "candidate_count": len(candidate_rows),
            "political_total_score": round(float(row.get("political_total_score", 0) or 0), 4),
        })
    return pd.DataFrame(rows), ontology_delta


def build_meaning_distribution(labels_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if labels_df.empty:
        empty = pd.DataFrame(columns=["primary_meaning_label", "comment_count", "share"])
        return empty, empty

    valid = labels_df[
        labels_df["is_political_historical"] &
        labels_df["primary_meaning_label"].astype(str).str.strip().ne("")
    ].copy()
    if valid.empty:
        empty = pd.DataFrame(columns=["primary_meaning_label", "comment_count", "share"])
        return empty, empty

    overall = (
        valid.groupby(["primary_meaning_category", "primary_meaning_label"])
        .size()
        .reset_index(name="comment_count")
        .sort_values("comment_count", ascending=False)
    )
    total = int(overall["comment_count"].sum())
    overall["share"] = overall["comment_count"].apply(lambda n: round(n / total, 6) if total else 0)
    overall["share_pct"] = overall["share"].apply(lambda n: round(n * 100, 2))

    by_song = (
        valid.groupby(["song_key", "song_name", "primary_meaning_category", "primary_meaning_label"])
        .size()
        .reset_index(name="comment_count")
    )
    song_totals = by_song.groupby("song_key")["comment_count"].sum().to_dict()
    by_song["share_within_song"] = by_song.apply(
        lambda r: round(r["comment_count"] / song_totals.get(r["song_key"], 1), 6),
        axis=1,
    )
    by_song["share_within_song_pct"] = by_song["share_within_song"].apply(lambda n: round(n * 100, 2))
    by_song = by_song.sort_values(["song_name", "comment_count"], ascending=[True, False]).reset_index(drop=True)
    return overall.reset_index(drop=True), by_song


def _rule_stance_scores(
    row: pd.Series,
    manual_stance: Optional[Dict[str, object]] = None,
) -> Tuple[Dict[str, float], Dict[str, object]]:
    content = str(row.get("content", "") or "")
    tokens = set(tokenize(content))
    primary_meaning = str(row.get("primary_meaning_label", "")).strip()
    hits = defaultdict(list)
    scores = {stance: 0.0 for stance in STANCE_ORDER if stance != "乐子人"}
    lezi_score = 0.0
    manual_stance = manual_stance or {}

    for stance, rule in STANCE_RULES.items():
        for term in rule["terms"]:
            if term in tokens or term in content:
                scores[stance] += 1.4
                hits[stance].append(term)
        if primary_meaning and primary_meaning in rule["meanings"]:
            scores[stance] += 2.1
            hits[stance].append(f"meaning:{primary_meaning}")
        for axis_name, weight in rule["axis_bonus"].items():
            axis_value = float(row.get(axis_name, 0) or 0)
            if axis_name in ("world_nation_net_score", "liberty_authority_net_score", "progress_conservative_net_score"):
                contribution = (-axis_value if weight < 0 else axis_value) * abs(weight)
            else:
                contribution = axis_value * weight
            if contribution > 0:
                scores[stance] += min(2.4, contribution / 5.0)

    for item in manual_stance.get("term_overrides", []):
        if not isinstance(item, dict):
            continue
        term = str(item.get("term", "")).strip()
        stance = str(item.get("stance", "")).strip()
        if not term or stance not in scores:
            if stance != "乐子人":
                continue
        if term in tokens or term in content:
            if bool(item.get("context_required", False)) and not _has_context_terms(
                content,
                term,
                item.get("context_terms", []),
            ):
                continue
            if stance == "乐子人":
                lezi_score += float(item.get("score", 2.2) or 2.2)
            else:
                scores[stance] += float(item.get("score", 2.2) or 2.2)
            hits[stance].append(f"manual:{term}")

    for item in manual_stance.get("meaning_overrides", []):
        if not isinstance(item, dict):
            continue
        meaning_label = str(item.get("meaning_label", "")).strip()
        stance = str(item.get("stance", "")).strip()
        if not meaning_label or stance not in scores:
            if stance != "乐子人":
                continue
        if primary_meaning == meaning_label:
            if stance == "乐子人":
                lezi_score += float(item.get("score", 2.0) or 2.0)
            else:
                scores[stance] += float(item.get("score", 2.0) or 2.0)
            hits[stance].append(f"manual_meaning:{meaning_label}")

    meta = {
        "matched_keywords": {stance: hits.get(stance, []) for stance in scores},
        "lezi_score": round(lezi_score, 4),
        "lezi_hits": hits.get("乐子人", []),
    }
    return scores, meta


def _ai_refine_stances(candidate_rows: List[Dict[str, object]], enabled: bool) -> Dict[str, Dict[str, object]]:
    if not enabled or not candidate_rows:
        return {}
    prompt = {
        "task": "请为评论分配单主立场：神、左、兔、皇、乐子人。",
        "instructions": [
            "输出严格 JSON 数组。",
            "每条评论只能有一个主立场。",
            "如果评论主要是玩梗、情绪、审美表达、语义不足或政治含义不明确，请标为乐子人。",
            "神、左、兔、皇是本研究的操作性标签，不要扩展出新标签。",
        ],
        "items": candidate_rows,
        "output_schema": {
            "comment_id": "评论ID",
            "stance": "神 | 左 | 兔 | 皇 | 乐子人",
            "confidence": "0-1 number",
            "reason": "简短中文说明",
        },
    }
    response = run_structured_ai_json(prompt, enabled=True)
    if not isinstance(response, list):
        return {}
    result = {}
    for item in response:
        if not isinstance(item, dict):
            continue
        comment_id = str(item.get("comment_id", "")).strip()
        stance = str(item.get("stance", "乐子人")).strip()
        if comment_id and stance in STANCE_ORDER:
            result[comment_id] = {
                "stance": stance,
                "confidence": round(float(item.get("confidence", 0) or 0), 4),
                "reason": str(item.get("reason", "")).strip(),
            }
    return result


def build_stance_labels(
    labels_df: pd.DataFrame,
    enable_ai: bool = False,
    manual_stance: Optional[Dict[str, object]] = None,
) -> pd.DataFrame:
    if labels_df.empty:
        return pd.DataFrame()

    records = []
    ai_candidates = []
    for _, row in labels_df.iterrows():
        scores, meta = _rule_stance_scores(row, manual_stance=manual_stance)
        ordered = sorted(scores.items(), key=lambda item: item[1], reverse=True)
        best_stance, best_score = ordered[0] if ordered else ("乐子人", 0)
        second_score = ordered[1][1] if len(ordered) > 1 else 0
        confidence = 0.0
        if best_score > 0:
            confidence = round(best_score / max(best_score + second_score, 1e-6), 4)
        if best_score < 1.6 or confidence < 0.58 or not bool(row.get("is_political_historical", False)):
            best_stance = "乐子人"
        if float(meta.get("lezi_score", 0) or 0) >= max(best_score + 0.5, 2.8):
            best_stance = "乐子人"
            confidence = min(1.0, round(float(meta.get("lezi_score", 0)) / 4.0, 4))

        record = {
            "comment_id": row.get("comment_id", ""),
            "song_key": row.get("song_key", ""),
            "song_name": row.get("song_name", ""),
            "bvid": row.get("bvid", ""),
            "content": row.get("content", ""),
            "primary_meaning_label": row.get("primary_meaning_label", ""),
            "stance": best_stance,
            "stance_confidence": confidence,
            "stance_reason": f"rule:{'; '.join(f'{k}={v:.2f}' for k, v in ordered[:3])}" if ordered else "rule:none",
            "matched_keywords": json.dumps({
                **meta["matched_keywords"],
                "乐子人": meta.get("lezi_hits", []),
            }, ensure_ascii=False),
            "political_total_score": row.get("political_total_score", 0),
        }
        records.append(record)

        if enable_ai and (
            best_stance == "乐子人" and bool(row.get("is_political_historical", False))
            or (best_score > 0 and abs(best_score - second_score) < 1.2)
        ):
            ai_candidates.append({
                "comment_id": row.get("comment_id", ""),
                "content": str(row.get("content", ""))[:300],
                "primary_meaning_label": row.get("primary_meaning_label", ""),
                "rule_scores": {k: round(v, 3) for k, v in ordered},
                "matched_keywords": meta["matched_keywords"],
            })

    ai_map = {}
    if ai_candidates:
        selected = ai_candidates[: min(80, len(ai_candidates))]
        for start in range(0, len(selected), 20):
            ai_map.update(_ai_refine_stances(selected[start:start + 20], enabled=True))

    for record in records:
        reviewed = ai_map.get(str(record["comment_id"]))
        if reviewed:
            record["stance"] = reviewed["stance"] if reviewed["confidence"] >= 0.65 else "乐子人"
            record["stance_confidence"] = reviewed["confidence"]
            record["stance_reason"] = reviewed["reason"] or record["stance_reason"]

    return pd.DataFrame(records)


def build_stance_distribution(stance_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if stance_df.empty:
        empty = pd.DataFrame(columns=["stance", "comment_count", "share"])
        return empty, empty

    overall = (
        stance_df.groupby("stance")
        .size()
        .reindex(STANCE_ORDER, fill_value=0)
        .reset_index(name="comment_count")
    )
    total = int(overall["comment_count"].sum())
    overall["share"] = overall["comment_count"].apply(lambda n: round(n / total, 6) if total else 0)
    overall["share_pct"] = overall["share"].apply(lambda n: round(n * 100, 2))

    by_song = (
        stance_df.groupby(["song_key", "song_name", "stance"])
        .size()
        .reset_index(name="comment_count")
    )
    song_totals = by_song.groupby("song_key")["comment_count"].sum().to_dict()
    by_song["share_within_song"] = by_song.apply(
        lambda r: round(r["comment_count"] / song_totals.get(r["song_key"], 1), 6),
        axis=1,
    )
    by_song["share_within_song_pct"] = by_song["share_within_song"].apply(lambda n: round(n * 100, 2))
    by_song["stance"] = pd.Categorical(by_song["stance"], categories=STANCE_ORDER, ordered=True)
    by_song = by_song.sort_values(["song_name", "stance"]).reset_index(drop=True)
    return overall, by_song


def build_song_level_summary(
    songs_df: pd.DataFrame,
    meaning_labels_df: pd.DataFrame,
    stance_df: pd.DataFrame,
) -> pd.DataFrame:
    if songs_df.empty:
        return pd.DataFrame()

    meaning_valid = meaning_labels_df[meaning_labels_df["is_political_historical"]].copy()
    political_counts = meaning_valid.groupby(["song_key", "song_name"]).size().reset_index(name="political_comment_count")

    top_meanings = (
        meaning_valid[meaning_valid["primary_meaning_label"].astype(str).str.strip().ne("")]
        .groupby(["song_key", "song_name", "primary_meaning_label"])
        .size()
        .reset_index(name="count")
        .sort_values(["song_key", "count"], ascending=[True, False])
    )
    top_meanings = top_meanings.groupby("song_key").head(3)
    top_meaning_map = (
        top_meanings.groupby("song_key")
        .apply(lambda df: " / ".join(
            f"{row['primary_meaning_label']} ({int(row['count'])})" for _, row in df.iterrows()
        ))
        .to_dict()
    )

    top_stances = (
        stance_df.groupby(["song_key", "song_name", "stance"])
        .size()
        .reset_index(name="count")
        .sort_values(["song_key", "count"], ascending=[True, False])
        .groupby("song_key")
        .head(2)
    )
    top_stance_map = (
        top_stances.groupby("song_key")
        .apply(lambda df: " / ".join(f"{row['stance']} ({int(row['count'])})" for _, row in df.iterrows()))
        .to_dict()
    )

    summary = songs_df.copy()
    summary = summary.merge(political_counts, on=["song_key", "song_name"], how="left")
    summary["political_comment_count"] = summary["political_comment_count"].fillna(0).astype(int)
    summary["political_comment_share"] = summary.apply(
        lambda r: round(r["political_comment_count"] / r["comment_count"], 6) if r["comment_count"] else 0,
        axis=1,
    )
    summary["top_meanings"] = summary["song_key"].map(top_meaning_map).fillna("")
    summary["top_stances"] = summary["song_key"].map(top_stance_map).fillna("")
    return summary.sort_values("song_name").reset_index(drop=True)


def _split_composite_segments(text: str) -> List[str]:
    values = [
        segment.strip()
        for segment in re.split(r"[\n\r。！？!?；;]+", str(text or ""))
        if segment.strip()
    ]
    return values or [str(text or "").strip()]


def build_composite_rule_summary(comments_df: pd.DataFrame) -> pd.DataFrame:
    rules = [rule for rule in load_manual_composite_rules() if rule.get("enabled") is not False]
    if not rules:
        return pd.DataFrame(columns=[
            "rule_id", "terms", "window", "semantic_category", "meaning_label",
            "semantic_weight", "stance", "stance_score", "confidence",
            "matched_comment_count", "matched_song_count", "share_of_comments_pct",
            "reason",
        ])

    total_comments = int(len(comments_df)) if comments_df is not None else 0
    rows = []
    for rule in rules:
        terms = [str(term).strip() for term in rule.get("terms", []) if str(term).strip()]
        if len(terms) < 2:
            continue
        compact_terms = [_compact_text(term) for term in terms if _compact_text(term)]
        if len(compact_terms) != len(terms):
            continue
        window = str(rule.get("window", "adjacent_segments") or "adjacent_segments")
        matched_comments = 0
        matched_song_keys = set()

        for _, comment in comments_df.iterrows():
            content = str(comment.get("content", "") or "")
            if not content.strip():
                continue
            segments = _split_composite_segments(content)
            if window == "full_text":
                candidate_windows = [content]
            elif window == "same_segment":
                candidate_windows = segments
            else:
                candidate_windows = []
                for idx, segment in enumerate(segments):
                    neighbor = segments[idx + 1] if idx + 1 < len(segments) else ""
                    candidate_windows.append(f"{segment}\n{neighbor}".strip())
                if not candidate_windows:
                    candidate_windows = segments

            is_match = False
            for candidate in candidate_windows:
                compact_candidate = _compact_text(candidate)
                if compact_candidate and all(term in compact_candidate for term in compact_terms):
                    is_match = True
                    break
            if not is_match:
                continue
            matched_comments += 1
            matched_song_keys.add(str(comment.get("song_key", "")).strip())

        rows.append({
            "rule_id": str(rule.get("rule_id", "")).strip() or "+".join(terms),
            "terms": " + ".join(terms),
            "window": window,
            "semantic_category": str(rule.get("semantic_category", "")).strip(),
            "meaning_label": str(rule.get("meaning_label", "")).strip(),
            "semantic_weight": round(float(rule.get("semantic_weight", 0) or 0), 4),
            "stance": str(rule.get("stance", "")).strip(),
            "stance_score": round(float(rule.get("stance_score", 0) or 0), 4),
            "confidence": round(float(rule.get("confidence", 0) or 0), 4),
            "matched_comment_count": int(matched_comments),
            "matched_song_count": int(len({key for key in matched_song_keys if key})),
            "share_of_comments_pct": round(matched_comments / total_comments * 100, 2) if total_comments else 0,
            "reason": str(rule.get("reason", "")).strip(),
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.sort_values(
        ["matched_comment_count", "semantic_weight", "stance_score"],
        ascending=[False, False, False],
    ).reset_index(drop=True)


def _save_figure(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(path, dpi=180)
    plt.close()


def create_summary_figures(
    output_dir: Path,
    meaning_overall_df: pd.DataFrame,
    stance_overall_df: pd.DataFrame,
    meaning_by_song_df: pd.DataFrame,
    stance_by_song_df: pd.DataFrame,
) -> Dict[str, str]:
    figures_dir = output_dir / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)
    _setup_plot()
    outputs: Dict[str, str] = {}

    if not meaning_overall_df.empty:
        top = meaning_overall_df.head(12).sort_values("comment_count", ascending=True)
        plt.figure(figsize=(10, 6))
        sns.barplot(data=top, x="comment_count", y="primary_meaning_label", hue="primary_meaning_category", dodge=False)
        plt.title("全部歌曲真正含义 Top 12")
        plt.xlabel("评论数")
        plt.ylabel("真正含义")
        _save_figure(figures_dir / "overall_meanings.png")
        outputs["overall_meanings"] = "overall_meanings.png"

    if not stance_overall_df.empty:
        plt.figure(figsize=(8, 5))
        palette = ["#5B8FF9", "#61DDAA", "#F6BD16", "#E8684A", "#BFBFBF"]
        sns.barplot(data=stance_overall_df, x="share", y="stance", hue="stance", palette=palette, legend=False)
        plt.title("全部歌曲立场占比")
        plt.xlabel("占比")
        plt.ylabel("立场")
        _save_figure(figures_dir / "overall_stances.png")
        outputs["overall_stances"] = "overall_stances.png"

    if not stance_by_song_df.empty:
        matrix = stance_by_song_df.pivot_table(
            index="song_name", columns="stance", values="share_within_song", fill_value=0
        )
        matrix = matrix.reindex(columns=STANCE_ORDER, fill_value=0)
        plt.figure(figsize=(10, max(4, len(matrix) * 0.45)))
        sns.heatmap(matrix, cmap="Reds", annot=True, fmt=".2f")
        plt.title("歌曲 × 立场 热力图")
        plt.xlabel("立场")
        plt.ylabel("歌曲")
        _save_figure(figures_dir / "song_stance_heatmap.png")
        outputs["song_stance_heatmap"] = "song_stance_heatmap.png"

    if not meaning_by_song_df.empty:
        top_labels = meaning_overall_df.head(10)["primary_meaning_label"].tolist()
        matrix = meaning_by_song_df[
            meaning_by_song_df["primary_meaning_label"].isin(top_labels)
        ].pivot_table(
            index="song_name", columns="primary_meaning_label", values="share_within_song", fill_value=0
        )
        plt.figure(figsize=(12, max(4, len(matrix) * 0.45)))
        sns.heatmap(matrix, cmap="Blues", annot=True, fmt=".2f")
        plt.title("歌曲 × 真正含义 热力图")
        plt.xlabel("真正含义")
        plt.ylabel("歌曲")
        _save_figure(figures_dir / "song_meaning_heatmap.png")
        outputs["song_meaning_heatmap"] = "song_meaning_heatmap.png"

    return outputs


def _markdown_table(df: pd.DataFrame, max_rows: int = 20) -> str:
    if df is None or df.empty:
        return "暂无数据\n"
    view = df.head(max_rows).fillna("").astype(str)
    columns = list(view.columns)
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join(["---"] * len(columns)) + " |",
    ]
    for _, row in view.iterrows():
        values = [str(row[col]).replace("|", "\\|").replace("\n", " ") for col in columns]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def build_summary_markdown(
    generated_at: str,
    song_summary_df: pd.DataFrame,
    clean_terms_df: pd.DataFrame,
    excluded_terms_df: pd.DataFrame,
    review_queue_df: pd.DataFrame,
    meaning_overall_df: pd.DataFrame,
    stance_overall_df: pd.DataFrame,
    composite_rule_df: pd.DataFrame,
    figures: Dict[str, str],
) -> str:
    total_songs = int(len(song_summary_df))
    total_comments = int(song_summary_df["comment_count"].sum()) if not song_summary_df.empty else 0
    total_political = int(song_summary_df["political_comment_count"].sum()) if not song_summary_df.empty else 0

    lines = [
        "# 古风歌曲评论深度清洗与总报告",
        "",
        "## 数据范围与方法说明",
        "",
        f"- 生成时间：{generated_at}",
        f"- 纳入歌曲数：{total_songs}",
        f"- 合并匿名评论数：{total_comments}",
        f"- 有效政治/历史评论数：{total_political}",
        "- 本报告只读取 `result/*/data/` 既有产物，不重新爬取 B 站。",
        "- `神 / 左 / 兔 / 皇 / 乐子人` 为本项目研究性操作定义，不作为稳定学术分类宣称。",
        "- “真正含义占比”按评论主标签聚合，不直接等同于词频。",
        "",
        "## 样本总览",
        "",
        _markdown_table(song_summary_df[[
            "song_name", "comment_count", "political_comment_count", "political_comment_share", "top_meanings", "top_stances"
        ]], 20),
        "",
        "## 全局真正含义占比",
        "",
        _markdown_table(meaning_overall_df[[
            "primary_meaning_category", "primary_meaning_label", "comment_count", "share_pct"
        ]].head(20), 20),
        "",
        "## 全局立场占比",
        "",
        _markdown_table(stance_overall_df[["stance", "comment_count", "share_pct"]], 20),
        "",
        "## 深度清洗后保留词条",
        "",
        _markdown_table(clean_terms_df[[
            "term", "semantic_category", "evidence_count", "decision_source", "confidence"
        ]], 30),
        "",
        "## 被排除词条",
        "",
        _markdown_table(excluded_terms_df[[
            "term", "evidence_count", "decision_source", "confidence", "reason"
        ]], 30),
        "",
        "## 待复核词条",
        "",
        _markdown_table(review_queue_df[[
            "term", "evidence_count", "decision_source", "confidence", "reason"
        ]], 30),
        "",
        "## 组合词构成及权重",
        "",
        _markdown_table(composite_rule_df[[
            "terms", "window", "meaning_label", "semantic_weight", "stance", "stance_score",
            "matched_comment_count", "matched_song_count", "share_of_comments_pct"
        ]], 30) if composite_rule_df is not None and not composite_rule_df.empty else "暂无数据\n",
        "",
        "## 图表",
        "",
    ]

    titles = {
        "overall_meanings": "全部歌曲真正含义 Top 12",
        "overall_stances": "全部歌曲立场占比",
        "song_stance_heatmap": "歌曲 × 立场 热力图",
        "song_meaning_heatmap": "歌曲 × 真正含义 热力图",
    }
    for key, filename in figures.items():
        lines.extend([
            f"### {titles.get(key, key)}",
            "",
            f"![{titles.get(key, key)}](figures/{filename})",
            "",
        ])
    lines.extend([
        "## 每首歌摘要",
        "",
    ])
    for _, row in song_summary_df.iterrows():
        lines.extend([
            f"### {row['song_name']}",
            "",
            f"- 评论数：{row['comment_count']}",
            f"- 有效政治/历史评论：{row['political_comment_count']} ({round(float(row['political_comment_share']) * 100, 2)}%)",
            f"- Top 真正含义：{row['top_meanings'] or '暂无'}",
            f"- Top 立场：{row['top_stances'] or '暂无'}",
            "",
        ])
    return "\n".join(lines)


def write_summary_reports(output_dir: Path, markdown_text: str) -> Dict[str, str]:
    md_path = output_dir / "report.md"
    html_path = output_dir / "report.html"
    md_path.write_text(markdown_text, encoding="utf-8")
    html_path.write_text(markdown_to_html(markdown_text, "古风歌曲评论深度清洗与总报告"), encoding="utf-8")
    return {"markdown": str(md_path), "html": str(html_path)}


def _write_json(path: Path, payload: object):
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def build_song_deep_cleaning_markdown(
    generated_at: str,
    song_name: str,
    source_dir: Path,
    raw_word_frequency_df: pd.DataFrame,
    raw_tfidf_df: pd.DataFrame,
    song_summary_df: pd.DataFrame,
    clean_terms_df: pd.DataFrame,
    excluded_terms_df: pd.DataFrame,
    review_queue_df: pd.DataFrame,
    meaning_overall_df: pd.DataFrame,
    stance_overall_df: pd.DataFrame,
    composite_rule_df: pd.DataFrame,
    figures: Dict[str, str],
) -> str:
    song_row = song_summary_df.iloc[0] if not song_summary_df.empty else pd.Series(dtype=object)
    comment_count = int(song_row.get("comment_count", 0) or 0)
    political_count = int(song_row.get("political_comment_count", 0) or 0)
    political_share = float(song_row.get("political_comment_share", 0) or 0)

    lines = [
        f"# {song_name} 深度清洗报告",
        "",
        "## 数据范围与说明",
        "",
        f"- 生成时间：{generated_at}",
        f"- 原始歌曲目录：`{source_dir}`",
        f"- 评论数：{comment_count}",
        f"- 有效政治/历史评论数：{political_count} ({round(political_share * 100, 2)}%)",
        "- 本报告基于该歌曲目录下既有匿名评论与分析文件重洗，不重新抓取 B 站。",
        "- 原始词频与 TF-IDF 保留作对照，深度清洗后的词条、真正含义和立场占比作为新版研究结果。",
        "",
        "## 原始词频对照",
        "",
        _markdown_table(raw_word_frequency_df[["keyword", "frequency"]], 25) if not raw_word_frequency_df.empty else "暂无数据\n",
        "",
        "## 原始 TF-IDF 对照",
        "",
        _markdown_table(raw_tfidf_df[["keyword", "tfidf"]], 25) if not raw_tfidf_df.empty else "暂无数据\n",
        "",
        "## 深度清洗后保留词条",
        "",
        _markdown_table(clean_terms_df[[
            "term", "semantic_category", "evidence_count", "weighted_frequency", "decision_source", "confidence"
        ]], 30),
        "",
        "## 被排除词条",
        "",
        _markdown_table(excluded_terms_df[[
            "term", "evidence_count", "decision_source", "confidence", "reason"
        ]], 25),
        "",
        "## 待复核词条",
        "",
        _markdown_table(review_queue_df[[
            "term", "evidence_count", "decision_source", "confidence", "reason"
        ]], 25),
        "",
        "## 真正含义占比",
        "",
        _markdown_table(meaning_overall_df[[
            "primary_meaning_category", "primary_meaning_label", "comment_count", "share_pct"
        ]], 20),
        "",
        "## 立场占比",
        "",
        _markdown_table(stance_overall_df[["stance", "comment_count", "share_pct"]], 10),
        "",
        "## 组合词构成及权重",
        "",
        _markdown_table(composite_rule_df[[
            "terms", "window", "meaning_label", "semantic_weight", "stance", "stance_score",
            "matched_comment_count", "share_of_comments_pct"
        ]], 20) if composite_rule_df is not None and not composite_rule_df.empty else "暂无数据\n",
        "",
        "## 单曲摘要",
        "",
        f"- Top 真正含义：{song_row.get('top_meanings', '暂无') or '暂无'}",
        f"- Top 立场：{song_row.get('top_stances', '暂无') or '暂无'}",
        "",
        "## 图表",
        "",
    ]

    titles = {
        "overall_meanings": "真正含义 Top 12",
        "overall_stances": "立场占比",
        "song_stance_heatmap": "单曲立场热力图",
        "song_meaning_heatmap": "单曲真正含义热力图",
    }
    for key, filename in figures.items():
        lines.extend([
            f"### {titles.get(key, key)}",
            "",
            f"![{titles.get(key, key)}](figures/{filename})",
            "",
        ])

    lines.extend([
        "## 数据文件",
        "",
        "- `data/comments_deep_cleaned_v2.csv`：合并单曲评论、真正含义与立场标签。",
        "- `data/clean_terms_v2.csv`：深度清洗后保留词条。",
        "- `data/excluded_terms_v2.csv`：被排除词条。",
        "- `data/semantic_review_queue_v2.csv`：待复核词条。",
        "- `data/meaning_labels_comments_v2.csv`：评论级真正含义标签。",
        "- `data/meaning_distribution_overall_v2.csv`：单曲真正含义分布。",
        "- `data/stance_labels_comments_v2.csv`：评论级立场标签。",
        "- `data/stance_distribution_overall_v2.csv`：单曲立场分布。",
        "- `data/composite_rule_summary_v2.csv`：命中的组合词规则摘要。",
        "- `data/overall_summary_v2.json`：本次单曲重洗统计摘要。",
        "",
    ])
    return "\n".join(lines)


def write_song_deep_cleaning_reports(output_dir: Path, markdown_text: str) -> Dict[str, str]:
    md_path = output_dir / "report.md"
    html_path = output_dir / "report.html"
    md_path.write_text(markdown_text, encoding="utf-8")
    html_path.write_text(markdown_to_html(markdown_text, "单曲深度清洗报告"), encoding="utf-8")
    return {"markdown": str(md_path), "html": str(html_path)}


def _compute_deep_cleaning_outputs(
    comments_df: pd.DataFrame,
    bundle: Dict[str, pd.DataFrame],
    enable_ai: bool = False,
) -> Dict[str, object]:
    manual_overrides = load_manual_lexicon_overrides()
    effective_term_map = build_effective_term_map(manual_overrides["semantic"])
    term_evidence_df = _build_term_evidence(bundle)
    term_outputs = semantic_clean_terms(
        term_evidence_df,
        enable_ai=enable_ai,
        manual_semantic=manual_overrides["semantic"],
        effective_term_map=effective_term_map,
    )
    meaning_labels_df, ontology_delta = build_meaning_labels(
        comments_df,
        term_outputs["clean_terms"],
        enable_ai=enable_ai,
        effective_term_map=effective_term_map,
    )
    stance_source_df = comments_df.merge(
        meaning_labels_df[[
            "comment_id", "song_key", "song_name", "bvid", "is_political_historical", "primary_meaning_label",
            "primary_meaning_category", "primary_meaning_score", "primary_matched_terms", "secondary_meaning_labels"
        ]],
        on=["comment_id", "song_key", "song_name", "bvid"],
        how="left",
    )
    stance_df = build_stance_labels(
        stance_source_df,
        enable_ai=enable_ai,
        manual_stance=manual_overrides["stance"],
    )
    meaning_overall_df, meaning_by_song_df = build_meaning_distribution(meaning_labels_df)
    stance_overall_df, stance_by_song_df = build_stance_distribution(stance_df)
    song_level_summary_df = build_song_level_summary(bundle["songs"], meaning_labels_df, stance_df)
    composite_rule_df = build_composite_rule_summary(comments_df)
    combined_comments_df = comments_df.merge(
        meaning_labels_df.drop(columns=["content"], errors="ignore"),
        on=["comment_id", "song_key", "song_name"],
        how="left",
    ).merge(
        stance_df.drop(columns=["content"], errors="ignore"),
        on=["comment_id", "song_key", "song_name", "primary_meaning_label"],
        how="left",
    )
    overall_summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "song_count": int(len(bundle["songs"])),
        "comment_count": int(len(comments_df)),
        "political_comment_count": int(meaning_labels_df["is_political_historical"].sum()) if not meaning_labels_df.empty else 0,
        "clean_term_count": int(len(term_outputs["clean_terms"])),
        "excluded_term_count": int(len(term_outputs["excluded_terms"])),
        "review_queue_count": int(len(term_outputs["review_queue"])),
        "enable_ai": bool(enable_ai),
        "top_meanings": meaning_overall_df.head(10).to_dict("records") if not meaning_overall_df.empty else [],
        "stance_distribution": stance_overall_df.to_dict("records") if not stance_overall_df.empty else [],
        "composite_rule_count": int(len(composite_rule_df)),
    }
    return {
        "manual_overrides": manual_overrides,
        "term_outputs": term_outputs,
        "meaning_labels_df": meaning_labels_df,
        "ontology_delta": ontology_delta,
        "stance_df": stance_df,
        "meaning_overall_df": meaning_overall_df,
        "meaning_by_song_df": meaning_by_song_df,
        "stance_overall_df": stance_overall_df,
        "stance_by_song_df": stance_by_song_df,
        "song_level_summary_df": song_level_summary_df,
        "composite_rule_df": composite_rule_df,
        "combined_comments_df": combined_comments_df,
        "overall_summary": overall_summary,
    }


def create_summary_output_dir(base_root: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = base_root / f"summary_{timestamp}"
    (output_dir / "data").mkdir(parents=True, exist_ok=True)
    (output_dir / "figures").mkdir(parents=True, exist_ok=True)
    return output_dir


def run_deep_cleaning_summary(
    result_root: Path,
    output_dir: Optional[Path] = None,
    enable_ai: bool = False,
) -> Dict[str, object]:
    result_root = Path(result_root)
    if not result_root.exists():
        raise FileNotFoundError(f"Result root not found: {result_root}")

    output_dir = Path(output_dir) if output_dir else create_summary_output_dir(result_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "data").mkdir(parents=True, exist_ok=True)
    (output_dir / "figures").mkdir(parents=True, exist_ok=True)

    comments_df, bundle = load_result_bundle(result_root)
    if comments_df.empty:
        raise RuntimeError("No song result bundles were found under result/")

    outputs = _compute_deep_cleaning_outputs(comments_df, bundle, enable_ai=enable_ai)
    manual_overrides = outputs["manual_overrides"]
    term_outputs = outputs["term_outputs"]
    meaning_labels_df = outputs["meaning_labels_df"]
    ontology_delta = outputs["ontology_delta"]
    stance_df = outputs["stance_df"]
    meaning_overall_df = outputs["meaning_overall_df"]
    meaning_by_song_df = outputs["meaning_by_song_df"]
    stance_overall_df = outputs["stance_overall_df"]
    stance_by_song_df = outputs["stance_by_song_df"]
    song_level_summary_df = outputs["song_level_summary_df"]
    composite_rule_df = outputs["composite_rule_df"]
    combined_comments_df = outputs["combined_comments_df"]
    overall_summary = outputs["overall_summary"]

    data_dir = output_dir / "data"
    combined_comments_df.to_csv(data_dir / "combined_comments_cleaned.csv", index=False, encoding=CSV_ENCODING)
    meaning_labels_df.to_csv(data_dir / "combined_meanings.csv", index=False, encoding=CSV_ENCODING)
    meaning_labels_df.to_csv(data_dir / "meaning_labels_comments.csv", index=False, encoding=CSV_ENCODING)
    stance_df.to_csv(data_dir / "combined_stances.csv", index=False, encoding=CSV_ENCODING)
    stance_df.to_csv(data_dir / "stance_labels_comments.csv", index=False, encoding=CSV_ENCODING)
    song_level_summary_df.to_csv(data_dir / "song_level_summary.csv", index=False, encoding=CSV_ENCODING)
    term_outputs["clean_terms"].to_csv(data_dir / "clean_terms.csv", index=False, encoding=CSV_ENCODING)
    term_outputs["excluded_terms"].to_csv(data_dir / "excluded_terms.csv", index=False, encoding=CSV_ENCODING)
    term_outputs["review_queue"].to_csv(data_dir / "semantic_review_queue.csv", index=False, encoding=CSV_ENCODING)
    meaning_overall_df.to_csv(data_dir / "meaning_distribution_overall.csv", index=False, encoding=CSV_ENCODING)
    meaning_by_song_df.to_csv(data_dir / "meaning_distribution_by_song.csv", index=False, encoding=CSV_ENCODING)
    stance_overall_df.to_csv(data_dir / "stance_distribution_overall.csv", index=False, encoding=CSV_ENCODING)
    stance_by_song_df.to_csv(data_dir / "stance_distribution_by_song.csv", index=False, encoding=CSV_ENCODING)
    composite_rule_df.to_csv(data_dir / "composite_rule_summary.csv", index=False, encoding=CSV_ENCODING)

    ontology_seed_payload = {
        "categories": SEMANTIC_ONTOLOGY_SEED,
        "stance_definitions": {
            stance: {
                **rule,
                "meanings": sorted(rule.get("meanings", [])),
            }
            for stance, rule in STANCE_RULES.items()
        },
        "manual_semantic_overrides_path": str(MANUAL_SEMANTIC_OVERRIDES_PATH),
        "manual_stance_overrides_path": str(MANUAL_STANCE_OVERRIDES_PATH),
    }
    _write_json(data_dir / "ontology_seed.json", ontology_seed_payload)
    _write_json(data_dir / "ontology_delta.json", ontology_delta)
    _write_json(data_dir / "manual_semantic_overrides_snapshot.json", manual_overrides["semantic"])
    _write_json(data_dir / "manual_stance_overrides_snapshot.json", manual_overrides["stance"])
    for doc_name in ("README.md", "SOURCES.md"):
        source_doc = MANUAL_LEXICON_DIR / doc_name
        if source_doc.exists():
            shutil.copy2(source_doc, data_dir / f"manual_lexicon_{doc_name.lower()}")
    _write_json(data_dir / "overall_summary.json", overall_summary)

    figures = create_summary_figures(
        output_dir,
        meaning_overall_df,
        stance_overall_df,
        meaning_by_song_df,
        stance_by_song_df,
    )
    markdown = build_summary_markdown(
        generated_at=overall_summary["generated_at"],
        song_summary_df=song_level_summary_df,
        clean_terms_df=term_outputs["clean_terms"],
        excluded_terms_df=term_outputs["excluded_terms"],
        review_queue_df=term_outputs["review_queue"],
        meaning_overall_df=meaning_overall_df,
        stance_overall_df=stance_overall_df,
        composite_rule_df=composite_rule_df,
        figures=figures,
    )
    reports = write_summary_reports(output_dir, markdown)

    return {
        "output_dir": str(output_dir),
        "reports": reports,
        "song_count": int(len(bundle["songs"])),
        "comment_count": int(len(comments_df)),
        "political_comment_count": int(overall_summary["political_comment_count"]),
        "clean_term_count": int(len(term_outputs["clean_terms"])),
        "stance_rows": int(len(stance_df)),
        "composite_rule_rows": int(len(composite_rule_df)),
    }


def run_song_deep_cleaning(
    song_dir: Path,
    output_dir: Optional[Path] = None,
    enable_ai: bool = False,
) -> Dict[str, object]:
    song_dir = Path(song_dir)
    comments_df, bundle = load_song_result_bundle(song_dir)
    if comments_df.empty:
        raise RuntimeError(f"No usable song data found under {song_dir}")

    output_dir = Path(output_dir) if output_dir else song_dir / "deep_cleaning_v2"
    data_dir = output_dir / "data"
    figures_dir = output_dir / "figures"
    data_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    outputs = _compute_deep_cleaning_outputs(comments_df, bundle, enable_ai=enable_ai)
    term_outputs = outputs["term_outputs"]
    meaning_labels_df = outputs["meaning_labels_df"]
    stance_df = outputs["stance_df"]
    meaning_overall_df = outputs["meaning_overall_df"]
    meaning_by_song_df = outputs["meaning_by_song_df"]
    stance_overall_df = outputs["stance_overall_df"]
    stance_by_song_df = outputs["stance_by_song_df"]
    song_level_summary_df = outputs["song_level_summary_df"]
    composite_rule_df = outputs["composite_rule_df"]
    combined_comments_df = outputs["combined_comments_df"]
    overall_summary = outputs["overall_summary"]

    combined_comments_df.to_csv(data_dir / "comments_deep_cleaned_v2.csv", index=False, encoding=CSV_ENCODING)
    term_outputs["clean_terms"].to_csv(data_dir / "clean_terms_v2.csv", index=False, encoding=CSV_ENCODING)
    term_outputs["excluded_terms"].to_csv(data_dir / "excluded_terms_v2.csv", index=False, encoding=CSV_ENCODING)
    term_outputs["review_queue"].to_csv(data_dir / "semantic_review_queue_v2.csv", index=False, encoding=CSV_ENCODING)
    meaning_labels_df.to_csv(data_dir / "meaning_labels_comments_v2.csv", index=False, encoding=CSV_ENCODING)
    meaning_overall_df.to_csv(data_dir / "meaning_distribution_overall_v2.csv", index=False, encoding=CSV_ENCODING)
    meaning_by_song_df.to_csv(data_dir / "meaning_distribution_by_song_v2.csv", index=False, encoding=CSV_ENCODING)
    stance_df.to_csv(data_dir / "stance_labels_comments_v2.csv", index=False, encoding=CSV_ENCODING)
    stance_overall_df.to_csv(data_dir / "stance_distribution_overall_v2.csv", index=False, encoding=CSV_ENCODING)
    stance_by_song_df.to_csv(data_dir / "stance_distribution_by_song_v2.csv", index=False, encoding=CSV_ENCODING)
    composite_rule_df.to_csv(data_dir / "composite_rule_summary_v2.csv", index=False, encoding=CSV_ENCODING)
    song_level_summary_df.to_csv(data_dir / "song_level_summary_v2.csv", index=False, encoding=CSV_ENCODING)
    _write_json(data_dir / "overall_summary_v2.json", overall_summary)
    _write_json(data_dir / "ontology_delta_v2.json", outputs["ontology_delta"])

    figures = create_summary_figures(
        output_dir,
        meaning_overall_df,
        stance_overall_df,
        meaning_by_song_df,
        stance_by_song_df,
    )
    raw_word_frequency_df = bundle.get("word_frequency", pd.DataFrame())
    raw_tfidf_df = bundle.get("tfidf", pd.DataFrame())
    song_name = str(song_level_summary_df.iloc[0]["song_name"]) if not song_level_summary_df.empty else song_dir.name
    markdown = build_song_deep_cleaning_markdown(
        generated_at=overall_summary["generated_at"],
        song_name=song_name,
        source_dir=song_dir,
        raw_word_frequency_df=raw_word_frequency_df,
        raw_tfidf_df=raw_tfidf_df,
        song_summary_df=song_level_summary_df,
        clean_terms_df=term_outputs["clean_terms"],
        excluded_terms_df=term_outputs["excluded_terms"],
        review_queue_df=term_outputs["review_queue"],
        meaning_overall_df=meaning_overall_df,
        stance_overall_df=stance_overall_df,
        composite_rule_df=composite_rule_df,
        figures=figures,
    )
    reports = write_song_deep_cleaning_reports(output_dir, markdown)

    return {
        "song_dir": str(song_dir),
        "output_dir": str(output_dir),
        "reports": reports,
        "comment_count": int(len(comments_df)),
        "political_comment_count": int(overall_summary["political_comment_count"]),
        "clean_term_count": int(len(term_outputs["clean_terms"])),
    }


def run_deep_cleaning_backfill(
    result_root: Path,
    enable_ai: bool = False,
) -> Dict[str, object]:
    result_root = Path(result_root)
    if not result_root.exists():
        raise FileNotFoundError(f"Result root not found: {result_root}")

    song_dirs = _iter_song_dirs(result_root)
    results = []
    for song_dir in song_dirs:
        results.append(run_song_deep_cleaning(song_dir, enable_ai=enable_ai))
    return {
        "result_root": str(result_root),
        "song_count": len(results),
        "songs": results,
    }
