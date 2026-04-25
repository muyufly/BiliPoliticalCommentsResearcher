"""
AI 辅助政治坐标轴词库自迭代。

流程：
1. 取原始词频最高的 5 个词。
2. 反向定位包含该词的评论，随机抽取最多 10 条。
3. 让 AI 理解语境，判断该词是否有明显四轴倾向。
4. 若某一维度/极性偏好置信度 >= 70%，写入本次增量词库。
"""
import json
import random
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from config.config import CSV_ENCODING
from src.research.ai_analyzer import run_structured_ai_json
from src.research.political_axis import POLITICAL_AXIS_SKILL


ITERATION_COLUMNS = [
    "keyword",
    "frequency",
    "sample_size",
    "dimension",
    "dimension_label",
    "pole",
    "pole_label",
    "confidence",
    "weight",
    "accepted",
    "reason",
]


def _contains_keyword(content: str, keyword: str) -> bool:
    return keyword and keyword in str(content or "")


def _build_prompt(keyword: str, frequency: int, samples: List[str]) -> Dict:
    dimensions = {
        dimension: {
            "label": meta["label"],
            "left": meta["left"]["label"],
            "right": meta["right"]["label"],
            "description": meta["description"],
        }
        for dimension, meta in POLITICAL_AXIS_SKILL.items()
    }
    return {
        "task": "判断一个高频词在这些B站古风歌曲评论语境中，是否具有稳定且明显的政治坐标轴倾向。",
        "keyword": keyword,
        "frequency": frequency,
        "samples": samples,
        "dimensions": dimensions,
        "decision_rule": [
            "必须基于样本语境判断，不要只凭词典常识。",
            "如果该词在样本中主要是玩梗、称呼、音乐评价或语义分散，应返回 accepted=false。",
            "如果至少70%的样本语境支持同一个 dimension + pole，返回 accepted=true。",
            "confidence 用0到1之间小数表示该 dimension + pole 的偏好比例/置信度。",
            "weight 建议1.0到3.5；越明显的政治隐喻权重越高。",
        ],
        "output_schema": {
            "keyword": "原词",
            "accepted": "boolean",
            "dimension": "plan_market | world_nation | liberty_authority | progress_conservative | none",
            "pole": "left | right | none",
            "confidence": "0-1 number",
            "weight": "1.0-3.5 number",
            "reason": "简短中文解释",
        },
    }


def _normalize_decision(keyword: str, frequency: int, sample_size: int, decision) -> Dict:
    if not isinstance(decision, dict):
        decision = {}
    dimension = str(decision.get("dimension", "none")).strip()
    pole = str(decision.get("pole", "none")).strip()
    confidence = float(decision.get("confidence", 0) or 0)
    weight = float(decision.get("weight", 2.0) or 2.0)
    accepted = bool(decision.get("accepted", False))

    if dimension not in POLITICAL_AXIS_SKILL or pole not in ("left", "right"):
        accepted = False
        dimension = "none"
        pole = "none"
    if confidence < 0.7:
        accepted = False

    meta = POLITICAL_AXIS_SKILL.get(dimension, {})
    return {
        "keyword": keyword,
        "frequency": frequency,
        "sample_size": sample_size,
        "dimension": dimension,
        "dimension_label": meta.get("label", ""),
        "pole": pole,
        "pole_label": meta.get(pole, {}).get("label", "") if pole in ("left", "right") else "",
        "confidence": round(confidence, 4),
        "weight": round(max(1.0, min(3.5, weight)), 4),
        "accepted": accepted,
        "reason": str(decision.get("reason", "")),
    }


def run_lexicon_iteration(
    comments: List[Dict],
    word_frequency: pd.DataFrame,
    output_data_dir: Path,
    enabled: bool = False,
    top_n: int = 5,
    samples_per_word: int = 10,
) -> Tuple[pd.DataFrame, Dict]:
    """运行 AI 词库自迭代，返回决策表和增量词库。"""
    rows = []
    delta: Dict[str, Dict[str, Dict[str, float]]] = {}

    if not enabled or word_frequency is None or word_frequency.empty:
        df = pd.DataFrame(columns=ITERATION_COLUMNS)
        _write_outputs(output_data_dir, df, delta)
        return df, delta

    rng = random.Random(20260425)
    for _, row in word_frequency.head(top_n).iterrows():
        keyword = str(row.get("keyword", "")).strip()
        if not keyword:
            continue
        frequency = int(row.get("frequency", 0) or 0)
        matched = [
            str(comment.get("content", ""))[:500]
            for comment in comments
            if _contains_keyword(str(comment.get("content", "")), keyword)
        ]
        if len(matched) > samples_per_word:
            samples = rng.sample(matched, samples_per_word)
        else:
            samples = matched

        prompt = _build_prompt(keyword, frequency, samples)
        decision = run_structured_ai_json(prompt, enabled=enabled)
        normalized = _normalize_decision(keyword, frequency, len(samples), decision)
        rows.append(normalized)

        if normalized["accepted"]:
            dimension = normalized["dimension"]
            pole = normalized["pole"]
            delta.setdefault(dimension, {}).setdefault(pole, {})[keyword] = normalized["weight"]

    df = pd.DataFrame(rows, columns=ITERATION_COLUMNS)
    _write_outputs(output_data_dir, df, delta)
    return df, delta


def _write_outputs(output_data_dir: Path, df: pd.DataFrame, delta: Dict):
    output_data_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_data_dir / "ai_lexicon_iterations.csv", index=False, encoding=CSV_ENCODING)
    (output_data_dir / "ai_lexicon_delta.json").write_text(
        json.dumps(delta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
