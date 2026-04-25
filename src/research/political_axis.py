"""
政治历史隐喻坐标轴 skill。

参考 8values 的四轴思想，但这里不是问卷测验，而是对评论文本做
可解释词典加权编码。四个维度为：
- 计划 - 市场
- 世界 - 国家
- 自由 - 威权
- 进步 - 保守

该模块不会替代原始词频/TF-IDF，只新增一套政治隐喻加权分析结果。
"""
from collections import Counter, defaultdict
from pathlib import Path
import copy
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from src.research.analysis import tokenize, _setup_plot


POLITICAL_AXIS_SKILL: Dict[str, Dict] = {
    "plan_market": {
        "label": "计划-市场",
        "description": "经济组织想象：国家调配、均平/民生保障，与交易、商贸、繁荣效率之间的张力。",
        "left": {
            "label": "计划",
            "terms": {
                "计划": 3.0, "调配": 2.6, "分配": 2.5, "均贫富": 3.2, "民生": 2.4,
                "赈灾": 2.5, "赋税": 2.7, "田亩": 2.4, "屯田": 2.5, "粮仓": 2.2,
                "官办": 2.5, "国库": 2.6, "公田": 2.7, "天下为公": 3.3,
            },
        },
        "right": {
            "label": "市场",
            "terms": {
                "市场": 3.0, "商贸": 2.6, "商贾": 2.5, "买卖": 2.2, "交易": 2.4,
                "银钱": 2.2, "繁华": 1.8, "富贵": 1.8, "盛景": 1.7, "茶楼": 1.6,
                "酒肆": 1.6, "长街": 1.4, "市井": 1.8, "逐利": 2.4,
            },
        },
    },
    "world_nation": {
        "label": "世界-国家",
        "description": "共同体边界：天下/四海/大同的世界想象，与山河/故国/主权的国家想象。",
        "left": {
            "label": "世界",
            "terms": {
                "世界": 2.8, "天下一家": 3.2, "四海": 2.6, "万国": 2.8, "大同": 3.0,
                "和平": 2.4, "远方": 1.8, "异域": 2.0, "丝路": 2.4, "交流": 2.0,
                "共生": 2.4, "众生": 2.2, "苍生": 2.2, "人间": 1.6,
            },
        },
        "right": {
            "label": "国家",
            "terms": {
                "国家": 2.8, "家国": 3.0, "山河": 3.0, "河山": 2.9, "故国": 3.0,
                "故土": 2.8, "疆土": 3.0, "边疆": 2.7, "中原": 2.6, "九州": 2.7,
                "社稷": 3.0, "国祚": 3.0, "华夏": 3.0, "民族": 2.8, "主权": 3.0,
                "亡国": 3.3, "旧都": 2.5, "长安": 2.0, "洛阳": 1.9, "金陵": 1.9,
            },
        },
    },
    "liberty_authority": {
        "label": "自由-威权",
        "description": "政治权力关系：江湖/逍遥/反抗的自由叙事，与皇权/朝廷/秩序的威权叙事。",
        "left": {
            "label": "自由",
            "terms": {
                "自由": 3.0, "逍遥": 2.5, "江湖": 2.4, "洒脱": 2.0, "无拘": 2.4,
                "反抗": 2.8, "起义": 3.0, "抗争": 2.9, "破局": 2.4, "出走": 2.0,
                "不羁": 2.2, "独立": 2.4, "逃离": 2.0, "觉醒": 2.6,
            },
        },
        "right": {
            "label": "威权",
            "terms": {
                "皇权": 3.2, "王权": 3.0, "君王": 2.6, "帝王": 2.6, "君主": 2.7,
                "朝廷": 2.8, "庙堂": 2.5, "臣服": 2.8, "忠臣": 2.5, "忠义": 2.4,
                "天命": 3.0, "正统": 3.0, "登基": 2.8, "称帝": 2.8, "礼法": 2.5,
                "秩序": 2.3, "镇压": 3.0, "禁令": 2.6, "铁律": 2.6,
            },
        },
    },
    "progress_conservative": {
        "label": "进步-保守",
        "description": "社会文化方向：变革、觉醒、新生、理性，与传统、礼教、宗法、复古之间的张力。",
        "left": {
            "label": "进步",
            "terms": {
                "进步": 3.0, "变革": 3.0, "革新": 2.8, "革命": 3.2, "新生": 2.4,
                "觉醒": 2.6, "平等": 2.8, "女性": 2.2, "破除": 2.4, "科学": 2.4,
                "理性": 2.3, "现代": 2.2, "未来": 2.0, "新风": 2.1, "改良": 2.6,
            },
        },
        "right": {
            "label": "保守",
            "terms": {
                "保守": 3.0, "传统": 2.4, "礼教": 3.0, "宗法": 3.0, "祖训": 2.8,
                "家法": 2.7, "礼法": 2.6, "纲常": 3.0, "复古": 2.5, "古制": 2.6,
                "守旧": 2.8, "门阀": 2.7, "血统": 2.6, "嫡庶": 2.5, "忠孝": 2.5,
            },
        },
    },
}


def _term_count(text: str, tokens: List[str], term: str) -> int:
    return max(tokens.count(term), text.count(term))


def merge_axis_skill(lexicon_delta: Optional[Dict] = None) -> Dict[str, Dict]:
    """合并基础词典和本次 AI 自迭代增量词典。"""
    skill = copy.deepcopy(POLITICAL_AXIS_SKILL)
    if not lexicon_delta:
        return skill

    for dimension, poles in lexicon_delta.items():
        if dimension not in skill or not isinstance(poles, dict):
            continue
        for pole, terms in poles.items():
            if pole not in ("left", "right") or not isinstance(terms, dict):
                continue
            for term, weight in terms.items():
                term = str(term).strip()
                if not term:
                    continue
                try:
                    weight_value = float(weight)
                except (TypeError, ValueError):
                    weight_value = 2.0
                skill[dimension][pole]["terms"][term] = max(1.0, min(3.5, weight_value))
    return skill


def score_comment(content: str, axis_skill: Optional[Dict[str, Dict]] = None) -> Tuple[Dict[str, Dict[str, float]], List[Dict]]:
    """返回单条评论的四轴双极得分和命中词。"""
    axis_skill = axis_skill or POLITICAL_AXIS_SKILL
    text = str(content or "")
    tokens = tokenize(text)
    scores: Dict[str, Dict[str, float]] = {}
    hits: List[Dict] = []

    for dimension, meta in axis_skill.items():
        scores[dimension] = {"left": 0.0, "right": 0.0}
        for pole in ("left", "right"):
            pole_label = meta[pole]["label"]
            for term, weight in meta[pole]["terms"].items():
                count = _term_count(text, tokens, term)
                if count <= 0:
                    continue
                weighted = count * float(weight)
                scores[dimension][pole] += weighted
                hits.append({
                    "dimension": dimension,
                    "dimension_label": meta["label"],
                    "pole": pole,
                    "pole_label": pole_label,
                    "keyword": term,
                    "raw_frequency": count,
                    "weight": weight,
                    "weighted_frequency": weighted,
                })
        scores[dimension]["left"] = round(scores[dimension]["left"], 4)
        scores[dimension]["right"] = round(scores[dimension]["right"], 4)
    return scores, hits


def classify_depth(total_score: float) -> str:
    """按总分划分政治隐喻深度。"""
    if total_score <= 0:
        return "none"
    if total_score < 3:
        return "shallow"
    if total_score < 7:
        return "medium"
    return "deep"


def _dominant(scores: Dict[str, Dict[str, float]]) -> Tuple[str, str, float]:
    best_dimension = ""
    best_pole = ""
    best_score = 0.0
    for dimension, poles in scores.items():
        for pole in ("left", "right"):
            score = float(poles.get(pole, 0))
            if score > best_score:
                best_dimension = dimension
                best_pole = pole
                best_score = score
    return best_dimension, best_pole, best_score


def analyze_political_axes(
    comments: List[Dict],
    figures_dir: Path,
    lexicon_delta: Optional[Dict] = None,
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, str]]:
    """执行四维政治坐标轴 skill 分析并生成图表。"""
    axis_skill = merge_axis_skill(lexicon_delta)
    comment_rows = []
    term_counter: Dict[Tuple[str, str, str], Counter] = defaultdict(Counter)
    dimension_totals = Counter()
    pole_totals = Counter()
    dimension_comment_hits = Counter()

    for comment in comments:
        scores, hits = score_comment(comment.get("content", ""), axis_skill=axis_skill)
        total_score = round(
            sum(poles["left"] + poles["right"] for poles in scores.values()),
            4,
        )
        dominant_dimension, dominant_pole, dominant_score = _dominant(scores)
        dominant_meta = axis_skill.get(dominant_dimension, {})

        row = {
            "comment_id": comment.get("comment_id", ""),
            "bvid": comment.get("bvid", ""),
            "content": comment.get("content", ""),
            "dominant_dimension": dominant_dimension,
            "dominant_dimension_label": dominant_meta.get("label", ""),
            "dominant_pole": dominant_pole,
            "dominant_pole_label": dominant_meta.get(dominant_pole, {}).get("label", ""),
            "dominant_score": round(dominant_score, 4),
            "political_total_score": total_score,
            "political_depth": classify_depth(total_score),
        }

        for dimension, poles in scores.items():
            meta = axis_skill[dimension]
            left_score = float(poles["left"])
            right_score = float(poles["right"])
            dim_total = left_score + right_score
            net_score = left_score - right_score
            row[f"{dimension}_left_label"] = meta["left"]["label"]
            row[f"{dimension}_right_label"] = meta["right"]["label"]
            row[f"{dimension}_left_score"] = round(left_score, 4)
            row[f"{dimension}_right_score"] = round(right_score, 4)
            row[f"{dimension}_net_score"] = round(net_score, 4)
            row[f"{dimension}_left_pct"] = round(left_score / dim_total, 6) if dim_total else 0
            row[f"{dimension}_right_pct"] = round(right_score / dim_total, 6) if dim_total else 0
            dimension_totals[dimension] += dim_total
            pole_totals[(dimension, "left")] += left_score
            pole_totals[(dimension, "right")] += right_score
            if dim_total > 0:
                dimension_comment_hits[dimension] += 1
        comment_rows.append(row)

        for hit in hits:
            key = (hit["dimension"], hit["pole"], hit["keyword"])
            term_counter[key]["raw_frequency"] += hit["raw_frequency"]
            term_counter[key]["weighted_frequency"] += hit["weighted_frequency"]
            term_counter[key]["weight"] = hit["weight"]

    total_weight = sum(dimension_totals.values())
    summary_rows = []
    for dimension, meta in axis_skill.items():
        left_weight = float(pole_totals[(dimension, "left")])
        right_weight = float(pole_totals[(dimension, "right")])
        dim_total = left_weight + right_weight
        summary_rows.append({
            "dimension": dimension,
            "dimension_label": meta["label"],
            "left_label": meta["left"]["label"],
            "right_label": meta["right"]["label"],
            "left_weight": round(left_weight, 4),
            "right_weight": round(right_weight, 4),
            "total_weight": round(dim_total, 4),
            "dimension_share_overall": round(dim_total / total_weight, 6) if total_weight else 0,
            "left_share_within_dimension": round(left_weight / dim_total, 6) if dim_total else 0,
            "right_share_within_dimension": round(right_weight / dim_total, 6) if dim_total else 0,
            "net_score_left_minus_right": round(left_weight - right_weight, 4),
            "comments_with_dimension": int(dimension_comment_hits[dimension]),
            "description": meta["description"],
        })

    term_rows = []
    for (dimension, pole, term), counts in term_counter.items():
        meta = axis_skill[dimension]
        term_rows.append({
            "dimension": dimension,
            "dimension_label": meta["label"],
            "pole": pole,
            "pole_label": meta[pole]["label"],
            "keyword": term,
            "raw_frequency": int(counts["raw_frequency"]),
            "weight": float(counts["weight"]),
            "weighted_frequency": round(float(counts["weighted_frequency"]), 4),
        })
    term_rows.sort(key=lambda row: row["weighted_frequency"], reverse=True)

    frames = {
        "political_axis_comments": pd.DataFrame(comment_rows),
        "political_axis_summary": pd.DataFrame(summary_rows),
        "political_axis_terms": pd.DataFrame(
            term_rows,
            columns=[
                "dimension", "dimension_label", "pole", "pole_label",
                "keyword", "raw_frequency", "weight", "weighted_frequency",
            ],
        ),
    }
    figures = create_political_axis_figures(frames, figures_dir)
    return frames, figures


def create_political_axis_figures(frames: Dict[str, pd.DataFrame], figures_dir: Path) -> Dict[str, str]:
    """创建政治坐标轴图表。"""
    figures_dir.mkdir(parents=True, exist_ok=True)
    _setup_plot()
    outputs: Dict[str, str] = {}
    summary = frames["political_axis_summary"]
    comments = frames["political_axis_comments"]

    if not summary.empty and summary["total_weight"].sum() > 0:
        plot_df = summary.copy()
        plot_df["left_plot"] = -plot_df["left_weight"]
        plot_df["right_plot"] = plot_df["right_weight"]
        y_labels = plot_df["dimension_label"].tolist()
        y_pos = range(len(plot_df))

        plt.figure(figsize=(11, 6))
        plt.barh(y_pos, plot_df["left_plot"], color="#D32F2F", label="左侧值")
        plt.barh(y_pos, plot_df["right_plot"], color="#1976D2", label="右侧值")
        plt.yticks(y_pos, y_labels)
        plt.axvline(0, color="#888", linewidth=0.8)
        plt.title("四维政治坐标轴双极权重")
        plt.xlabel("左侧值 ← 加权分数 → 右侧值")
        plt.ylabel("维度")
        plt.legend()
        plt.tight_layout()
        path = figures_dir / "political_axis_weights.png"
        plt.savefig(path, dpi=180)
        plt.close()
        outputs["political_axis_weights"] = path.name

        net_df = summary.sort_values("net_score_left_minus_right", ascending=True)
        plt.figure(figsize=(10, 5))
        sns.barplot(
            data=net_df,
            x="net_score_left_minus_right",
            y="dimension_label",
            hue="dimension_label",
            palette="coolwarm",
            legend=False,
        )
        plt.axvline(0, color="#888", linewidth=0.8)
        plt.title("四维政治坐标轴净倾向（左侧值 - 右侧值）")
        plt.xlabel("净倾向分")
        plt.ylabel("维度")
        plt.tight_layout()
        path = figures_dir / "political_dimension_net_scores.png"
        plt.savefig(path, dpi=180)
        plt.close()
        outputs["political_dimension_net_scores"] = path.name

    required = {"liberty_authority_net_score", "progress_conservative_net_score", "political_depth"}
    if not comments.empty and required.issubset(comments.columns):
        plot_comments = comments.copy()
        # net_score is left minus right. For display convention:
        # x: progress on the left, conservative on the right;
        # y: authority on top, liberty on bottom.
        plot_comments["x_progress_to_conservative"] = -plot_comments["progress_conservative_net_score"]
        plot_comments["y_liberty_to_authority"] = -plot_comments["liberty_authority_net_score"]
        plt.figure(figsize=(8, 7))
        sns.scatterplot(
            data=plot_comments,
            x="x_progress_to_conservative",
            y="y_liberty_to_authority",
            hue="political_depth",
            palette="viridis",
        )
        plt.axhline(0, color="#888", linewidth=0.8)
        plt.axvline(0, color="#888", linewidth=0.8)
        plt.title("评论政治隐喻二维投影")
        plt.xlabel("进步 ←→ 保守")
        plt.ylabel("自由 ↓ / 威权 ↑")
        plt.tight_layout()
        path = figures_dir / "political_comment_coordinates.png"
        plt.savefig(path, dpi=180)
        plt.close()
        outputs["political_comment_coordinates"] = path.name

    return outputs
