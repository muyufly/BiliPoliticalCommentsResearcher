"""
评论文本分析、匿名化和可视化。
"""
import hashlib
import logging
import re
import warnings
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path
from typing import Dict, List, Tuple

warnings.filterwarnings("ignore", message="pkg_resources is deprecated.*", category=UserWarning)

import jieba
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib import font_manager
from sklearn.feature_extraction.text import TfidfVectorizer

jieba.setLogLevel(logging.WARNING)


DEFAULT_STOPWORDS = {
    "一个", "一些", "这个", "那个", "这么", "那么", "就是", "不是", "没有", "还是",
    "可以", "感觉", "真的", "什么", "怎么", "因为", "所以", "但是", "然后", "已经",
    "自己", "我们", "你们", "他们", "她们", "大家", "哈哈", "哈哈哈", "视频", "评论",
    "歌曲", "古风", "音乐", "老师", "up", "UP", "B站", "b站",
}


def anonymize_value(value: object, salt: str = "bilibili-research") -> str:
    """稳定匿名化用户标识。"""
    raw = str(value or "").strip()
    if not raw:
        return ""
    digest = hashlib.sha256((salt + raw).encode("utf-8")).hexdigest()
    return digest[:16]


def anonymize_comments(comments: List[Dict]) -> List[Dict]:
    """匿名化评论中的用户字段。"""
    anonymized = []
    for comment in comments:
        item = dict(comment)
        item["user_hash"] = anonymize_value(item.get("user_id") or item.get("username"))
        item.pop("user_id", None)
        item.pop("username", None)
        anonymized.append(item)
    return anonymized


def normalize_text(text: str) -> str:
    """清理评论文本，保留适合中文分词的内容。"""
    text = re.sub(r"https?://\S+", " ", str(text or ""))
    text = re.sub(r"@\S+", " ", text)
    text = re.sub(r"\[[^\]]+\]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def tokenize(text: str, extra_stopwords: set = None) -> List[str]:
    """中文分词并过滤停用词。"""
    stopwords = DEFAULT_STOPWORDS | (extra_stopwords or set())
    words = []
    for word in jieba.lcut(normalize_text(text)):
        word = word.strip()
        if len(word) < 2:
            continue
        if word in stopwords:
            continue
        if re.fullmatch(r"[\W_]+|\d+", word):
            continue
        words.append(word)
    return words


def build_word_frequency(comments: List[Dict], top_n: int = 100) -> pd.DataFrame:
    """生成词频表。"""
    counter = Counter()
    for comment in comments:
        counter.update(tokenize(comment.get("content", "")))
    rows = [{"keyword": word, "frequency": count} for word, count in counter.most_common(top_n)]
    return pd.DataFrame(rows)


def build_tfidf(comments: List[Dict], top_n: int = 100) -> pd.DataFrame:
    """生成全语料 TF-IDF 关键词表。"""
    docs = [" ".join(tokenize(comment.get("content", ""))) for comment in comments]
    docs = [doc for doc in docs if doc.strip()]
    if not docs:
        return pd.DataFrame(columns=["keyword", "tfidf"])

    vectorizer = TfidfVectorizer(token_pattern=r"(?u)\b\w+\b", max_features=1000)
    matrix = vectorizer.fit_transform(docs)
    scores = matrix.mean(axis=0).A1
    features = vectorizer.get_feature_names_out()
    pairs = sorted(zip(features, scores), key=lambda x: x[1], reverse=True)[:top_n]
    return pd.DataFrame([{"keyword": word, "tfidf": round(float(score), 6)} for word, score in pairs])


def build_cooccurrence(comments: List[Dict], keywords: List[str], top_n: int = 25) -> pd.DataFrame:
    """生成关键词共现矩阵。"""
    selected = keywords[:top_n]
    matrix = defaultdict(Counter)
    selected_set = set(selected)

    for comment in comments:
        present = sorted(set(tokenize(comment.get("content", ""))) & selected_set)
        for a, b in combinations(present, 2):
            matrix[a][b] += 1
            matrix[b][a] += 1
        for word in present:
            matrix[word][word] += 1

    return pd.DataFrame(
        [[matrix[row][col] for col in selected] for row in selected],
        index=selected,
        columns=selected,
    )


def _setup_plot():
    sns.set_theme(style="whitegrid")
    available_fonts = {f.name for f in font_manager.fontManager.ttflist}
    preferred = ["Microsoft YaHei", "SimHei", "Arial Unicode MS", "Noto Sans CJK SC"]
    font_name = next((name for name in preferred if name in available_fonts), "DejaVu Sans")
    plt.rcParams["font.sans-serif"] = [font_name, "DejaVu Sans"]
    plt.rcParams["font.family"] = "sans-serif"
    plt.rcParams["axes.unicode_minus"] = False


def create_figures(
    comments_df: pd.DataFrame,
    freq_df: pd.DataFrame,
    cooccur_df: pd.DataFrame,
    figures_dir: Path,
) -> Dict[str, str]:
    """创建静态图表，返回图表文件名映射。"""
    figures_dir.mkdir(parents=True, exist_ok=True)
    _setup_plot()
    outputs: Dict[str, str] = {}

    if not freq_df.empty:
        top = freq_df.head(20).sort_values("frequency", ascending=True)
        plt.figure(figsize=(10, 7))
        sns.barplot(data=top, x="frequency", y="keyword", color="#23ADE5")
        plt.title("Top 20 关键词词频")
        plt.xlabel("词频")
        plt.ylabel("关键词")
        plt.tight_layout()
        path = figures_dir / "top_keywords.png"
        plt.savefig(path, dpi=180)
        plt.close()
        outputs["top_keywords"] = path.name

    if not comments_df.empty and "bvid" in comments_df.columns:
        counts = comments_df.groupby("bvid").size().sort_values(ascending=False).head(20)
        plt.figure(figsize=(11, 6))
        sns.barplot(x=counts.values, y=counts.index, color="#FB7299")
        plt.title("评论量 Top 20 视频")
        plt.xlabel("评论数")
        plt.ylabel("BV号")
        plt.tight_layout()
        path = figures_dir / "comments_by_video.png"
        plt.savefig(path, dpi=180)
        plt.close()
        outputs["comments_by_video"] = path.name

    if not comments_df.empty and "like_count" in comments_df.columns:
        plt.figure(figsize=(10, 6))
        sns.histplot(comments_df["like_count"].fillna(0), bins=30, color="#52C41A")
        plt.title("评论点赞数分布")
        plt.xlabel("点赞数")
        plt.ylabel("评论数")
        plt.tight_layout()
        path = figures_dir / "like_distribution.png"
        plt.savefig(path, dpi=180)
        plt.close()
        outputs["like_distribution"] = path.name

    if cooccur_df.shape[0] > 1:
        plt.figure(figsize=(12, 10))
        sns.heatmap(cooccur_df, cmap="Blues")
        plt.title("关键词共现热力图")
        plt.tight_layout()
        path = figures_dir / "keyword_cooccurrence.png"
        plt.savefig(path, dpi=180)
        plt.close()
        outputs["keyword_cooccurrence"] = path.name

    return outputs


def analyze_comments(comments: List[Dict], figures_dir: Path) -> Tuple[Dict[str, pd.DataFrame], Dict[str, str]]:
    """运行本地分析并生成图表。"""
    comments_df = pd.DataFrame(comments)
    freq_df = build_word_frequency(comments)
    tfidf_df = build_tfidf(comments)
    keywords = freq_df["keyword"].tolist() if not freq_df.empty else []
    cooccur_df = build_cooccurrence(comments, keywords)
    figures = create_figures(comments_df, freq_df, cooccur_df, figures_dir)
    return {
        "comments": comments_df,
        "word_frequency": freq_df,
        "tfidf": tfidf_df,
        "cooccurrence": cooccur_df,
    }, figures
