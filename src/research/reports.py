"""
Markdown 和 HTML 研究报告生成。
"""
import html
from pathlib import Path
from typing import Dict

import pandas as pd


def _markdown_table(df: pd.DataFrame, max_rows: int = 20) -> str:
    if df is None or df.empty:
        return "暂无数据\n"
    table = df.head(max_rows).fillna("").astype(str)
    columns = table.columns.tolist()
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join(["---"] * len(columns)) + " |",
    ]
    for _, row in table.iterrows():
        values = [str(row[col]).replace("|", "\\|").replace("\n", " ") for col in columns]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def build_markdown_report(
    keyword: str,
    run_config: Dict,
    videos_df: pd.DataFrame,
    comments_df: pd.DataFrame,
    freq_df: pd.DataFrame,
    tfidf_df: pd.DataFrame,
    political_summary_df: pd.DataFrame,
    political_terms_df: pd.DataFrame,
    lexicon_iteration_df: pd.DataFrame,
    ai_df: pd.DataFrame,
    figures: Dict[str, str],
) -> str:
    """构建 Markdown 报告文本。"""
    video_count = len(videos_df) if videos_df is not None else 0
    comment_count = len(comments_df) if comments_df is not None else 0

    lines = [
        f"# Bilibili 古风歌曲评论研究报告：{keyword}",
        "",
        "## 样本说明",
        "",
        f"- 搜索关键词：{keyword}",
        f"- 搜索视频数：{video_count}",
        f"- 候选视频数：{run_config.get('candidate_limit', video_count)}",
        f"- 匿名评论数：{comment_count}",
        f"- 单视频评论上限：{run_config.get('comments_per_video')}",
        f"- 是否启用 AI 辅助分析：{run_config.get('enable_ai')}",
        f"- 是否因风控/限流中断：{run_config.get('interrupted', False)}",
        "",
        "## 合规与隐私说明",
        "",
        "- 本次采集使用用户自行提供的 B 站 Cookie，在低频、可停止的研究模式下运行。",
        "- 程序不实现验证码绕过、代理池、账号轮换或规避风控逻辑。",
        "- 导出的评论数据默认移除用户 ID 和昵称，仅保留稳定匿名哈希。",
        "- `run_config.json` 不保存 Cookie 或 API key。",
        "",
        "## Top 关键词",
        "",
        _markdown_table(freq_df, 30),
        "",
        "## TF-IDF 关键词",
        "",
        _markdown_table(tfidf_df, 30),
        "",
        "## 政治坐标轴权重占比",
        "",
        "该部分参考 8values 的四轴框架，使用可解释词典 skill 做文本加权统计；政治历史隐喻更深的词语权重更高。四维度为：计划-市场、世界-国家、自由-威权、进步-保守。原始词频和 TF-IDF 保留在上方用于对比。",
        "",
        _markdown_table(political_summary_df, 20),
        "",
        "## 政治隐喻加权词频",
        "",
        _markdown_table(political_terms_df, 40),
        "",
        "## AI 词库自迭代",
        "",
        "该部分取原始词频 Top 5，回溯来源评论并随机抽取样本，由 AI 判断该词是否在某一政治坐标轴上具有不低于 70% 的稳定偏好。通过的词会写入本次增量词库并参与四轴重算。",
        "",
        _markdown_table(lexicon_iteration_df, 20),
        "",
        "## AI 辅助主题标签",
        "",
        _markdown_table(ai_df, 20),
        "",
        "## 图表",
        "",
    ]
    if run_config.get("interrupted"):
        lines.extend([
            "## 运行中断说明",
            "",
            f"- 中断原因：{run_config.get('interruption_reason', '')}",
            "- 报告基于中断前已保存的部分样本生成。",
            "",
        ])

    figure_titles = {
        "top_keywords": "Top 20 关键词词频",
        "comments_by_video": "评论量 Top 20 视频",
        "like_distribution": "评论点赞数分布",
        "keyword_cooccurrence": "关键词共现热力图",
        "political_axis_weights": "四维政治坐标轴双极权重",
        "political_dimension_net_scores": "四维政治坐标轴净倾向",
        "political_comment_coordinates": "评论政治隐喻二维投影",
    }
    for key, filename in figures.items():
        lines.extend([
            f"### {figure_titles.get(key, key)}",
            "",
            f"![{figure_titles.get(key, key)}](figures/{filename})",
            "",
        ])

    lines.extend([
        "## 数据文件",
        "",
        "- `data/search_videos.csv`：搜索到的视频元数据。",
        "- `data/comments_anonymized.csv`：匿名化评论数据。",
        "- `data/word_frequency.csv`：关键词词频。",
        "- `data/tfidf_keywords.csv`：TF-IDF 关键词。",
        "- `data/political_axis_comments.csv`：每条评论的坐标轴得分、主导轴和二维坐标。",
        "- `data/political_axis_summary.csv`：不同政治坐标轴的权重占比。",
        "- `data/political_axis_terms.csv`：政治隐喻加权词频。",
        "- `data/ai_lexicon_iterations.csv`：AI 词库自迭代决策记录。",
        "- `data/ai_lexicon_delta.json`：本次运行应用的增量词库。",
        "- `data/ai_themes.csv`：AI 辅助主题标签。",
        "- `run_config.json`：本次运行参数快照，不含敏感凭据。",
        "",
    ])

    return "\n".join(lines)


def markdown_to_html(markdown_text: str, title: str) -> str:
    """生成轻量 HTML，支持标题、列表、表格和图片。"""
    body_lines = []
    in_table = False
    for line in markdown_text.splitlines():
        if line.startswith("# "):
            body_lines.append(f"<h1>{html.escape(line[2:])}</h1>")
        elif line.startswith("## "):
            body_lines.append(f"<h2>{html.escape(line[3:])}</h2>")
        elif line.startswith("### "):
            body_lines.append(f"<h3>{html.escape(line[4:])}</h3>")
        elif line.startswith("![") and "](" in line and line.endswith(")"):
            alt = line[2:line.index("]")]
            src = line[line.index("](") + 2:-1]
            body_lines.append(f'<img src="{html.escape(src)}" alt="{html.escape(alt)}">')
        elif line.startswith("- "):
            body_lines.append(f"<p>{html.escape(line)}</p>")
        elif line.startswith("|") and line.endswith("|"):
            cells = [html.escape(cell.strip()) for cell in line.strip("|").split("|")]
            if set(cells) == {"---"}:
                continue
            if not in_table:
                body_lines.append("<table>")
                body_lines.append("<tr>" + "".join(f"<th>{cell}</th>" for cell in cells) + "</tr>")
                in_table = True
            else:
                body_lines.append("<tr>" + "".join(f"<td>{cell}</td>" for cell in cells) + "</tr>")
        else:
            if in_table:
                body_lines.append("</table>")
                in_table = False
            body_lines.append(f"<p>{html.escape(line)}</p>" if line else "")
    if in_table:
        body_lines.append("</table>")
    body = "\n".join(body_lines)
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>{html.escape(title)}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Microsoft YaHei", sans-serif; max-width: 980px; margin: 32px auto; line-height: 1.7; color: #18191c; }}
    table {{ border-collapse: collapse; width: 100%; margin: 12px 0 24px; }}
    th, td {{ border: 1px solid #e3e5e7; padding: 8px; text-align: left; }}
    th {{ background: #f6f7f9; }}
    code {{ background: #f6f7f9; padding: 2px 4px; border-radius: 4px; }}
    img {{ max-width: 100%; border: 1px solid #e3e5e7; border-radius: 6px; }}
  </style>
</head>
<body>
  {body}
</body>
</html>
"""


def write_reports(output_dir: Path, keyword: str, markdown_text: str) -> Dict[str, str]:
    """写出 Markdown 和 HTML 报告。"""
    md_path = output_dir / "report.md"
    html_path = output_dir / "report.html"
    md_path.write_text(markdown_text, encoding="utf-8")
    html_path.write_text(markdown_to_html(markdown_text, f"{keyword} 研究报告"), encoding="utf-8")
    return {"markdown": str(md_path), "html": str(html_path)}
