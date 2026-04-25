"""
歌曲评论研究流水线。
"""
import json
import logging
import random
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional

import pandas as pd

from config.config import (
    CSV_ENCODING,
    DEFAULT_COMMENTS_PER_VIDEO,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SEARCH_CANDIDATE_LIMIT,
    DEFAULT_SEARCH_VIDEO_LIMIT,
    RESEARCH_VIDEO_DELAY_MIN,
    RESEARCH_VIDEO_DELAY_MAX,
    RATE_LIMIT_HEARTBEAT_MIN,
    RATE_LIMIT_HEARTBEAT_MAX,
)
from src.api.bilibili_api import BilibiliAPI, BilibiliRateLimitError
from src.crawler.comment_crawler import CommentCrawler
from src.processor.data_processor import DataProcessor
from src.research.ai_analyzer import run_ai_thematic_analysis
from src.research.analysis import analyze_comments, anonymize_comments
from src.research.lexicon_iteration import run_lexicon_iteration
from src.research.political_axis import analyze_political_axes
from src.research.reports import build_markdown_report, write_reports
from utils.helpers import ContentType

logger = logging.getLogger(__name__)


def safe_slug(value: str) -> str:
    """生成适合文件夹名的短 slug。"""
    value = re.sub(r'[\\/:*?"<>|\s]+', "_", value.strip())
    value = re.sub(r"_+", "_", value).strip("_")
    return value[:60] or "research"


def create_output_dirs(keyword: str, base_dir: str = DEFAULT_OUTPUT_DIR) -> Dict[str, Path]:
    """创建本次运行的 output 子目录。"""
    root = Path(base_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = root / f"{safe_slug(keyword)}_{timestamp}"
    data_dir = output_dir / "data"
    figures_dir = output_dir / "figures"
    data_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)
    return {"output": output_dir, "data": data_dir, "figures": figures_dir}


class ResearchPipeline:
    """古风歌曲评论研究流水线。"""

    def __init__(
        self,
        progress_callback: Optional[Callable[[str], None]] = None,
        output_base_dir: str = DEFAULT_OUTPUT_DIR,
    ):
        self.progress_callback = progress_callback or (lambda message: None)
        self.output_base_dir = output_base_dir
        self.api = BilibiliAPI()
        self.crawler = CommentCrawler(progress_callback=progress_callback)
        self._stop_flag = False
        self._last_status = {}

    def _log(self, message: str):
        logger.info(message)
        self.progress_callback(message)

    def _status(
        self,
        state: str,
        video_index: int = 0,
        total_videos: int = 0,
        comments_count: int = 0,
        output_dir: str = "",
        note: str = "",
    ):
        """输出可被 GUI 解析的结构化进度状态。"""
        self._last_status = {
            "state": state,
            "video_index": video_index,
            "total_videos": total_videos,
            "comments_count": comments_count,
            "output_dir": output_dir,
            "note": note,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        payload = json.dumps(self._last_status, ensure_ascii=False)
        self._log(f"STATUS_JSON {payload}")

    def stop(self):
        """停止当前任务。"""
        self._stop_flag = True
        self.crawler.stop()

    def run(
        self,
        keyword: str,
        video_limit: int = DEFAULT_SEARCH_VIDEO_LIMIT,
        comments_per_video: int = DEFAULT_COMMENTS_PER_VIDEO,
        enable_ai: bool = False,
    ) -> Dict:
        """执行完整研究流水线。"""
        keyword = keyword.strip()
        if not keyword:
            raise ValueError("研究关键词不能为空")

        video_limit = max(1, min(DEFAULT_SEARCH_VIDEO_LIMIT, int(video_limit)))
        comments_per_video = max(1, min(DEFAULT_COMMENTS_PER_VIDEO, int(comments_per_video)))
        dirs = create_output_dirs(keyword, self.output_base_dir)
        self._log(f"输出目录: {dirs['output']}")
        self._status("created", output_dir=str(dirs["output"]), note="任务已创建")

        run_config = {
            "keyword": keyword,
            "video_limit": video_limit,
            "candidate_limit": max(video_limit, min(DEFAULT_SEARCH_CANDIDATE_LIMIT, 110)),
            "comments_per_video": comments_per_video,
            "include_replies": False,
            "enable_ai": bool(enable_ai),
            "created_at": datetime.now().isoformat(timespec="seconds"),
        }
        (dirs["output"] / "run_config.json").write_text(
            json.dumps(run_config, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        try:
            candidate_limit = max(video_limit, min(DEFAULT_SEARCH_CANDIDATE_LIMIT, 110))
            videos = self._search_videos(keyword, candidate_limit)
            videos_df = pd.DataFrame(videos)
            videos_df.to_csv(dirs["data"] / "search_videos.csv", index=False, encoding=CSV_ENCODING)
            run_config["candidate_limit"] = len(videos)

            comments = self._crawl_video_comments(videos, comments_per_video, dirs["data"])
            interrupted = self._stop_flag
            interruption_reason = "用户停止任务" if self._stop_flag else ""

            processed_video_count = self._count_processed_videos(comments)
            report_result = self._finalize_outputs(
                keyword=keyword,
                run_config={
                    **run_config,
                    "interrupted": interrupted,
                    "interruption_reason": interruption_reason,
                },
                dirs=dirs,
                videos_df=videos_df,
                comments=comments,
                enable_ai=enable_ai,
            )
            report_paths = report_result["reports"]
            self._log(f"研究报告已生成: {report_paths['markdown']}")

            return {
                "output_dir": str(dirs["output"]),
                "video_count": processed_video_count,
                "candidate_count": len(videos),
                "comment_count": report_result["comment_count"],
                "interrupted": interrupted,
                "interruption_reason": interruption_reason,
                "reports": report_paths,
            }
        except BilibiliRateLimitError as e:
            self._log(f"任务因风控/限流停止: {e}")
            raise

    def _search_videos(self, keyword: str, video_limit: int) -> List[Dict]:
        self._log(f"正在搜索视频: {keyword}，上限 {video_limit} 个")
        while not self._stop_flag:
            try:
                videos = self.api.search_video_list(keyword, limit=video_limit)
                self._log(f"搜索完成，获取 {len(videos)} 个视频")
                if not videos:
                    raise RuntimeError("未搜索到视频，请检查关键词或Cookie状态")
                return videos
            except BilibiliRateLimitError as e:
                wait_seconds = random.randint(RATE_LIMIT_HEARTBEAT_MIN, RATE_LIMIT_HEARTBEAT_MAX)
                note = f"搜索阶段触发风控/限流: {e}。{wait_seconds}s 后心跳探测。"
                self._log(note)
                self._status("rate_limited", note=note)
                self._sleep_with_stop(wait_seconds, 0, 0, lambda: 0, note)
        raise RuntimeError("任务已停止")

    def _finalize_outputs(
        self,
        keyword: str,
        run_config: Dict,
        dirs: Dict[str, Path],
        videos_df: pd.DataFrame,
        comments: List[Dict],
        enable_ai: bool,
    ) -> Dict:
        cleaned = DataProcessor.clean_comments(comments)
        anonymized = anonymize_comments(cleaned)
        comments_df = pd.DataFrame(anonymized)
        comments_df.to_csv(dirs["data"] / "comments_anonymized.csv", index=False, encoding=CSV_ENCODING)

        frames, figures = analyze_comments(anonymized, dirs["figures"])
        frames["word_frequency"].to_csv(
            dirs["data"] / "word_frequency.csv",
            index=False,
            encoding=CSV_ENCODING,
        )
        frames["tfidf"].to_csv(
            dirs["data"] / "tfidf_keywords.csv",
            index=False,
            encoding=CSV_ENCODING,
        )
        frames["cooccurrence"].to_csv(
            dirs["data"] / "keyword_cooccurrence.csv",
            encoding=CSV_ENCODING,
        )
        lexicon_iteration_df, lexicon_delta = run_lexicon_iteration(
            anonymized,
            frames["word_frequency"],
            dirs["data"],
            enabled=enable_ai,
        )
        political_frames, political_figures = analyze_political_axes(
            anonymized,
            dirs["figures"],
            lexicon_delta=lexicon_delta,
        )
        political_frames["political_axis_comments"].to_csv(
            dirs["data"] / "political_axis_comments.csv",
            index=False,
            encoding=CSV_ENCODING,
        )
        political_frames["political_axis_summary"].to_csv(
            dirs["data"] / "political_axis_summary.csv",
            index=False,
            encoding=CSV_ENCODING,
        )
        political_frames["political_axis_terms"].to_csv(
            dirs["data"] / "political_axis_terms.csv",
            index=False,
            encoding=CSV_ENCODING,
        )
        figures.update(political_figures)

        ai_df = run_ai_thematic_analysis(
            anonymized,
            frames["word_frequency"],
            enabled=enable_ai,
        )
        ai_df.to_csv(dirs["data"] / "ai_themes.csv", index=False, encoding=CSV_ENCODING)

        markdown = build_markdown_report(
            keyword=keyword,
            run_config=run_config,
            videos_df=videos_df,
            comments_df=comments_df,
            freq_df=frames["word_frequency"],
            tfidf_df=frames["tfidf"],
            political_summary_df=political_frames["political_axis_summary"],
            political_terms_df=political_frames["political_axis_terms"],
            lexicon_iteration_df=lexicon_iteration_df,
            ai_df=ai_df,
            figures=figures,
        )
        report_paths = write_reports(dirs["output"], keyword, markdown)
        return {"reports": report_paths, "comment_count": len(anonymized)}

    @staticmethod
    def _count_processed_videos(comments: List[Dict]) -> int:
        return len({c.get("bvid") or c.get("aid") for c in comments if c.get("bvid") or c.get("aid")})

    def _crawl_video_comments(
        self,
        videos: List[Dict],
        comments_per_video: int,
        data_dir: Optional[Path] = None,
    ) -> List[Dict]:
        all_comments: List[Dict] = []
        total = len(videos)
        partial_path = data_dir / "comments_partial_raw.csv" if data_dir else None
        progress_path = data_dir / "crawl_progress.json" if data_dir else None
        skipped_videos: List[Dict] = []
        processed_video_keys = set()

        def save_progress(index: int, state: str, note: str = ""):
            if partial_path:
                pd.DataFrame(all_comments).to_csv(partial_path, index=False, encoding=CSV_ENCODING)
            if progress_path:
                progress = {
                    "state": state,
                    "current_video_index": index,
                    "total_videos": total,
                    "comments_count": len(all_comments),
                    "processed_videos": len(processed_video_keys),
                    "target_videos": DEFAULT_SEARCH_VIDEO_LIMIT,
                    "skipped_videos": len(skipped_videos),
                    "note": note,
                    "updated_at": datetime.now().isoformat(timespec="seconds"),
                }
                progress_path.write_text(
                    json.dumps(progress, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            self._status(
                state,
                video_index=index,
                total_videos=total,
                comments_count=len(all_comments),
                note=note,
            )

        for index, video in enumerate(videos, start=1):
            if self._stop_flag:
                break
            if len(processed_video_keys) >= min(DEFAULT_SEARCH_VIDEO_LIMIT, total):
                self._log(f"已处理满 {min(DEFAULT_SEARCH_VIDEO_LIMIT, total)} 个视频，停止候选列表遍历")
                break
            aid = video.get("aid")
            bvid = video.get("bvid", "")
            title = video.get("title", "")
            if not aid:
                self._log(f"跳过无 aid 视频: {bvid or title}")
                continue

            self._log(f"[{index}/{total}] 爬取评论: {bvid} {title[:28]}")
            save_progress(index, "crawling", f"正在爬取 {bvid}")
            try:
                comments = self.crawler.crawl_target_comments(
                    oid=int(aid),
                    type_id=ContentType.VIDEO,
                    max_comments=comments_per_video,
                    mode=3,
                    include_replies=False,
                )
            except BilibiliRateLimitError as e:
                skipped_videos.append({
                    "candidate_index": index,
                    "aid": aid,
                    "bvid": bvid,
                    "title": title,
                    "reason": str(e),
                    "skipped_at": datetime.now().isoformat(timespec="seconds"),
                })
                if data_dir:
                    pd.DataFrame(skipped_videos).to_csv(
                        data_dir / "skipped_videos.csv",
                        index=False,
                        encoding=CSV_ENCODING,
                    )
                wait_seconds = random.randint(RATE_LIMIT_HEARTBEAT_MIN, RATE_LIMIT_HEARTBEAT_MAX)
                note = (
                    f"{bvid} 触发风控/限流，已跳过当前视频。"
                    f" 已保存当前数据，{wait_seconds}s 后心跳等待结束并爬取下一个视频。"
                )
                self._log(note)
                save_progress(index, "rate_limited_skipped", note)
                self._sleep_with_stop(
                    wait_seconds,
                    index,
                    total,
                    lambda: len(all_comments),
                    note,
                    waiting_callback=lambda remaining: save_progress(
                        index,
                        "rate_limited_waiting",
                        f"{note} 剩余约 {remaining}s",
                    ),
                )
                continue

            for comment in comments:
                comment.update({
                    "keyword": video.get("keyword", ""),
                    "search_rank": video.get("search_rank", 0),
                    "aid": aid,
                    "bvid": bvid,
                    "video_title": title,
                    "video_author": video.get("author", ""),
                    "video_pubdate": video.get("pubdate", 0),
                })
            all_comments.extend(comments)
            processed_video_keys.add(bvid or aid)
            self._log(f"  本视频获取 {len(comments)} 条，累计 {len(all_comments)} 条")
            save_progress(index, "saved", f"已保存 {bvid} 的评论")
            time.sleep(random.uniform(RESEARCH_VIDEO_DELAY_MIN, RESEARCH_VIDEO_DELAY_MAX))

        return all_comments

    def _sleep_with_stop(
        self,
        seconds: int,
        index: int,
        total: int,
        current_comments_count,
        note: str,
        waiting_callback=None,
    ):
        """可被 stop() 打断的长等待。"""
        remaining = seconds
        while remaining > 0 and not self._stop_flag:
            sleep_for = min(30, remaining)
            time.sleep(sleep_for)
            remaining -= sleep_for
            if remaining > 0:
                if waiting_callback:
                    waiting_callback(remaining)
                else:
                    self._status(
                        "rate_limited_waiting",
                        video_index=index,
                        total_videos=total,
                        comments_count=current_comments_count(),
                        note=f"{note} 剩余约 {remaining}s",
                    )
