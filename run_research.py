"""
研究模式命令行入口。

示例：
python run_research.py --keyword 弱水三千 --videos 100 --comments 100 --ai
python run_research.py --keyword 弱水三千 --videos-csv output/xxx/data/search_videos.csv --comments 100 --ai
"""
import argparse
import sys

import pandas as pd

from src.research.pipeline import ResearchPipeline, create_output_dirs
from config.config import CSV_ENCODING


def main():
    parser = argparse.ArgumentParser(description="Bilibili 古风歌曲评论研究流水线")
    parser.add_argument("--keyword", required=True, help="歌曲名或搜索关键词")
    parser.add_argument("--videos", type=int, default=100, help="搜索视频上限")
    parser.add_argument("--comments", type=int, default=100, help="每个视频主评论上限")
    parser.add_argument("--output", default="output", help="输出目录")
    parser.add_argument("--ai", action="store_true", help="启用 AI 辅助主题标签")
    parser.add_argument("--videos-csv", default="", help="复用已有 search_videos.csv，跳过搜索")
    args = parser.parse_args()

    sys.stdout.reconfigure(encoding="utf-8")
    pipeline = ResearchPipeline(progress_callback=print, output_base_dir=args.output)

    if not args.videos_csv:
        result = pipeline.run(
            keyword=args.keyword,
            video_limit=args.videos,
            comments_per_video=args.comments,
            enable_ai=args.ai,
        )
        print("RESULT:", result)
        return

    dirs = create_output_dirs(args.keyword, args.output)
    videos_df = pd.read_csv(args.videos_csv).head(args.videos)
    videos_df.to_csv(dirs["data"] / "search_videos.csv", index=False, encoding=CSV_ENCODING)
    comments = []
    interrupted = False
    reason = ""
    try:
        comments = pipeline._crawl_video_comments(
            videos_df.to_dict("records"),
            args.comments,
            dirs["data"],
        )
    except Exception as e:
        interrupted = True
        reason = str(e)
        partial = dirs["data"] / "comments_partial_raw.csv"
        if partial.exists():
            comments = pd.read_csv(partial).fillna("").to_dict("records")
        print(f"INTERRUPTED: {reason}")

    result = pipeline._finalize_outputs(
        keyword=args.keyword,
        run_config={
            "keyword": args.keyword,
            "video_limit": args.videos,
            "candidate_limit": len(videos_df),
            "comments_per_video": args.comments,
            "include_replies": False,
            "enable_ai": args.ai,
            "interrupted": interrupted,
            "interruption_reason": reason,
        },
        dirs=dirs,
        videos_df=videos_df,
        comments=comments,
        enable_ai=args.ai,
    )
    print("RESULT:", {"output_dir": str(dirs["output"]), **result})


if __name__ == "__main__":
    main()
