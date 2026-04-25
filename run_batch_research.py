r"""
批量研究入口。

示例：
python run_batch_research.py --songs-file C:\Users\muyufly\Desktop\古风DJ名单.md --videos 100 --comments 100 --ai
python run_batch_research.py --songs-file C:\Users\muyufly\Desktop\古风DJ名单.md --start-index 2 --videos 100 --comments 100 --ai
"""
import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

from src.research.pipeline import ResearchPipeline


def parse_song_list(path: str) -> list:
    """从 Markdown/文本文件中解析歌曲名。"""
    text = Path(path).read_text(encoding="utf-8")
    songs = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        line = re.sub(r"^\s*[-*]\s+", "", line)
        line = re.sub(r"^\s*\d+[.)、．]?\s*", "", line)
        line = line.strip()
        if line:
            songs.append(line)
    return songs


def write_batch_progress(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="批量运行 Bilibili 古风歌曲评论研究")
    parser.add_argument("--songs-file", required=True, help="歌曲名单 Markdown/文本文件")
    parser.add_argument("--videos", type=int, default=100, help="每首歌目标成功视频数")
    parser.add_argument("--comments", type=int, default=100, help="每个视频主评论上限")
    parser.add_argument("--output", default="output", help="输出目录")
    parser.add_argument("--ai", action="store_true", help="启用 AI 分析和词库自迭代")
    parser.add_argument(
        "--start-index",
        type=int,
        default=1,
        help="从歌曲名单中的第几首开始运行，1 表示从头开始",
    )
    args = parser.parse_args()

    sys.stdout.reconfigure(encoding="utf-8")
    songs = parse_song_list(args.songs_file)
    if not songs:
        raise RuntimeError("歌曲名单为空")
    if args.start_index < 1 or args.start_index > len(songs):
        raise ValueError(f"--start-index 必须在 1 到 {len(songs)} 之间")

    songs_to_run = list(enumerate(songs[args.start_index - 1:], start=args.start_index))

    batch_id = datetime.now().strftime("batch_%Y%m%d_%H%M%S")
    batch_dir = Path(args.output) / batch_id
    progress_path = batch_dir / "batch_progress.json"
    results = []

    print(f"BATCH_DIR: {batch_dir}")
    print(f"SONGS: {songs}")
    if args.start_index > 1:
        print(f"RESUME_FROM_INDEX: {args.start_index}")

    for run_number, (index, song) in enumerate(songs_to_run, start=1):
        progress = {
            "state": "running",
            "batch_id": batch_id,
            "current_index": index,
            "total_songs": len(songs),
            "run_number": run_number,
            "remaining_run_total": len(songs_to_run),
            "start_index": args.start_index,
            "current_song": song,
            "results": results,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        write_batch_progress(progress_path, progress)
        print(f"[{index}/{len(songs)}] START {song}")

        pipeline = ResearchPipeline(progress_callback=print, output_base_dir=args.output)
        try:
            result = pipeline.run(
                keyword=song,
                video_limit=args.videos,
                comments_per_video=args.comments,
                enable_ai=args.ai,
            )
            item = {"song": song, "status": "completed", **result}
        except Exception as e:
            item = {
                "song": song,
                "status": "failed",
                "error": str(e),
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            }
            print(f"[{index}/{len(songs)}] FAILED {song}: {e}")
        results.append(item)
        write_batch_progress(progress_path, {
            "state": "running" if run_number < len(songs_to_run) else "completed",
            "batch_id": batch_id,
            "current_index": index,
            "total_songs": len(songs),
            "run_number": run_number,
            "remaining_run_total": len(songs_to_run),
            "start_index": args.start_index,
            "current_song": song,
            "results": results,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        })
        print(f"[{index}/{len(songs)}] DONE {song}: {item.get('status')}")

    print("BATCH_COMPLETED")
    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
