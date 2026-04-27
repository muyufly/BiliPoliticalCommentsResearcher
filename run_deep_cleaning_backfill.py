"""
Command-line entrypoint for per-song deep-cleaning backfill and refreshed summary generation.
"""
import argparse
import sys
from pathlib import Path

from src.research.deep_cleaning import (
    create_summary_output_dir,
    run_deep_cleaning_backfill,
    run_deep_cleaning_summary,
)


def main():
    parser = argparse.ArgumentParser(
        description="Backfill deep-cleaning outputs into each result/<song>/deep_cleaning_v2 and regenerate summary"
    )
    parser.add_argument("--result-root", default="result", help="Root directory containing per-song result folders")
    parser.add_argument("--summary-output", default="", help="Optional explicit output directory for refreshed summary")
    parser.add_argument("--ai", action="store_true", help="Enable AI-assisted term review and stance refinement")
    args = parser.parse_args()

    sys.stdout.reconfigure(encoding="utf-8")
    result_root = Path(args.result_root)
    backfill_result = run_deep_cleaning_backfill(result_root=result_root, enable_ai=args.ai)
    summary_output = Path(args.summary_output) if args.summary_output.strip() else create_summary_output_dir(result_root)
    summary_result = run_deep_cleaning_summary(result_root=result_root, output_dir=summary_output, enable_ai=args.ai)
    print("BACKFILL_RESULT:", backfill_result)
    print("SUMMARY_RESULT:", summary_result)


if __name__ == "__main__":
    main()
