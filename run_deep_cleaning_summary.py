"""
Command-line entrypoint for deep cleaning and cross-song summary reporting.
"""
import argparse
import sys
from pathlib import Path

from src.research.deep_cleaning import create_summary_output_dir, run_deep_cleaning_summary


def main():
    parser = argparse.ArgumentParser(description="Run deep cleaning and aggregated summary for result/* research outputs")
    parser.add_argument("--result-root", default="result", help="Root directory containing per-song result folders")
    parser.add_argument("--output", default="", help="Optional explicit output directory")
    parser.add_argument("--ai", action="store_true", help="Enable AI-assisted term review and stance refinement")
    args = parser.parse_args()

    sys.stdout.reconfigure(encoding="utf-8")
    result_root = Path(args.result_root)
    output_dir = Path(args.output) if args.output.strip() else create_summary_output_dir(result_root)
    summary = run_deep_cleaning_summary(result_root=result_root, output_dir=output_dir, enable_ai=args.ai)
    print("SUMMARY_RESULT:", summary)


if __name__ == "__main__":
    main()
