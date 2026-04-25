"""
Command-line entrypoint for single uploaded text analysis.
"""
import argparse
import sys
from pathlib import Path

from src.research.text_analyzer import (
    analyze_text,
    apply_lexicon_candidates,
    create_text_analysis_output_dir,
    reject_lexicon_candidates,
)


def main():
    parser = argparse.ArgumentParser(description="Analyze one uploaded text with manual lexicons and fuzzy matching")
    parser.add_argument("--text", default="", help="Text content to analyze")
    parser.add_argument("--file", default="", help="UTF-8 text file to analyze")
    parser.add_argument("--output", default="", help="Optional output directory")
    parser.add_argument("--no-fuzzy", action="store_true", help="Disable homophone/edit-distance fuzzy matching")
    parser.add_argument("--ai", action="store_true", help="Enable AI full-text review weighting")
    parser.add_argument("--expected-stance", default="", help="Expected stance: 神/左/兔/皇/乐子人")
    parser.add_argument("--suggest-correction", action="store_true", help="Generate pending lexicon candidates when result mismatches expected stance")
    parser.add_argument("--apply-candidates", default="", help="Comma-separated candidate ids to accept; use ALL for every pending candidate")
    parser.add_argument("--reject-candidates", default="", help="Comma-separated candidate ids to reject")
    args = parser.parse_args()

    sys.stdout.reconfigure(encoding="utf-8")
    if args.apply_candidates.strip():
        ids = [] if args.apply_candidates.strip().upper() == "ALL" else [
            item.strip() for item in args.apply_candidates.split(",") if item.strip()
        ]
        print("APPLY_CANDIDATES_RESULT:", apply_lexicon_candidates(ids))
        return
    if args.reject_candidates.strip():
        ids = [item.strip() for item in args.reject_candidates.split(",") if item.strip()]
        print("REJECT_CANDIDATES_RESULT:", reject_lexicon_candidates(ids))
        return

    if args.file.strip():
        text = Path(args.file).read_text(encoding="utf-8")
    else:
        text = args.text

    if not text.strip():
        raise SystemExit("Please provide --text or --file")

    output_dir = Path(args.output) if args.output.strip() else create_text_analysis_output_dir()
    result = analyze_text(
        text=text,
        output_dir=output_dir,
        enable_fuzzy=not args.no_fuzzy,
        enable_ai=args.ai,
        expected_stance=args.expected_stance.strip() or None,
        enable_correction_suggestion=args.suggest_correction,
    )
    print("TEXT_ANALYSIS_RESULT:", result)


if __name__ == "__main__":
    main()
