# BiliPoliticalCommentsResearcher (BPCR)

BPCR is a local research tool for reproducible social-science analysis of Bilibili comment corpora, especially around ancient-style / guofeng song discourse, political-historical metaphor, and stance coding.

This repository ships the code only. It does **not** include:
- real cookies
- API keys
- raw collected comments
- prior `output/` or `result/` datasets

## Base Project Credit

The basic crawling functionality and early implementation direction build on:

- [BilibiliCommentsCrawler](https://github.com/Yi-luo-hua/BilibiliCommentsCrawler)

BPCR extends that base with research-mode collection, text validation, manual lexicon workflows, per-song deep-cleaning backfill, and cross-song reporting.

## Features

- compliant, low-frequency Bilibili comment collection
- GUI workflow with three tabs
- anonymized comment export
- word frequency / TF-IDF / co-occurrence analysis
- four-axis political coordinate analysis
- stance classification: `神 / 左 / 兔 / 皇 / 乐子人`
- manual lexicons, composite rules, and AI-assisted correction candidates
- per-song `deep_cleaning_v2/` backfill
- cross-song HTML / Markdown summary reports

## Technical Documentation

Detailed documentation for data processing, weight design, semantic cleaning, stance coding, and table generation:

- [docs/TECHNICAL_PIPELINE.md](docs/TECHNICAL_PIPELINE.md)

## Install

Recommended: Python 3.9+

```bash
python -m venv .venv
```

Windows:

```powershell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

macOS / Linux:

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration

Copy the template:

```bash
copy user_config.example.json user_config.json
```

or:

```bash
cp user_config.example.json user_config.json
```

Then edit `user_config.json`:

```json
{
  "bilibili_cookie": "",
  "ai_provider": "google",
  "openai": {
    "base_url": "",
    "api_key": "",
    "model": "gpt-4o-mini"
  },
  "google": {
    "api_key": "",
    "model": "gemini-2.5-flash"
  }
}
```

Notes:
- `bilibili_cookie` should be a browser cookie string you are authorized to use
- AI config is optional
- `user_config.json` must stay local and should never be committed

## GUI

Start the app:

```bash
python main.py
```

The GUI has three tabs.

### 1. `爬取研究`

Collects per-song datasets and reports.

Default output:

```text
output/<song>_<timestamp>/
├─ data/
├─ figures/
├─ report.md
├─ report.html
└─ run_config.json
```

### 2. `文本验证`

Analyze a pasted or uploaded text passage with:
- local lexicon matching
- fuzzy matching
- optional AI full-text interpretation
- candidate lexicon suggestions

### 3. `汇总报告`

Works on existing per-song folders under `result/` and provides two entry points:

- `仅生成汇总`
  - scans `result/*/data/`
  - generates a new cross-song summary

- `回填单曲 + 生成汇总`
  - first creates `deep_cleaning_v2/` under each song folder
  - then generates a new cross-song summary
  - does not overwrite the original crawl outputs or old reports

Per-song backfill output:

```text
result/<song-folder>/deep_cleaning_v2/
├─ data/
├─ figures/
├─ report.md
└─ report.html
```

Cross-song summary output:

```text
result/summary_<timestamp>/
├─ data/
├─ figures/
├─ report.md
└─ report.html
```

## `output/` vs `result/`

Suggested workflow:

1. Run per-song collection into `output/`
2. Move or curate the song folders you want to study into `result/`
3. Use the summary tab to:
   - generate a summary directly, or
   - backfill every song with `deep_cleaning_v2/` and then summarize

In practice:
- `output/` is the run-output area
- `result/` is the curated research corpus

## CLI entry points

Per-song research:

```bash
python run_research.py --keyword "弱水三千" --videos 100 --comments 100 --ai
```

Text validation:

```bash
python run_text_analysis.py --file "C:\path\to\text.md" --expected-stance 左 --ai
```

Cross-song summary only:

```bash
python run_deep_cleaning_summary.py --result-root result --ai
```

Backfill every song, then regenerate summary:

```bash
python run_deep_cleaning_backfill.py --result-root result --ai
```

## Lexicon maintenance

Manual lexicons live in `config/manual_lexicons/`:

- `semantic_overrides.json`
- `stance_overrides.json`
- `composite_overrides.json`
- `lexicon_candidates.json`
- `SOURCES.md`

Recommended loop:

1. run single-song research or text validation
2. inspect false positives / ambiguous terms
3. generate AI candidates when useful
4. manually accept or reject candidates
5. rerun backfill or summary

## Reproducibility

- pin the code version
- preserve `run_config.json`
- record lexicon versions
- keep anonymized comments and derived outputs
- preserve both rule-only and AI-assisted results for comparison

## Compliance

This tool is for research and teaching use. It does not implement CAPTCHA bypass, proxy pools, account rotation, or anti-rate-limit evasion.

### Original Disclaimer Notes

- Early versions of `manual_lexicons` were initially assembled with AI assistance and should be treated as research materials requiring human review.
- This project does not represent the views or positions of the author or any affiliated group or organization.
- Users are responsible for auditing content, judging reliability, and taking responsibility for research design, interpretation, and publication decisions.

Please read:

- [DISCLAIMER.md](DISCLAIMER.md)

## License

[MIT License](LICENSE)
