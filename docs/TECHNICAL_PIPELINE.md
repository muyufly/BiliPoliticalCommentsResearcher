# BPCR Technical Documentation: Data Processing, Weight Design, and Table Generation

For the full project-facing technical description in Chinese, see:

- [../../docs/TECHNICAL_PIPELINE.md](D:/BilibiliCommentsCrawler/docs/TECHNICAL_PIPELINE.md)

This GitHub release keeps a mirrored copy of the core documentation intent:

## Scope

This document describes how BPCR currently:

1. organizes raw per-song research outputs
2. anonymizes and normalizes comment text
3. computes raw frequency, TF-IDF, and co-occurrence tables
4. applies four-axis political weighting
5. performs deep semantic cleaning
6. assigns “meaning labels” at comment level
7. derives stance labels
8. generates per-song backfill outputs and cross-song summary tables

## Core Modules

- `src/research/pipeline.py`
- `src/research/analysis.py`
- `src/research/political_axis.py`
- `src/research/text_analyzer.py`
- `src/research/deep_cleaning.py`
- `src/research/reports.py`

## Pipeline Layers

### 1. Per-song research

Outputs under:

```text
output/<song>_<timestamp>/
```

Main data files:

- `search_videos.csv`
- `comments_anonymized.csv`
- `word_frequency.csv`
- `tfidf_keywords.csv`
- `keyword_cooccurrence.csv`
- `political_axis_comments.csv`
- `political_axis_summary.csv`
- `political_axis_terms.csv`
- `ai_themes.csv`

### 2. Text validation

Works on a single pasted/uploaded text and produces:

- exact matches
- fuzzy matches
- composite-rule hits
- axis scores
- stance result
- candidate lexicon updates

### 3. Deep cleaning and summary

Works on curated per-song folders under:

```text
result/
```

Produces:

- per-song `deep_cleaning_v2/`
- cross-song `summary_<timestamp>/`

## Anonymization

BPCR removes direct user identifiers from research outputs and keeps a stable anonymous hash:

```text
user_hash = SHA256(salt + raw_user_identifier)[:16]
```

This preserves stable linkage inside the corpus without exposing the original user ID or username.

## Raw Text Processing

Normalization removes:

- URLs
- `@mentions`
- bracketed emoji-like markers
- repeated whitespace

Tokenization uses `jieba` and filters:

- tokens shorter than 2 characters
- stopwords
- numeric-only tokens
- symbol-only tokens

## Raw Tables

### Word Frequency

`word_frequency.csv`

```text
frequency(term) = total token count across all comments
```

Fields:

- `keyword`
- `frequency`

### TF-IDF

`tfidf_keywords.csv`

The implementation:

1. tokenizes each comment
2. builds document strings
3. computes TF-IDF with `TfidfVectorizer`
4. averages scores across documents

Fields:

- `keyword`
- `tfidf`

### Co-occurrence

`keyword_cooccurrence.csv`

Built from the top keywords and counted per comment as pairwise co-occurrence.

## Four-Axis Political Weighting

The project keeps four axes:

1. `计划 - 市场`
2. `世界 - 国家`
3. `自由 - 威权`
4. `进步 - 保守`

Each pole has a weighted lexicon:

```text
term -> weight
```

Comment-level pole scoring:

```text
pole_score(comment, dimension, pole)
= Σ count(term in comment) × weight(term)
```

Per dimension:

- `left_score`
- `right_score`
- `net_score = left_score - right_score`

Total political score:

```text
political_total_score
= Σ(all dimensions) (left_score + right_score)
```

This produces:

- `political_axis_comments.csv`
- `political_axis_summary.csv`
- `political_axis_terms.csv`

## Ambiguity Handling

Highly ambiguous terms such as:

- `世界`
- `天下`
- `四海`
- `五洲`
- `发展`
- `崛起`
- `民族主义`
- `加速`

are not allowed to directly push stance labels without context.

Current strategy:

- keep axis polarity intact
- gate stance/meaning effects through contextual rules and composite evidence

Example:

- `世界人民大团结万岁` can map to left/internationalist meaning
- bare `世界` does not directly add stance weight

## Deep Semantic Cleaning

Deep cleaning merges evidence from:

- `word_frequency.csv`
- `political_axis_terms.csv`

and classifies each term into:

- `keep`
- `exclude`
- `review`

Main outputs:

- `clean_terms.csv`
- `excluded_terms.csv`
- `semantic_review_queue.csv`

Important fields:

- `term`
- `semantic_status`
- `semantic_category`
- `evidence_count`
- `decision_source`
- `confidence`
- `reason`
- `raw_frequency`
- `weighted_frequency`

## Meaning Labels

Meaning labels are assigned at the comment level rather than copied from raw word frequency.

Reason:

- a frequent word may still be semantically ambiguous
- a political-historical meaning may require multiple terms or composite cues

For each comment, BPCR aggregates:

- contextual-rule hits
- ontology term hits
- manual override hits

and selects:

- `primary_meaning_label`
- optional `secondary_meaning_labels`

Meaning distribution is then computed as:

```text
share(label)
= comments with primary label = label
  / all valid political-historical comments
```

## Stance Labels

The stance layer uses a single-label output:

- `神`
- `左`
- `兔`
- `皇`
- `乐子人`

Stance scoring combines:

1. stance lexicon hits
2. meaning-label bonuses
3. axis-net-score bonuses

Conceptually:

```text
stance_score(stance)
= Σ matched_stance_term_score
+ Σ meaning_bonus
+ Σ axis_bonus
```

Low-confidence or weakly political comments fall back to `乐子人`.

Outputs:

- `stance_labels_comments.csv`
- `stance_distribution_overall.csv`
- `stance_distribution_by_song.csv`

## Composite Rules

Composite rules are stored in:

- `config/manual_lexicons/composite_overrides.json`

They support:

- `same_segment`
- `adjacent_segments`
- `full_text`

and produce:

- `composite_rule_summary.csv`

Key fields:

- `terms`
- `window`
- `meaning_label`
- `semantic_weight`
- `stance`
- `stance_score`
- `matched_comment_count`
- `matched_song_count`
- `share_of_comments_pct`

## Per-song Backfill

Backfill creates:

```text
result/<song-folder>/deep_cleaning_v2/
```

without overwriting the original crawl outputs.

Its purpose is to keep each song folder aligned with the latest deep-cleaning and reporting logic.

## Cross-song Summary

The cross-song summary still reads from the stable original per-song `data/` directories under `result/*/data/`.

This design keeps:

1. original research inputs stable
2. per-song backfill outputs reviewable
3. summary regeneration reproducible

## Main Output Tables

### Per-song backfill

- `comments_deep_cleaned_v2.csv`
- `clean_terms_v2.csv`
- `excluded_terms_v2.csv`
- `semantic_review_queue_v2.csv`
- `meaning_labels_comments_v2.csv`
- `meaning_distribution_overall_v2.csv`
- `stance_labels_comments_v2.csv`
- `stance_distribution_overall_v2.csv`
- `composite_rule_summary_v2.csv`
- `overall_summary_v2.json`

### Cross-song summary

- `combined_comments_cleaned.csv`
- `combined_meanings.csv`
- `combined_stances.csv`
- `song_level_summary.csv`
- `clean_terms.csv`
- `excluded_terms.csv`
- `semantic_review_queue.csv`
- `meaning_distribution_overall.csv`
- `meaning_distribution_by_song.csv`
- `stance_distribution_overall.csv`
- `stance_distribution_by_song.csv`
- `composite_rule_summary.csv`
- `overall_summary.json`

## Reproducibility Notes

To reproduce a study run, keep:

1. code version
2. curated `result/` directories
3. current `config/manual_lexicons/`
4. per-song `deep_cleaning_v2/`
5. summary outputs
6. whether AI review was enabled
