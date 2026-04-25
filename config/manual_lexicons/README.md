手动词条库说明

你现在可以直接修改这两个文件，而不用进 Python 代码：

- `semantic_overrides.json`
- `stance_overrides.json`
- `composite_overrides.json`
- `lexicon_candidates.json`

用途：

1. `semantic_overrides.json`
   - `include_terms`: 强制把某个词纳入政治/历史词条，并指定一级类、真正含义、权重
   - `exclude_terms`: 强制排除某个词
   - `meaning_label_overrides`: 强制把某个词映射到指定“真正含义”

2. `stance_overrides.json`
   - `term_overrides`: 某个词一旦出现，就给某个立场加分
   - `meaning_overrides`: 某个“真正含义”一旦命中，就给某个立场加分

3. `composite_overrides.json`
   - `rules`: 非连续或拆分互文规则，例如“四海 + 五洲 + 风雷”在同段或相邻分段共现时触发某个真正含义和立场
   - `window`: 支持 `same_segment`、`adjacent_segments`、`full_text`

4. `lexicon_candidates.json`
   - AI 纠偏只写入这里，默认 `status=pending`
   - GUI 文本验证页点击“接受全部待审”后才会合并到正式词库
   - 点击“拒绝全部待审”只改候选状态，不影响正式词库

建议：

- 先改 JSON，再运行：
  `python run_deep_cleaning_summary.py --result-root result --ai`
- 每次运行都会把你当前的人工词库快照复制到新的 summary 输出目录里，方便追踪口径变化。
