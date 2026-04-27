# BiliPoliticalCommentsResearcher (BPCR)

BPCR 是一个面向社会科学研究的本地工具，用于在合规、低频、可复核的前提下，对 Bilibili 古风歌曲评论进行采集、匿名化、词频统计、政治历史隐喻识别、立场分类，以及跨歌曲汇总分析。

## 基础项目来源

本项目的基础爬取功能与早期实现思路，参考并继承自：

- [BilibiliCommentsCrawler](https://github.com/Yi-luo-hua/BilibiliCommentsCrawler)

在此基础上，BPCR 扩展了研究模式、文本验证、词库维护、单曲回填和跨歌曲汇总分析等功能。

它保留两条并行能力：
- `爬取研究`：采集并生成单曲研究结果
- `文本验证`：对单段文本做词典匹配、模糊匹配和 AI 辅助分析
- `汇总报告`：对 `result/` 下既有单曲结果做深度清洗、单曲回填和总报告生成

## 主要功能

- 低频、可中断的 Bilibili 评论研究采集
- GUI 三界面工作流
- 匿名化评论输出
- 原始词频 / TF-IDF / 共现分析
- 四维政治坐标轴分析
- `神 / 左 / 兔 / 皇 / 乐子人` 立场分类
- 手工词库、组合词规则、AI 候选词库纠偏
- 单曲 `deep_cleaning_v2/` 回填
- 跨歌曲单页 HTML / Markdown 总报告

##  **注意**

- 本项目初始词库 `manual_lexicons` 的早期版本由 AI 搜索、整理并统一生成，后续需要人工持续校订。
- 本项目不代表作者本人及其所在团体、组织的任何观点和立场。
- 使用者有义务自行审核内容、辨别真伪，并对自己的研究设计、解释和发布行为负责。


## 技术文档

详细的数据处理、权重规划、真正含义归并、立场分类和表格生成说明见：

- [docs/TECHNICAL_PIPELINE.md](D:/BilibiliCommentsCrawler/docs/TECHNICAL_PIPELINE.md)

## 环境要求

- Python 3.9+
- Windows / macOS / Linux

## 安装

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

依赖检查：

```bash
python -c "import requests, pandas, jieba, sklearn, matplotlib, customtkinter; print('ok')"
```

## 配置

复制模板：

```bash
copy user_config.example.json user_config.json
```

或：

```bash
cp user_config.example.json user_config.json
```

然后编辑 `user_config.json`：

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

说明：
- `bilibili_cookie` 需要填写你有权使用的完整浏览器 Cookie
- AI 配置是可选项；不配置时，规则版分析仍可运行
- `user_config.json` 不应提交到仓库

## GUI 使用

启动：

```bash
python main.py
```

GUI 分为三个界面。

### 1. `爬取研究`

用于输入歌曲名并抓取评论，生成单曲研究结果。

默认产物位于：

```text
output/<歌曲名>_<时间戳>/
├─ data/
├─ figures/
├─ report.md
├─ report.html
└─ run_config.json
```

### 2. `文本验证`

用于上传或粘贴单段文本，进行：
- 本地词典匹配
- 模糊匹配
- AI 全文理解
- 候选词库生成与审核

### 3. `汇总报告`

用于读取 `result/` 下的既有单曲目录，并提供两种入口：

- `仅生成汇总`
  - 直接扫描 `result/*/data/`
  - 生成新的跨歌曲 summary

- `回填单曲 + 生成汇总`
  - 先为每首歌生成 `deep_cleaning_v2/`
  - 再生成新的跨歌曲 summary
  - 不覆盖原始抓取结果和旧报告

每首歌的回填目录结构：

```text
result/<歌曲目录>/deep_cleaning_v2/
├─ data/
│  ├─ comments_deep_cleaned_v2.csv
│  ├─ clean_terms_v2.csv
│  ├─ excluded_terms_v2.csv
│  ├─ semantic_review_queue_v2.csv
│  ├─ meaning_labels_comments_v2.csv
│  ├─ meaning_distribution_overall_v2.csv
│  ├─ stance_labels_comments_v2.csv
│  ├─ stance_distribution_overall_v2.csv
│  ├─ composite_rule_summary_v2.csv
│  └─ overall_summary_v2.json
├─ figures/
├─ report.md
└─ report.html
```

新的总汇总目录结构：

```text
result/summary_<时间戳>/
├─ data/
├─ figures/
├─ report.md
├─ report.html
└─ data/overall_summary.json
```

## `output/` 和 `result/` 的关系

这是最重要的数据流：

1. 用 `爬取研究` 生成单曲结果，默认落在 `output/`
2. 将要纳入总研究的单曲目录整理到 `result/`
3. 在 `汇总报告` 页里：
   - 可以直接 `仅生成汇总`
   - 也可以先 `回填单曲 + 生成汇总`

也就是说：
- `output/` 更像运行产物区
- `result/` 更像研究样本库

## 命令行入口

单曲研究：

```bash
python run_research.py --keyword "弱水三千" --videos 100 --comments 100 --ai
```

文本验证：

```bash
python run_text_analysis.py --file "C:\path\to\text.md" --expected-stance 左 --ai
```

仅生成总汇总：

```bash
python run_deep_cleaning_summary.py --result-root result --ai
```

先回填每首歌，再生成总汇总：

```bash
python run_deep_cleaning_backfill.py --result-root result --ai
```

## 词库维护

词库位于 `config/manual_lexicons/`：

- `semantic_overrides.json`
- `stance_overrides.json`
- `composite_overrides.json`
- `lexicon_candidates.json`
- `SOURCES.md`

推荐流程：

1. 运行单曲研究或文本验证
2. 检查误判、高歧义词和待复核词
3. 在文本验证页生成候选词库
4. 人工接受或拒绝候选
5. 再运行单曲回填或总汇总

## 可复现建议

- 固定代码版本
- 保留 `run_config.json`
- 记录词库版本
- 保留匿名评论、词频、真正含义、立场与报告产物
- AI 结果与规则版结果同时保留，便于对照

## 合规与免责声明

本工具仅用于研究与教学用途，不提供验证码绕过、代理池、账号轮换或规避风控逻辑。


使用前请阅读：

- [DISCLAIMER.md](DISCLAIMER.md)

## 开源

本项目默认采用 [MIT License](LICENSE)。
