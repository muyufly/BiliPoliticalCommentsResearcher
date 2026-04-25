# BiliPoliticalCommentsResercher (BPCR)

BiliPoliticalCommentsResercher，简称 BPCR，是一个面向社会科学研究的本地数据工具，用于在合规、低频、可复核的前提下，对 Bilibili 古风 / 古风 DJ 歌曲相关视频评论进行采集、匿名化清洗、词频统计、政治历史隐喻识别、四轴坐标分析、立场分类和跨歌曲汇总报告生成。

本仓库发布版不包含任何 Cookie、API key、原始评论数据或研究结果。研究者需要在本地自行配置授权 Cookie 和可选 AI API，运行后生成自己的 `output/` 或 `result/` 数据目录。

特别鸣谢：BilibiliCommentsCrawler（项目地址：`github.com/Yi-luo-hua/BilibiliCommentsCrawler`）,该项目提供了本工具最基础的功能和爬取思路。

## 核心功能

- `爬取研究`：输入歌曲名，搜索相关视频，按限制采集每个视频的主评论（提供断点重连，静默等候机制），输出 CSV、Markdown、HTML 和图表。
- `文本验证`：上传或粘贴单段文本，使用人工词库、模糊匹配、组合规则和可选 AI 全文理解判断立场。
- `汇总报告`：扫描 `result/` 下已有单曲结果(**你需要手动挑选并复制爬取到的 `/output` 中的文件夹到`result/`目录下**)，生成跨歌曲单页 HTML 总报告。
- `人工词库`：支持手动维护语义词库、立场词库、组合规则和 AI 纠偏候选。
- `可复现输出`：每次运行记录参数快照，不保存 Cookie 或 API key。

## 研究框架

项目内置两套互补框架：

- 四维政治坐标轴：`计划-市场`、`世界-国家`、`自由-威权`、`进步-保守`。
- 简中互联网语境立场标签：`神`、`左`、`兔`、`皇`、`乐子人`。

这些标签是本项目的研究性操作定义，只用于语料编码和比较，不宣称是稳定的学术分类。AI 输出只作为辅助编码，最终解释应由研究者复核。所产生的一切道德、法律、政治问题，由使用者承担，本人概不负责。

## 注意
**本项目的初始的字典（manual_lexicons）由AI自动搜索爬取并统一生成，来源详见`.\config\manual_lexicons\SOURCES.md` ，**

**不代表本人及本人所在团体、组织的任何观点和立场，使用者有义务审查内容，辨别真伪**


## 目录结构

```text
githubVersion/
├── main.py                         # GUI 入口
├── run_research.py                 # 单曲研究命令行入口
├── run_batch_research.py           # 批量歌曲研究入口
├── run_text_analysis.py            # 单段文本分析入口
├── run_deep_cleaning_summary.py    # 跨歌曲汇总入口
├── requirements.txt
├── user_config.example.json        # 配置模板，不含密钥
├── DISCLAIMER.md                   # 研究伦理与免责声明
├── config/
│   ├── config.py
│   ├── user_config.py              # 本地配置读写，不含默认密钥
│   └── manual_lexicons/            # 人工词库与候选词库
├── src/
│   ├── api/
│   ├── crawler/
│   ├── exporter/
│   ├── gui/
│   ├── processor/
│   └── research/
└── utils/
```

## 安装

建议使用 Python 3.9 或更高版本。

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

验证依赖：

```bash
python -c "import requests, pandas, jieba, sklearn, matplotlib, customtkinter; print('ok')"
```

## 配置

复制配置模板：

```bash
copy user_config.example.json user_config.json
```

macOS / Linux:

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

- `bilibili_cookie` 应填写你在浏览器中登录后获得、且你有权使用的完整 Cookie 字符串。

- AI 配置是可选项；不配置时仍可运行规则版词频、词库匹配、图表和报告。

- `user_config.json` 已被 `.gitignore` 排除，禁止提交到公开仓库。

- 如果看不懂以上配置过程，你可以在GUI中配置

  

## GUI 使用

启动：

```bash
python main.py
```
或双击`启动程序.bat`

三个界面分别用于：

- `爬取研究`：填写歌曲名、视频数量、每视频评论数、是否启用 AI，开始单曲研究。
- `文本验证`：粘贴文本或上传文本文件，选择期望立场，运行规则 / AI 分析；如判断不符合，可生成候选词库，人工确认后合并。
- `汇总报告`：扫描 `result/` 下全部歌曲目录，生成跨歌曲总报告。

## 命令行使用

小样本单曲研究：

```bash
python run_research.py --keyword "弱水三千" --videos 10 --comments 10
```

启用 AI 辅助：

```bash
python run_research.py --keyword "弱水三千" --videos 100 --comments 100 --ai
```

复用已有搜索结果继续采集：

```bash
python run_research.py --keyword "弱水三千" --videos-csv output/某次运行/data/search_videos.csv --videos 100 --comments 100
```

单段文本分析：

```bash
python run_text_analysis.py --file "C:\path\to\text.md" --expected-stance 左 --ai
```

生成纠偏候选：

```bash
python run_text_analysis.py --file "C:\path\to\text.md" --expected-stance 左 --ai --suggest-correction
```

接受全部待审核候选：

```bash
python run_text_analysis.py --apply-candidates ALL
```

跨歌曲汇总：

```bash
python run_deep_cleaning_summary.py --result-root result --ai
```

## 输出目录

单次研究默认输出到：

```text
output/<歌曲名>_<时间戳>/
├── data/
├── figures/
├── report.md
├── report.html
└── run_config.json
```

跨歌曲汇总默认输出到：

```text
result/summary_<时间戳>/
├── data/
├── figures/
├── report.md
├── report.html
└── overall_summary.json
```

`run_config.json` 只记录运行参数，不保存 Cookie 或 API key。

## 词库维护

人工词库位于 `config/manual_lexicons/`：

- `semantic_overrides.json`：语义 / 历史政治含义词库。
- `stance_overrides.json`：立场词库。
- `composite_overrides.json`：非连续组合规则，例如同段或相邻段中多个隐喻词共现。
- `lexicon_candidates.json`：AI 纠偏候选，默认待人工确认，不自动写入正式词库。
- `SOURCES.md`：词库来源说明。

推荐流程：

1. 先运行文本验证或汇总报告。
2. 检查低置信度、误判或边界词。
3. 让 AI 生成候选。
4. 人工接受或拒绝候选。
5. 重新运行分析，比较变更前后的报告。

## 合规与伦理

本工具只实现低频、受限、本地化的研究采集流程，不实现验证码绕过、代理池、账号轮换、风控规避或批量攻击能力。遇到平台风控、HTTP 412、429、验证码或异常登录状态时，程序应退避、暂停、跳过或等待，不应尝试绕过。

请在使用前阅读 [DISCLAIMER.md](DISCLAIMER.md)。使用者需要自行确认研究设计、数据处理、公开发布和论文写作符合平台规则、当地法律法规、所在机构伦理审查要求和个人信息保护要求。

如使用本工具，则默认同意**所有**的用户协议和开源协议。

## 可复现建议

- 固定代码版本：记录 Git commit hash。
- 保存 `run_config.json`：记录歌曲名、视频数量、评论数量、AI 开关等参数。
- 保存词库版本：记录 `config/manual_lexicons/` 的提交状态。
- 保留原始匿名评论、清洗评论、词频、四轴结果、立场结果和报告。
- AI 结果需要标注模型、时间和配置，并保留规则版结果作为对照。

## 二次开发

### 不应提交到 GitHub 的内容

- `user_config.json`
- Cookie、SESSDATA、bili_jct、API key
- `output/`
- `result/`
- 含可识别个人信息的原始数据
- 本地虚拟环境 `.venv/`

欢迎提出PR,issues

## 许可证

本发布副本默认采用 [MIT License](LICENSE)。
