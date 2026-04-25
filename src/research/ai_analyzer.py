"""
OpenAI 兼容接口或 Google Gemini API 的辅助主题编码。
"""
import json
import logging
import time
from typing import Dict, List

import pandas as pd

from config.user_config import load_user_config

logger = logging.getLogger(__name__)
GOOGLE_RETRY_DELAYS = (8, 20, 45)


def run_ai_thematic_analysis(
    comments: List[Dict],
    word_frequency: pd.DataFrame,
    enabled: bool = False,
) -> pd.DataFrame:
    """
    使用 OpenAI 兼容接口或 Google Gemini API 生成主题标签。

    默认只发送匿名评论文本抽样和聚合关键词，不发送用户标识。
    """
    columns = ["provider", "theme", "historical_or_political_reference", "evidence_keywords", "notes"]
    if not enabled:
        return pd.DataFrame(columns=columns)

    user_config = load_user_config()
    provider = user_config.get("ai_provider", "openai").strip().lower()
    top_keywords = []
    if word_frequency is not None and not word_frequency.empty:
        top_keywords = word_frequency.head(50)["keyword"].tolist()

    sample_comments = [
        str(c.get("content", ""))[:300]
        for c in comments[:120]
        if str(c.get("content", "")).strip()
    ]

    prompt = {
        "task": "分析B站古风歌曲评论中的政治历史隐喻主题，输出JSON数组。",
        "requirements": [
            "只做社会科学研究辅助编码，不判断用户身份。",
            "每个主题包含 theme、historical_or_political_reference、evidence_keywords、notes。",
            "避免过度推断；证据不足时写'待人工复核'。",
        ],
        "top_keywords": top_keywords,
        "sample_comments": sample_comments,
    }

    if provider == "google":
        return _run_google_analysis(user_config, prompt, columns)
    return _run_openai_analysis(user_config, prompt, columns)


def parse_ai_json(content: str):
    content = (content or "[]").strip().strip("`")
    if content.startswith("json"):
        content = content[4:].strip()
    return json.loads(content)


def _parse_json_table(content: str, columns: List[str], provider: str) -> pd.DataFrame:
    data = parse_ai_json(content)
    if isinstance(data, dict):
        data = data.get("themes", [])
    if not isinstance(data, list):
        return pd.DataFrame(columns=columns)
    rows = []
    for item in data:
        if not isinstance(item, dict):
            continue
        row = {col: item.get(col, "") for col in columns}
        row["provider"] = provider
        rows.append(row)
    return pd.DataFrame(rows, columns=columns)


def _run_openai_analysis(user_config: Dict, prompt: Dict, columns: List[str]) -> pd.DataFrame:
    config = user_config.get("openai", {})
    api_key = config.get("api_key", "").strip()
    model = config.get("model", "").strip() or "gpt-4o-mini"
    base_url = config.get("base_url", "").strip() or None
    if not api_key:
        logger.warning("未配置OpenAI兼容API key，跳过AI分析")
        return pd.DataFrame(columns=columns)

    try:
        from openai import OpenAI
    except ImportError:
        logger.warning("未安装openai依赖，跳过AI分析")
        return pd.DataFrame(columns=columns)

    client = OpenAI(api_key=api_key, base_url=base_url)
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "你是社会科学文本编码助手，输出严格JSON。"},
                {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
            ],
            temperature=0.2,
        )
        content = response.choices[0].message.content or "[]"
        return _parse_json_table(content, columns, "openai")
    except Exception as e:
        logger.warning(f"AI分析失败，已跳过: {e}")
        return pd.DataFrame(columns=columns)


def _run_google_analysis(user_config: Dict, prompt: Dict, columns: List[str]) -> pd.DataFrame:
    config = user_config.get("google", {})
    api_key = config.get("api_key", "").strip()
    model = config.get("model", "").strip() or "gemini-2.5-flash"
    if not api_key:
        logger.warning("未配置Google Gemini API key，跳过AI分析")
        return pd.DataFrame(columns=columns)

    try:
        from google import genai
    except ImportError:
        logger.warning("未安装google-genai依赖，跳过Google AI分析")
        return pd.DataFrame(columns=columns)

    try:
        response = _google_generate_content_with_retry(
            genai=genai,
            api_key=api_key,
            model=model,
            contents=(
                "你是社会科学文本编码助手，输出严格JSON数组，不要输出Markdown。\n"
                + json.dumps(prompt, ensure_ascii=False)
            ),
            context="Google AI分析",
        )
        return _parse_json_table(getattr(response, "text", "") or "[]", columns, "google")
    except Exception as e:
        logger.warning(f"Google AI分析失败，已跳过: {e}")
        return pd.DataFrame(columns=columns)


def _google_generate_content_with_retry(genai, api_key: str, model: str, contents: str, context: str):
    """Gemini 高峰期偶发 503，做少量退避重试，不绕过服务端限制。"""
    client = genai.Client(api_key=api_key)
    last_error = None
    for attempt, delay in enumerate((0, *GOOGLE_RETRY_DELAYS), start=1):
        if delay:
            time.sleep(delay)
        try:
            return client.models.generate_content(model=model, contents=contents)
        except Exception as e:
            last_error = e
            message = str(e)
            retryable = any(code in message for code in ("503", "UNAVAILABLE", "429", "RESOURCE_EXHAUSTED"))
            if not retryable or attempt > len(GOOGLE_RETRY_DELAYS):
                raise
            logger.warning(f"{context}暂时不可用，{delay or GOOGLE_RETRY_DELAYS[0]}s 后重试: {e}")
    raise last_error


def run_structured_ai_json(prompt: Dict, enabled: bool = False):
    """使用当前 AI provider 返回结构化 JSON；供词库自迭代等模块复用。"""
    if not enabled:
        return None

    user_config = load_user_config()
    provider = user_config.get("ai_provider", "openai").strip().lower()
    if provider == "google":
        config = user_config.get("google", {})
        api_key = config.get("api_key", "").strip()
        model = config.get("model", "").strip() or "gemini-2.5-flash"
        if not api_key:
            logger.warning("未配置Google Gemini API key，跳过结构化AI分析")
            return None
        try:
            from google import genai
            response = _google_generate_content_with_retry(
                genai=genai,
                api_key=api_key,
                model=model,
                contents=(
                    "你是社会科学文本编码助手，输出严格JSON，不要输出Markdown。\n"
                    + json.dumps(prompt, ensure_ascii=False)
                ),
                context="Google结构化AI分析",
            )
            return parse_ai_json(getattr(response, "text", "") or "{}")
        except Exception as e:
            logger.warning(f"Google结构化AI分析失败，已跳过: {e}")
            return None

    config = user_config.get("openai", {})
    api_key = config.get("api_key", "").strip()
    model = config.get("model", "").strip() or "gpt-4o-mini"
    base_url = config.get("base_url", "").strip() or None
    if not api_key:
        logger.warning("未配置OpenAI兼容API key，跳过结构化AI分析")
        return None
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key, base_url=base_url)
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "你是社会科学文本编码助手，输出严格JSON。"},
                {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
            ],
            temperature=0.1,
        )
        return parse_ai_json(response.choices[0].message.content or "{}")
    except Exception as e:
        logger.warning(f"OpenAI结构化AI分析失败，已跳过: {e}")
        return None
