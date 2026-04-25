"""
Local user configuration helpers.

The real configuration is stored in project-root user_config.json, which is
ignored by git. This module intentionally ships with empty defaults so the
GitHub version never contains Cookie strings or API keys.
"""
import json
from pathlib import Path
from typing import Any, Dict


PROJECT_ROOT = Path(__file__).resolve().parents[1]
USER_CONFIG_PATH = PROJECT_ROOT / "user_config.json"


DEFAULT_USER_CONFIG: Dict[str, Any] = {
    "bilibili_cookie": "",
    "ai_provider": "google",
    "openai": {
        "base_url": "",
        "api_key": "",
        "model": "gpt-4o-mini",
    },
    "google": {
        "api_key": "",
        "model": "gemini-2.5-flash",
    },
}


def load_user_config() -> Dict[str, Any]:
    """Load local user config; return empty safe defaults if unavailable."""
    if not USER_CONFIG_PATH.exists():
        return json.loads(json.dumps(DEFAULT_USER_CONFIG))

    try:
        with USER_CONFIG_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return json.loads(json.dumps(DEFAULT_USER_CONFIG))

    merged = json.loads(json.dumps(DEFAULT_USER_CONFIG))
    for key, value in data.items():
        if key in ("openai", "google") and isinstance(value, dict):
            merged[key].update(value)
        elif key not in ("openai", "google"):
            merged[key] = value
    return merged


def save_user_config(config: Dict[str, Any]) -> None:
    """Save local user config to user_config.json."""
    USER_CONFIG_PATH.write_text(
        json.dumps(config, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def has_bilibili_cookie() -> bool:
    """Return whether a Bilibili Cookie has been configured locally."""
    return bool(load_user_config().get("bilibili_cookie", "").strip())
