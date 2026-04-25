"""
B站 Web WBI 签名工具。
"""
import hashlib
import re
import time
from pathlib import PurePosixPath
from typing import Dict, Any
from urllib.parse import quote, urlsplit


MIXIN_KEY_ENC_TAB = [
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35,
    27, 43, 5, 49, 33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13,
    37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4,
    22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52,
]


def extract_wbi_key(url: str) -> str:
    """从 wbi 图片 URL 中提取不带扩展名的 key。"""
    path = urlsplit(url).path
    name = PurePosixPath(path).name
    return name.rsplit(".", 1)[0]


def get_mixin_key(img_key: str, sub_key: str) -> str:
    """生成 mixin key。"""
    raw = img_key + sub_key
    return "".join(raw[i] for i in MIXIN_KEY_ENC_TAB)[:32]


def encode_wbi_params(params: Dict[str, Any], img_key: str, sub_key: str) -> Dict[str, Any]:
    """
    为参数添加 wts 和 w_rid。

    WBI 签名会过滤部分特殊字符，然后按 key 排序并 URL 编码。
    """
    signed = dict(params)
    signed["wts"] = int(time.time())
    mixin_key = get_mixin_key(img_key, sub_key)

    filtered = {
        k: re.sub(r"[!'()*]", "", str(v))
        for k, v in signed.items()
        if v is not None
    }
    query = "&".join(
        f"{quote(str(k), safe='')}={quote(str(filtered[k]), safe='')}"
        for k in sorted(filtered)
    )
    signed["w_rid"] = hashlib.md5((query + mixin_key).encode("utf-8")).hexdigest()
    return signed
