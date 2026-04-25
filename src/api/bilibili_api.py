"""
B站API调用封装模块
- 自适应请求延迟（正常时快速，被限速时退避）
- 迭代式重试（非递归）
- 统一日志接口
- 支持视频、动态、专栏文章
"""
import time
import logging
import random
import re
import requests
from html import unescape
from typing import Dict, Optional, Any
from config.config import (
    COMMENT_API_URL,
    REPLY_API_URL,
    SEARCH_VIDEO_API_URL,
    NAV_API_URL,
    BILIBILI_HOME_URL,
    DYNAMIC_DETAIL_API_URL,
    ARTICLE_INFO_API_URL,
    DEFAULT_HEADERS,
    REQUEST_TIMEOUT,
    REQUEST_DELAY_MIN,
    REQUEST_DELAY_MAX,
    REQUEST_DELAY_DEFAULT,
    REQUEST_JITTER_MAX,
    MAX_RETRIES,
    DEFAULT_PAGE_SIZE,
)
from config.user_config import load_user_config
from src.api.wbi import encode_wbi_params, extract_wbi_key

logger = logging.getLogger(__name__)


class BilibiliAPIError(Exception):
    """B站 API 基础异常。"""


class BilibiliRateLimitError(BilibiliAPIError):
    """触发 B站风控或限流。"""


class BilibiliAPI:
    """B站API调用封装类"""

    def __init__(self, headers: Optional[Dict[str, str]] = None, load_cookie: bool = True):
        self.headers = headers or DEFAULT_HEADERS.copy()
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        # 自适应延迟：初始值较小，被限速后动态增大
        self._current_delay = REQUEST_DELAY_DEFAULT
        self._wbi_keys = None
        self._wbi_keys_ts = 0

        if load_cookie:
            user_config = load_user_config()
            cookie = user_config.get("bilibili_cookie", "").strip()
            if cookie:
                self.set_cookie(cookie)
        self.initialize_cookies()

    def _adaptive_sleep(self, was_rate_limited: bool = False):
        """
        自适应延迟控制。
        正常时逐步缩短到最小值；被限速时倍增到最大值。
        """
        if was_rate_limited:
            self._current_delay = min(self._current_delay * 2, REQUEST_DELAY_MAX)
        else:
            # 成功时缓慢恢复到最小延迟
            self._current_delay = max(self._current_delay * 0.8, REQUEST_DELAY_MIN)
        time.sleep(self._current_delay + random.uniform(0, REQUEST_JITTER_MAX))

    def initialize_cookies(self):
        """访问首页以初始化 buvid 等基础 Cookie；失败不阻断主流程。"""
        try:
            self.session.get(
                BILIBILI_HOME_URL,
                timeout=REQUEST_TIMEOUT,
                headers={"Referer": BILIBILI_HOME_URL},
            )
        except requests.RequestException as e:
            logger.debug(f"初始化B站Cookie失败，将继续尝试API请求: {e}")

    def _request(self, url: str, params: Dict[str, Any]) -> Optional[Dict]:
        """
        发送HTTP请求（带重试机制，迭代式）

        Args:
            url: 请求URL
            params: 请求参数

        Returns:
            JSON响应数据，如果请求失败则返回None
        """
        for attempt in range(MAX_RETRIES + 1):
            try:
                self._adaptive_sleep(was_rate_limited=(attempt > 0))
                response = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
                if response.status_code in (412, 429):
                    raise BilibiliRateLimitError(
                        f"触发B站风控或限流(HTTP {response.status_code})，已停止当前任务"
                    )
                response.raise_for_status()
                data = response.json()

                code = data.get('code', -1)
                if code == 0:
                    return data

                # -412 = 被风控限速
                if code == -412:
                    self._adaptive_sleep(was_rate_limited=True)
                    raise BilibiliRateLimitError("触发B站风控(code=-412)，已停止当前任务")

                logger.warning(f"API返回错误: code={code}, message={data.get('message')}")
                return None

            except requests.exceptions.Timeout:
                if attempt < MAX_RETRIES:
                    wait = 2 ** attempt
                    logger.warning(f"请求超时，{wait}s 后重试 ({attempt+1}/{MAX_RETRIES})")
                    time.sleep(wait)
                else:
                    logger.error("请求超时，已达到最大重试次数")
                    return None

            except requests.exceptions.RequestException as e:
                if attempt < MAX_RETRIES:
                    wait = 2 ** attempt
                    logger.warning(f"请求失败 ({attempt+1}/{MAX_RETRIES}): {e}")
                    time.sleep(wait)
                else:
                    logger.error(f"请求失败，已达到最大重试次数: {e}")
                    return None

            except BilibiliRateLimitError:
                raise

            except ValueError as e:
                logger.error(f"JSON解析错误: {e}")
                return None

        return None

    def _request_signed(self, url: str, params: Dict[str, Any]) -> Optional[Dict]:
        """发送需要 WBI 签名的 GET 请求。"""
        img_key, sub_key = self.get_wbi_keys()
        signed_params = encode_wbi_params(params, img_key, sub_key)
        return self._request(url, signed_params)

    def get_wbi_keys(self) -> tuple:
        """获取并缓存 WBI img_key/sub_key。"""
        now = time.time()
        if self._wbi_keys and now - self._wbi_keys_ts < 12 * 3600:
            return self._wbi_keys

        self._adaptive_sleep()
        try:
            response = self.session.get(NAV_API_URL, timeout=REQUEST_TIMEOUT)
            if response.status_code in (412, 429):
                raise BilibiliRateLimitError(
                    f"触发B站风控或限流(HTTP {response.status_code})，已停止当前任务"
                )
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            raise BilibiliAPIError(f"无法获取WBI签名参数: {e}") from e
        except ValueError as e:
            raise BilibiliAPIError(f"WBI签名参数响应解析失败: {e}") from e

        if not data or not data.get("data"):
            raise BilibiliAPIError("无法获取WBI签名参数，请检查网络或Cookie")

        wbi_img = data["data"].get("wbi_img", {})
        img_url = wbi_img.get("img_url", "")
        sub_url = wbi_img.get("sub_url", "")
        if not img_url or not sub_url:
            raise BilibiliAPIError("WBI签名参数缺失")

        self._wbi_keys = (extract_wbi_key(img_url), extract_wbi_key(sub_url))
        self._wbi_keys_ts = now
        return self._wbi_keys

    # ============================================================
    #  视频相关
    # ============================================================
    def get_video_info(self, bvid: str) -> Optional[Dict]:
        """
        获取视频基本信息（用于获取真实的AV号）

        Args:
            bvid: BV号

        Returns:
            视频信息字典
        """
        url = "https://api.bilibili.com/x/web-interface/view"
        params = {"bvid": bvid}
        return self._request(url, params)

    def search_videos(
        self,
        keyword: str,
        page: int = 1,
        order: str = "totalrank",
        duration: int = 0,
        tids: int = 0,
    ) -> Optional[Dict]:
        """
        搜索视频。

        B站 Web 搜索接口通常需要 WBI 签名和 buvid/cookie。
        """
        params = {
            "search_type": "video",
            "keyword": keyword,
            "page": page,
            "order": order,
            "duration": duration,
            "tids": tids,
            "highlight": 0,
        }
        return self._request_signed(SEARCH_VIDEO_API_URL, params)

    @staticmethod
    def normalize_video_search_item(item: Dict, keyword: str, rank: int) -> Dict:
        """规范化搜索结果中的视频字段。"""
        title = unescape(re.sub(r"<[^>]+>", "", str(item.get("title", "")))).strip()
        pic = item.get("pic", "")
        if isinstance(pic, str) and pic.startswith("//"):
            pic = "https:" + pic
        return {
            "keyword": keyword,
            "search_rank": rank,
            "aid": item.get("aid") or item.get("id"),
            "bvid": item.get("bvid", ""),
            "title": title,
            "author": item.get("author", ""),
            "mid": item.get("mid", ""),
            "play": item.get("play", 0),
            "danmaku": item.get("video_review", 0),
            "favorites": item.get("favorites", 0),
            "pubdate": item.get("pubdate", 0),
            "duration": item.get("duration", ""),
            "description": unescape(re.sub(r"<[^>]+>", "", str(item.get("description", "")))).strip(),
            "pic": pic,
            "arcurl": item.get("arcurl", ""),
        }

    def search_video_list(self, keyword: str, limit: int = 100, order: str = "totalrank") -> list:
        """按关键词搜索视频列表，最多抓取 limit 条。"""
        videos = []
        seen = set()
        pages = max(1, min(50, (limit + 19) // 20))

        for page in range(1, pages + 1):
            data = self.search_videos(keyword=keyword, page=page, order=order)
            if not data or not data.get("data"):
                break
            result = data["data"].get("result", []) or []
            if not result:
                break
            for item in result:
                aid = item.get("aid") or item.get("id")
                bvid = item.get("bvid", "")
                key = aid or bvid
                if not key or key in seen:
                    continue
                seen.add(key)
                videos.append(self.normalize_video_search_item(item, keyword, len(videos) + 1))
                if len(videos) >= limit:
                    return videos

        return videos

    # ============================================================
    #  动态相关
    # ============================================================
    def get_dynamic_detail(self, dynamic_id: int) -> Optional[Dict]:
        """
        获取动态详情（新版API）

        通过动态详情可以获取评论区的 oid 和 type

        Args:
            dynamic_id: 动态ID

        Returns:
            动态详情字典
        """
        params = {
            "id": dynamic_id,
            "timezone_offset": -480,
        }
        return self._request(DYNAMIC_DETAIL_API_URL, params)

    # ============================================================
    #  专栏文章相关
    # ============================================================
    def get_article_info(self, cvid: int) -> Optional[Dict]:
        """
        获取专栏文章信息

        Args:
            cvid: 文章CV号

        Returns:
            文章信息字典
        """
        params = {"id": cvid}
        return self._request(ARTICLE_INFO_API_URL, params)

    # ============================================================
    #  评论相关（通用）
    # ============================================================
    def get_comments(
        self,
        oid: int,
        page: int = 1,
        mode: int = 3,
        type_id: int = 1,
        next_page: int = 0,
    ) -> Optional[Dict]:
        """
        获取评论列表（通用，支持视频/动态/文章）

        Args:
            oid: 对象ID（视频aid / 动态ID / 文章cvid）
            page: 页码（兼容旧版API）
            mode: 排序模式，3=按时间排序，2=按热度排序
            type_id: 类型ID，1=视频, 11=图文动态, 12=专栏, 17=文字动态
            next_page: 下一页标识（cursor.next值），用于新版API分页

        Returns:
            评论数据字典
        """
        params = {
            "oid": oid,
            "type": type_id,
            "mode": mode,
            "pn": page,
            "ps": DEFAULT_PAGE_SIZE,
            "next": next_page,
        }
        return self._request(COMMENT_API_URL, params)

    def get_replies(
        self, oid: int, root: int, page: int = 1, type_id: int = 1
    ) -> Optional[Dict]:
        """
        获取评论的回复（子评论）

        Args:
            oid: 对象ID
            root: 根评论ID
            page: 页码
            type_id: 类型ID

        Returns:
            回复数据字典
        """
        params = {
            "oid": oid,
            "type": type_id,
            "root": root,
            "pn": page,
            "ps": DEFAULT_PAGE_SIZE,
        }
        return self._request(REPLY_API_URL, params)

    def set_cookie(self, cookie: str):
        """
        设置Cookie（用于需要登录的场景）

        Args:
            cookie: Cookie字符串
        """
        self.session.headers.update({"Cookie": cookie})
