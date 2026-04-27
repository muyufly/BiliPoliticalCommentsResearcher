"""
使用 customtkinter 的现代化 B站风格 GUI
"""
import logging
import json
import threading
import tkinter.filedialog as filedialog
import tkinter.messagebox as messagebox
from pathlib import Path
from typing import Optional

import customtkinter as ctk

from config.config import DEFAULT_COMMENTS_PER_VIDEO, DEFAULT_OUTPUT_DIR, DEFAULT_SEARCH_VIDEO_LIMIT
from config.user_config import load_user_config, save_user_config
from src.crawler.comment_crawler import CommentCrawler
from src.exporter.csv_exporter import CSVExporter
from src.processor.data_processor import DataProcessor
from src.research.deep_cleaning import (
    create_summary_output_dir,
    run_deep_cleaning_backfill,
    run_deep_cleaning_summary,
)
from src.research.text_analyzer import (
    analyze_text,
    apply_lexicon_candidates,
    create_text_analysis_output_dir,
    load_lexicon_candidates,
    reject_lexicon_candidates,
)
from src.research.pipeline import ResearchPipeline
from src.gui.theme import Theme, init_theme
from src.gui.widgets.header_bar import HeaderBar
from src.gui.widgets.card_frame import CardFrame
from src.gui.widgets.stat_card import StatCard
from src.gui.widgets.log_console import LogConsole

logger = logging.getLogger(__name__)


class MainWindow:
    """主窗口类（customtkinter版）"""

    def __init__(self, root: ctk.CTk):
        self.root = root
        init_theme()
        self.appearance = "light"

        self.root.title("BiliPoliticalCommentsResearcher (BPCR)")
        self.root.geometry("980x980")
        self.root.minsize(920, 820)
        self.root.configure(fg_color=Theme.BACKGROUND)

        # 逻辑
        self.crawler: Optional[CommentCrawler] = None
        self.research_pipeline: Optional[ResearchPipeline] = None
        self.crawler_thread: Optional[threading.Thread] = None
        self.research_thread: Optional[threading.Thread] = None
        self.summary_thread: Optional[threading.Thread] = None
        self.backfill_thread: Optional[threading.Thread] = None
        self.text_analysis_thread: Optional[threading.Thread] = None
        self.is_crawling = False
        self.comments = []
        self.stat_cards = {}
        self._all_cards = []  # 收集所有 CardFrame 用于主题切换
        self._last_external_progress_key = ""
        self._last_status_log_key = ""

        self._build_layout()
        self._schedule_external_progress_poll()
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)

    # ============================================================
    #  UI 构建
    # ============================================================
    def _build_layout(self):
        self.root.grid_columnconfigure(0, weight=1)
        self.root.grid_rowconfigure(1, weight=1)
        self.root.grid_rowconfigure(3, weight=1)

        # 顶部栏
        self.header = HeaderBar(self.root, on_toggle_theme=self._toggle_theme)
        self.header.grid(row=0, column=0, sticky="we", padx=20, pady=(16, 6))

        self.tabs = ctk.CTkTabview(
            self.root,
            fg_color=Theme.SURFACE,
            segmented_button_fg_color=Theme.BORDER,
            segmented_button_selected_color=Theme.PRIMARY,
            segmented_button_selected_hover_color=Theme.PRIMARY,
            segmented_button_unselected_color=Theme.SURFACE,
            segmented_button_unselected_hover_color=Theme.BORDER,
            text_color=Theme.TEXT_PRIMARY,
        )
        self.tabs.grid(row=1, column=0, sticky="nsew", padx=20, pady=6)
        self.crawl_tab = self.tabs.add("爬取研究")
        self.text_tab = self.tabs.add("文本验证")
        self.summary_tab = self.tabs.add("汇总报告")
        for tab in (self.crawl_tab, self.text_tab, self.summary_tab):
            tab.grid_columnconfigure(0, weight=1)

        self._build_video_card(self.crawl_tab, row=0)
        self._build_params_card(self.crawl_tab, row=1)
        self._build_research_card(self.crawl_tab, row=2)
        self._build_actions(self.crawl_tab, row=3)
        self._build_text_validation_tab(self.text_tab)
        self._build_summary_tab_v2(self.summary_tab)
        self._build_stat_cards()
        self._build_log_console()

    def _build_video_card(self, parent=None, row=1):
        parent = parent or self.root
        card = CardFrame(parent)
        card.grid(row=row, column=0, sticky="we", padx=10, pady=6)
        card.grid_columnconfigure(1, weight=1)
        self._all_cards.append(card)

        title = ctk.CTkLabel(card, text="内容信息", font=Theme.FONT_SECTION, text_color=Theme.TEXT_PRIMARY)
        title.grid(row=0, column=0, columnspan=2, sticky="w", padx=14, pady=(12, 4))
        self._video_title_label = title

        label = ctk.CTkLabel(card, text="链接 / ID", text_color=Theme.TEXT_SECONDARY, font=Theme.FONT_NORMAL)
        label.grid(row=1, column=0, sticky="w", padx=14, pady=(6, 12))
        self._video_label = label

        self.video_entry = ctk.CTkEntry(
            card,
            placeholder_text="视频BV号/链接、动态链接(t.bilibili.com/xxx)、文章cv号/链接",
            corner_radius=Theme.RADIUS_INPUT,
            border_width=1,
            fg_color=Theme.SURFACE,
            border_color=Theme.BORDER,
            font=Theme.FONT_NORMAL,
            text_color=Theme.TEXT_PRIMARY,
        )
        self.video_entry.grid(row=1, column=1, sticky="we", padx=14, pady=(6, 12))

    def _build_params_card(self, parent=None, row=2):
        parent = parent or self.root
        card = CardFrame(parent)
        card.grid(row=row, column=0, sticky="we", padx=10, pady=6)
        for i in range(3):
            card.grid_columnconfigure(i, weight=1)
        self._all_cards.append(card)

        title = ctk.CTkLabel(card, text="爬取参数", font=Theme.FONT_SECTION, text_color=Theme.TEXT_PRIMARY)
        title.grid(row=0, column=0, columnspan=3, sticky="w", padx=14, pady=(12, 6))
        self._params_title_label = title

        # 开关
        self.include_replies_var = ctk.BooleanVar(value=True)
        self.include_switch = ctk.CTkSwitch(
            card,
            text="包含子评论/回复",
            variable=self.include_replies_var,
            onvalue=True,
            offvalue=False,
            fg_color=Theme.BORDER,
            progress_color=Theme.PRIMARY,
            button_color=Theme.SURFACE,
            button_hover_color=Theme.BORDER,
            text_color=Theme.TEXT_PRIMARY,
            font=Theme.FONT_NORMAL,
        )
        self.include_switch.grid(row=1, column=0, sticky="w", padx=14, pady=(4, 10))

        # 最大爬取页数
        self._pages_label = ctk.CTkLabel(
            card, text="最大爬取页数", text_color=Theme.TEXT_SECONDARY, font=Theme.FONT_NORMAL
        )
        self._pages_label.grid(row=1, column=1, sticky="w", padx=14, pady=(4, 2))
        self.max_pages_var = ctk.StringVar(value="100")
        self.max_pages_entry = ctk.CTkEntry(
            card,
            textvariable=self.max_pages_var,
            width=120,
            corner_radius=Theme.RADIUS_INPUT,
            border_width=1,
            fg_color=Theme.SURFACE,
            border_color=Theme.BORDER,
            font=Theme.FONT_NORMAL,
            text_color=Theme.TEXT_PRIMARY,
        )
        self.max_pages_entry.grid(row=2, column=1, sticky="w", padx=14, pady=(0, 10))

        # 排序模式
        self._sort_label = ctk.CTkLabel(
            card, text="排序模式", text_color=Theme.TEXT_SECONDARY, font=Theme.FONT_NORMAL
        )
        self._sort_label.grid(row=1, column=2, sticky="w", padx=14, pady=(4, 2))
        self.sort_mode_var = ctk.StringVar(value="3")
        self.sort_segment = ctk.CTkSegmentedButton(
            card,
            values=["按时间", "按热度"],
            font=("Microsoft YaHei UI", 13),
            width=200,
            fg_color=Theme.BORDER,
            selected_color=Theme.PRIMARY,
            selected_hover_color=Theme.PRIMARY,
            unselected_color=Theme.SURFACE,
            unselected_hover_color=Theme.PRIMARY,
            corner_radius=Theme.RADIUS_INPUT,
            command=self._on_sort_change,
        )
        self.sort_segment.set("按时间")
        self.sort_segment.grid(row=2, column=2, sticky="w", padx=14, pady=(0, 10))

    def _build_research_card(self, parent=None, row=3):
        parent = parent or self.root
        card = CardFrame(parent)
        card.grid(row=row, column=0, sticky="we", padx=10, pady=6)
        for i in range(4):
            card.grid_columnconfigure(i, weight=1)
        self._all_cards.append(card)

        title = ctk.CTkLabel(card, text="研究模式", font=Theme.FONT_SECTION, text_color=Theme.TEXT_PRIMARY)
        title.grid(row=0, column=0, columnspan=4, sticky="w", padx=14, pady=(12, 6))
        self._research_title_label = title

        self._song_label = ctk.CTkLabel(card, text="歌曲名 / 关键词", text_color=Theme.TEXT_SECONDARY, font=Theme.FONT_NORMAL)
        self._song_label.grid(row=1, column=0, sticky="w", padx=14, pady=(4, 2))
        self.song_entry = ctk.CTkEntry(
            card,
            placeholder_text="例如：权御天下 / 牵丝戏 / 山河令",
            corner_radius=Theme.RADIUS_INPUT,
            border_width=1,
            fg_color=Theme.SURFACE,
            border_color=Theme.BORDER,
            font=Theme.FONT_NORMAL,
            text_color=Theme.TEXT_PRIMARY,
        )
        self.song_entry.grid(row=2, column=0, columnspan=2, sticky="we", padx=14, pady=(0, 10))

        self._video_limit_label = ctk.CTkLabel(card, text="视频上限", text_color=Theme.TEXT_SECONDARY, font=Theme.FONT_NORMAL)
        self._video_limit_label.grid(row=1, column=2, sticky="w", padx=14, pady=(4, 2))
        self.video_limit_var = ctk.StringVar(value=str(DEFAULT_SEARCH_VIDEO_LIMIT))
        self.video_limit_entry = ctk.CTkEntry(
            card,
            textvariable=self.video_limit_var,
            corner_radius=Theme.RADIUS_INPUT,
            border_width=1,
            fg_color=Theme.SURFACE,
            border_color=Theme.BORDER,
            font=Theme.FONT_NORMAL,
            text_color=Theme.TEXT_PRIMARY,
        )
        self.video_limit_entry.grid(row=2, column=2, sticky="we", padx=14, pady=(0, 10))

        self._comment_limit_label = ctk.CTkLabel(card, text="每视频评论上限", text_color=Theme.TEXT_SECONDARY, font=Theme.FONT_NORMAL)
        self._comment_limit_label.grid(row=1, column=3, sticky="w", padx=14, pady=(4, 2))
        self.comment_limit_var = ctk.StringVar(value=str(DEFAULT_COMMENTS_PER_VIDEO))
        self.comment_limit_entry = ctk.CTkEntry(
            card,
            textvariable=self.comment_limit_var,
            corner_radius=Theme.RADIUS_INPUT,
            border_width=1,
            fg_color=Theme.SURFACE,
            border_color=Theme.BORDER,
            font=Theme.FONT_NORMAL,
            text_color=Theme.TEXT_PRIMARY,
        )
        self.comment_limit_entry.grid(row=2, column=3, sticky="we", padx=14, pady=(0, 10))

        self._output_label = ctk.CTkLabel(card, text="输出目录", text_color=Theme.TEXT_SECONDARY, font=Theme.FONT_NORMAL)
        self._output_label.grid(row=3, column=0, sticky="w", padx=14, pady=(4, 2))
        self.output_dir_var = ctk.StringVar(value=DEFAULT_OUTPUT_DIR)
        self.output_dir_entry = ctk.CTkEntry(
            card,
            textvariable=self.output_dir_var,
            corner_radius=Theme.RADIUS_INPUT,
            border_width=1,
            fg_color=Theme.SURFACE,
            border_color=Theme.BORDER,
            font=Theme.FONT_NORMAL,
            text_color=Theme.TEXT_PRIMARY,
        )
        self.output_dir_entry.grid(row=4, column=0, columnspan=2, sticky="we", padx=14, pady=(0, 12))

        self.output_browse_btn = ctk.CTkButton(
            card,
            text="选择目录",
            width=90,
            height=34,
            corner_radius=Theme.RADIUS_BUTTON,
            fg_color=Theme.ACCENT,
            hover_color="#1f9bcb",
            text_color="white",
            command=self._browse_output_dir,
        )
        self.output_browse_btn.grid(row=4, column=2, sticky="w", padx=14, pady=(0, 12))

        self.ai_enabled_var = ctk.BooleanVar(value=False)
        self.ai_switch = ctk.CTkSwitch(
            card,
            text="启用AI主题标签",
            variable=self.ai_enabled_var,
            onvalue=True,
            offvalue=False,
            fg_color=Theme.BORDER,
            progress_color=Theme.PRIMARY,
            button_color=Theme.SURFACE,
            button_hover_color=Theme.BORDER,
            text_color=Theme.TEXT_PRIMARY,
            font=Theme.FONT_NORMAL,
        )
        self.ai_switch.grid(row=4, column=3, sticky="w", padx=14, pady=(0, 12))

        bottom = ctk.CTkFrame(card, fg_color="transparent")
        bottom.grid(row=5, column=0, columnspan=4, sticky="we", padx=14, pady=(0, 12))
        bottom.grid_columnconfigure(0, weight=1)

        self.cookie_status_var = ctk.StringVar(value=self._credential_status_text())
        self.cookie_status_label = ctk.CTkLabel(
            bottom,
            textvariable=self.cookie_status_var,
            text_color=Theme.TEXT_SECONDARY,
            font=Theme.FONT_NORMAL,
        )
        self.cookie_status_label.grid(row=0, column=0, sticky="w")

        self.credential_btn = ctk.CTkButton(
            bottom,
            text="配置凭据",
            width=110,
            height=34,
            corner_radius=Theme.RADIUS_BUTTON,
            fg_color=Theme.ACCENT,
            hover_color="#1f9bcb",
            text_color="white",
            command=self._open_credential_dialog,
        )
        self.credential_btn.grid(row=0, column=1, sticky="e", padx=6)

        self.start_research_button = ctk.CTkButton(
            bottom,
            text="开始研究",
            width=120,
            height=34,
            corner_radius=Theme.RADIUS_BUTTON,
            fg_color=Theme.PRIMARY,
            hover_color="#f25f88",
            text_color="white",
            font=("Microsoft YaHei UI", 13, "bold"),
            command=self._start_research,
        )
        self.start_research_button.grid(row=0, column=2, sticky="e", padx=6)

    def _build_actions(self, parent=None, row=4):
        parent = parent or self.root
        frame = CardFrame(parent)
        frame.grid(row=row, column=0, sticky="we", padx=10, pady=(4, 6))
        frame.grid_columnconfigure(0, weight=1)
        self._all_cards.append(frame)

        self._path_label = ctk.CTkLabel(
            frame, text="导出路径", text_color=Theme.TEXT_SECONDARY, font=Theme.FONT_NORMAL
        )
        self._path_label.grid(row=0, column=0, sticky="w", padx=14, pady=(10, 4))

        path_frame = ctk.CTkFrame(frame, fg_color="transparent")
        path_frame.grid(row=1, column=0, sticky="we", padx=14, pady=(0, 10))
        path_frame.grid_columnconfigure(0, weight=1)

        self.export_path_var = ctk.StringVar(value="bilibili_comments.csv")
        self.path_entry = ctk.CTkEntry(
            path_frame,
            textvariable=self.export_path_var,
            corner_radius=Theme.RADIUS_INPUT,
            border_width=1,
            fg_color=Theme.SURFACE,
            border_color=Theme.BORDER,
            font=Theme.FONT_NORMAL,
            text_color=Theme.TEXT_PRIMARY,
        )
        self.path_entry.grid(row=0, column=0, sticky="we", padx=(0, 8))

        self.browse_btn = ctk.CTkButton(
            path_frame,
            text="浏览...",
            width=90,
            height=34,
            corner_radius=Theme.RADIUS_BUTTON,
            fg_color=Theme.ACCENT,
            hover_color="#1f9bcb",
            text_color="white",
            command=self._browse_file,
        )
        self.browse_btn.grid(row=0, column=1, sticky="w")

        btn_wrap = ctk.CTkFrame(frame, fg_color="transparent")
        btn_wrap.grid(row=2, column=0, columnspan=2, sticky="e", padx=8, pady=(4, 10))

        self.start_button = ctk.CTkButton(
            btn_wrap,
            text="▶ 开始爬取",
            height=38,
            width=130,
            corner_radius=Theme.RADIUS_BUTTON,
            fg_color=Theme.PRIMARY,
            hover_color="#f25f88",
            text_color="white",
            border_width=0,
            font=("Microsoft YaHei UI", 13, "bold"),
            command=self._start_crawling,
        )
        self.start_button.pack(side="left", padx=6)

        self.stop_button = ctk.CTkButton(
            btn_wrap,
            text="⏹ 停止",
            height=38,
            width=100,
            corner_radius=Theme.RADIUS_BUTTON,
            fg_color=Theme.DISABLED_BG,
            hover_color=Theme.DISABLED_BG,
            text_color=Theme.DISABLED_FG,
            border_width=0,
            font=("Microsoft YaHei UI", 13),
            state="disabled",
            command=self._stop_crawling,
        )
        self.stop_button.pack(side="left", padx=6)

        self.export_button = ctk.CTkButton(
            btn_wrap,
            text="💾 导出 CSV",
            height=38,
            width=130,
            corner_radius=Theme.RADIUS_BUTTON,
            fg_color=Theme.DISABLED_BG,
            hover_color=Theme.DISABLED_BG,
            text_color=Theme.DISABLED_FG,
            border_width=0,
            font=("Microsoft YaHei UI", 13),
            state="disabled",
            command=self._export_csv,
        )
        self.export_button.pack(side="left", padx=6)

    def _build_text_validation_tab(self, parent):
        parent.grid_rowconfigure(0, weight=1)
        parent.grid_columnconfigure(0, weight=3)
        parent.grid_columnconfigure(1, weight=2)

        input_card = CardFrame(parent)
        input_card.grid(row=0, column=0, sticky="nsew", padx=(10, 6), pady=6)
        input_card.grid_columnconfigure(0, weight=1)
        input_card.grid_rowconfigure(1, weight=1)
        self._all_cards.append(input_card)

        title = ctk.CTkLabel(input_card, text="单段文本验证", font=Theme.FONT_SECTION, text_color=Theme.TEXT_PRIMARY)
        title.grid(row=0, column=0, sticky="w", padx=14, pady=(12, 6))

        self.text_validation_box = ctk.CTkTextbox(input_card, height=300, font=Theme.FONT_NORMAL)
        self.text_validation_box.grid(row=1, column=0, sticky="nsew", padx=14, pady=8)

        controls = ctk.CTkFrame(input_card, fg_color="transparent")
        controls.grid(row=2, column=0, sticky="we", padx=14, pady=(4, 12))
        for i in range(4):
            controls.grid_columnconfigure(i, weight=1)

        self.expected_stance_var = ctk.StringVar(value="左")
        self.expected_stance_menu = ctk.CTkOptionMenu(
            controls,
            variable=self.expected_stance_var,
            values=["神", "左", "兔", "皇", "乐子人"],
            fg_color=Theme.ACCENT,
            button_color=Theme.ACCENT,
            button_hover_color="#1f9bcb",
        )
        self.expected_stance_menu.grid(row=0, column=0, sticky="w", padx=(0, 8))

        self.text_fuzzy_var = ctk.BooleanVar(value=True)
        self.text_fuzzy_switch = ctk.CTkSwitch(
            controls,
            text="模糊匹配",
            variable=self.text_fuzzy_var,
            onvalue=True,
            offvalue=False,
            fg_color=Theme.BORDER,
            progress_color=Theme.PRIMARY,
            text_color=Theme.TEXT_PRIMARY,
            font=Theme.FONT_NORMAL,
        )
        self.text_fuzzy_switch.grid(row=0, column=1, sticky="w", padx=8)

        self.text_ai_var = ctk.BooleanVar(value=False)
        self.text_ai_switch = ctk.CTkSwitch(
            controls,
            text="AI全文理解",
            variable=self.text_ai_var,
            onvalue=True,
            offvalue=False,
            fg_color=Theme.BORDER,
            progress_color=Theme.PRIMARY,
            text_color=Theme.TEXT_PRIMARY,
            font=Theme.FONT_NORMAL,
        )
        self.text_ai_switch.grid(row=0, column=2, sticky="w", padx=8)

        self.text_correction_var = ctk.BooleanVar(value=True)
        self.text_correction_switch = ctk.CTkSwitch(
            controls,
            text="不符合时生成候选",
            variable=self.text_correction_var,
            onvalue=True,
            offvalue=False,
            fg_color=Theme.BORDER,
            progress_color=Theme.PRIMARY,
            text_color=Theme.TEXT_PRIMARY,
            font=Theme.FONT_NORMAL,
        )
        self.text_correction_switch.grid(row=0, column=3, sticky="w", padx=8)

        actions = ctk.CTkFrame(input_card, fg_color="transparent")
        actions.grid(row=3, column=0, sticky="e", padx=14, pady=(0, 12))

        self.load_text_button = ctk.CTkButton(
            actions,
            text="上传文本",
            width=100,
            fg_color=Theme.ACCENT,
            hover_color="#1f9bcb",
            command=self._load_text_validation_file,
        )
        self.load_text_button.pack(side="left", padx=6)
        self.run_text_button = ctk.CTkButton(
            actions,
            text="开始验证",
            width=110,
            fg_color=Theme.PRIMARY,
            hover_color="#f25f88",
            command=self._start_text_validation,
        )
        self.run_text_button.pack(side="left", padx=6)

        result_card = CardFrame(parent)
        result_card.grid(row=0, column=1, sticky="nsew", padx=(6, 10), pady=6)
        result_card.grid_columnconfigure(0, weight=1)
        result_card.grid_rowconfigure(3, weight=1)
        self._all_cards.append(result_card)

        result_title = ctk.CTkLabel(result_card, text="结果与候选词库", font=Theme.FONT_SECTION, text_color=Theme.TEXT_PRIMARY)
        result_title.grid(row=0, column=0, sticky="w", padx=14, pady=(12, 6))

        self.text_validation_status_var = ctk.StringVar(value="尚未分析")
        self.text_validation_status_label = ctk.CTkLabel(
            result_card,
            textvariable=self.text_validation_status_var,
            text_color=Theme.TEXT_SECONDARY,
            font=Theme.FONT_NORMAL,
            justify="left",
        )
        self.text_validation_status_label.grid(row=1, column=0, sticky="we", padx=14, pady=4)

        candidate_actions = ctk.CTkFrame(result_card, fg_color="transparent")
        candidate_actions.grid(row=2, column=0, sticky="we", padx=14, pady=4)
        self.refresh_candidates_button = ctk.CTkButton(
            candidate_actions,
            text="刷新候选",
            width=90,
            fg_color=Theme.ACCENT,
            command=self._refresh_candidate_box,
        )
        self.refresh_candidates_button.pack(side="left", padx=(0, 6))
        self.accept_candidates_button = ctk.CTkButton(
            candidate_actions,
            text="接受全部待审",
            width=120,
            fg_color=Theme.PRIMARY,
            hover_color="#f25f88",
            command=self._accept_pending_candidates,
        )
        self.accept_candidates_button.pack(side="left", padx=6)
        self.reject_candidates_button = ctk.CTkButton(
            candidate_actions,
            text="拒绝全部待审",
            width=120,
            fg_color=Theme.DANGER,
            hover_color=Theme.DANGER_HOVER,
            command=self._reject_pending_candidates,
        )
        self.reject_candidates_button.pack(side="left", padx=6)

        self.candidate_box = ctk.CTkTextbox(result_card, height=300, font=Theme.FONT_MONO)
        self.candidate_box.grid(row=3, column=0, sticky="nsew", padx=14, pady=(4, 12))
        self._refresh_candidate_box()

    def _build_summary_tab(self, parent):
        parent.grid_columnconfigure(0, weight=1)
        card = CardFrame(parent)
        card.grid(row=0, column=0, sticky="we", padx=10, pady=6)
        card.grid_columnconfigure(1, weight=1)
        self._all_cards.append(card)

        title = ctk.CTkLabel(card, text="全量汇总报告", font=Theme.FONT_SECTION, text_color=Theme.TEXT_PRIMARY)
        title.grid(row=0, column=0, columnspan=3, sticky="w", padx=14, pady=(12, 6))

        label = ctk.CTkLabel(card, text="结果目录", text_color=Theme.TEXT_SECONDARY, font=Theme.FONT_NORMAL)
        label.grid(row=1, column=0, sticky="w", padx=14, pady=8)
        self.summary_result_root_var = ctk.StringVar(value="result")
        self.summary_result_root_entry = ctk.CTkEntry(
            card,
            textvariable=self.summary_result_root_var,
            fg_color=Theme.SURFACE,
            border_color=Theme.BORDER,
            text_color=Theme.TEXT_PRIMARY,
            font=Theme.FONT_NORMAL,
        )
        self.summary_result_root_entry.grid(row=1, column=1, sticky="we", padx=8, pady=8)

        self.summary_ai_var = ctk.BooleanVar(value=False)
        self.summary_ai_switch = ctk.CTkSwitch(
            card,
            text="启用AI复核",
            variable=self.summary_ai_var,
            onvalue=True,
            offvalue=False,
            fg_color=Theme.BORDER,
            progress_color=Theme.PRIMARY,
            text_color=Theme.TEXT_PRIMARY,
            font=Theme.FONT_NORMAL,
        )
        self.summary_ai_switch.grid(row=1, column=2, sticky="w", padx=14, pady=8)

        self.start_summary_button = ctk.CTkButton(
            card,
            text="生成汇总报告",
            width=140,
            height=36,
            corner_radius=Theme.RADIUS_BUTTON,
            fg_color=Theme.PRIMARY,
            hover_color="#f25f88",
            text_color="white",
            font=("Microsoft YaHei UI", 13, "bold"),
            command=self._start_deep_summary,
        )
        self.start_summary_button.grid(row=2, column=2, sticky="e", padx=14, pady=(4, 14))

        self.summary_status_var = ctk.StringVar(value="尚未运行")
        self.summary_status_label = ctk.CTkLabel(
            card,
            textvariable=self.summary_status_var,
            text_color=Theme.TEXT_SECONDARY,
            font=Theme.FONT_NORMAL,
            justify="left",
        )
        self.summary_status_label.grid(row=2, column=0, columnspan=2, sticky="w", padx=14, pady=(4, 14))

    def _build_summary_tab_v2(self, parent):
        parent.grid_columnconfigure(0, weight=1)
        card = CardFrame(parent)
        card.grid(row=0, column=0, sticky="we", padx=10, pady=6)
        card.grid_columnconfigure(1, weight=1)
        self._all_cards.append(card)

        title = ctk.CTkLabel(card, text="汇总报告与回填", font=Theme.FONT_SECTION, text_color=Theme.TEXT_PRIMARY)
        title.grid(row=0, column=0, columnspan=3, sticky="w", padx=14, pady=(12, 6))

        label = ctk.CTkLabel(card, text="结果目录", text_color=Theme.TEXT_SECONDARY, font=Theme.FONT_NORMAL)
        label.grid(row=1, column=0, sticky="w", padx=14, pady=8)
        self.summary_result_root_var = ctk.StringVar(value="result")
        self.summary_result_root_entry = ctk.CTkEntry(
            card,
            textvariable=self.summary_result_root_var,
            fg_color=Theme.SURFACE,
            border_color=Theme.BORDER,
            text_color=Theme.TEXT_PRIMARY,
            font=Theme.FONT_NORMAL,
        )
        self.summary_result_root_entry.grid(row=1, column=1, sticky="we", padx=8, pady=8)

        self.summary_browse_button = ctk.CTkButton(
            card,
            text="选择目录",
            width=96,
            height=34,
            corner_radius=Theme.RADIUS_BUTTON,
            fg_color=Theme.ACCENT,
            hover_color="#1f9bcb",
            text_color="white",
            command=self._browse_summary_result_root,
        )
        self.summary_browse_button.grid(row=1, column=2, sticky="e", padx=14, pady=8)

        self.summary_ai_var = ctk.BooleanVar(value=False)
        self.summary_ai_switch = ctk.CTkSwitch(
            card,
            text="启用 AI 复核",
            variable=self.summary_ai_var,
            onvalue=True,
            offvalue=False,
            fg_color=Theme.BORDER,
            progress_color=Theme.PRIMARY,
            text_color=Theme.TEXT_PRIMARY,
            font=Theme.FONT_NORMAL,
        )
        self.summary_ai_switch.grid(row=2, column=0, sticky="w", padx=14, pady=8)

        self.summary_backfill_var = ctk.BooleanVar(value=True)
        self.summary_backfill_switch = ctk.CTkSwitch(
            card,
            text="先回填每首歌新版清洗结果",
            variable=self.summary_backfill_var,
            onvalue=True,
            offvalue=False,
            fg_color=Theme.BORDER,
            progress_color=Theme.PRIMARY,
            text_color=Theme.TEXT_PRIMARY,
            font=Theme.FONT_NORMAL,
        )
        self.summary_backfill_switch.grid(row=2, column=1, sticky="w", padx=8, pady=8)

        self.summary_hint_label = ctk.CTkLabel(
            card,
            text="回填会在每首歌目录下生成 deep_cleaning_v2/，不覆盖原始抓取和旧报告。",
            text_color=Theme.TEXT_SECONDARY,
            font=Theme.FONT_NORMAL,
            justify="left",
        )
        self.summary_hint_label.grid(row=3, column=0, columnspan=3, sticky="w", padx=14, pady=(2, 8))

        action_frame = ctk.CTkFrame(card, fg_color="transparent")
        action_frame.grid(row=4, column=0, columnspan=3, sticky="e", padx=14, pady=(4, 10))

        self.start_backfill_button = ctk.CTkButton(
            action_frame,
            text="回填单曲 + 生成汇总",
            width=180,
            height=36,
            corner_radius=Theme.RADIUS_BUTTON,
            fg_color=Theme.PRIMARY,
            hover_color="#f25f88",
            text_color="white",
            font=("Microsoft YaHei UI", 13, "bold"),
            command=self._start_backfill_summary,
        )
        self.start_backfill_button.pack(side="left", padx=(0, 8))

        self.start_summary_button = ctk.CTkButton(
            action_frame,
            text="仅生成汇总",
            width=140,
            height=36,
            corner_radius=Theme.RADIUS_BUTTON,
            fg_color=Theme.ACCENT,
            hover_color="#1f9bcb",
            text_color="white",
            font=("Microsoft YaHei UI", 13, "bold"),
            command=self._start_deep_summary,
        )
        self.start_summary_button.pack(side="left")

        self.summary_status_var = ctk.StringVar(value="尚未运行")
        self.summary_status_label = ctk.CTkLabel(
            card,
            textvariable=self.summary_status_var,
            text_color=Theme.TEXT_SECONDARY,
            font=Theme.FONT_NORMAL,
            justify="left",
        )
        self.summary_status_label.grid(row=5, column=0, columnspan=3, sticky="w", padx=14, pady=(2, 14))

    def _build_stat_cards(self):
        frame = ctk.CTkFrame(self.root, fg_color="transparent")
        frame.grid(row=2, column=0, sticky="we", padx=20, pady=4)
        for i in range(4):
            frame.grid_columnconfigure(i, weight=1)
        self._stat_frame = frame

        self.stat_cards["total"] = StatCard(
            frame, "📊", "总评论数", "0", Theme.STAT_PINK, bg_tint=Theme.STAT_BG_PINK
        )
        self.stat_cards["main"] = StatCard(
            frame, "💬", "主评论", "0", Theme.STAT_BLUE, bg_tint=Theme.STAT_BG_BLUE
        )
        self.stat_cards["replies"] = StatCard(
            frame, "↩️", "回复", "0", Theme.STAT_GREEN, bg_tint=Theme.STAT_BG_GREEN
        )
        self.stat_cards["likes"] = StatCard(
            frame, "👍", "总点赞", "0", Theme.STAT_ORANGE, bg_tint=Theme.STAT_BG_ORANGE
        )

        for idx, key in enumerate(["total", "main", "replies", "likes"]):
            self.stat_cards[key].grid(row=0, column=idx, padx=6, pady=4, sticky="we")

    def _build_log_console(self):
        self.log_card = CardFrame(self.root)
        self.log_card.grid(row=3, column=0, sticky="nsew", padx=20, pady=(4, 16))
        self.root.grid_rowconfigure(3, weight=1)
        self.log_card.grid_columnconfigure(0, weight=1)
        self.log_card.grid_rowconfigure(1, weight=1)
        self._all_cards.append(self.log_card)

        self._log_title = ctk.CTkLabel(
            self.log_card, text="日志输出", font=Theme.FONT_SECTION, text_color=Theme.TEXT_PRIMARY
        )
        self._log_title.grid(row=0, column=0, sticky="w", padx=14, pady=(12, 4))

        self.log_console = LogConsole(self.log_card, dark_mode=False)
        self.log_console.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 10))

        # 进度条 — 初始静止
        self.progress_bar = ctk.CTkProgressBar(
            self.log_card,
            height=8,
            fg_color=Theme.BORDER,
            progress_color=Theme.PRIMARY,
            corner_radius=Theme.RADIUS_INPUT,
            mode="determinate",
        )
        self.progress_bar.grid(row=2, column=0, sticky="we", padx=14, pady=(0, 10))
        self.progress_bar.set(0)  # 空闲时静止

        self.progress_var = ctk.StringVar(value="就绪")
        self.progress_label = ctk.CTkLabel(
            self.log_card,
            textvariable=self.progress_var,
            text_color=Theme.TEXT_SECONDARY,
            font=Theme.FONT_NORMAL,
        )
        self.progress_label.grid(row=3, column=0, sticky="w", padx=14, pady=(0, 12))

    # ============================================================
    #  事件处理
    # ============================================================
    def _on_sort_change(self, value):
        self.sort_mode_var.set("3" if value == "按时间" else "2")

    def _toggle_theme(self):
        self.appearance = "dark" if self.appearance == "light" else "light"
        is_dark = self.appearance == "dark"
        Theme.set_mode(self.appearance)
        ctk.set_appearance_mode(self.appearance)

        # 更新组件
        self.root.configure(fg_color=Theme.get("BACKGROUND"))
        self.header.set_mode_icon(self.appearance)
        self.header.update_theme()

        for card in self._all_cards:
            card.update_theme()

        # 统计卡片
        tint_map = {
            "total": "STAT_BG_PINK",
            "main": "STAT_BG_BLUE",
            "replies": "STAT_BG_GREEN",
            "likes": "STAT_BG_ORANGE",
        }
        for key, tint_key in tint_map.items():
            self.stat_cards[key].update_theme(bg_tint=Theme.get(tint_key))

        # 日志
        self.log_console.update_theme(is_dark)

        # 各种 label
        text_primary = Theme.get("TEXT_PRIMARY")
        text_secondary = Theme.get("TEXT_SECONDARY")
        surface = Theme.get("SURFACE")
        border = Theme.get("BORDER")

        for lbl in [self._video_title_label, self._params_title_label, self._research_title_label, self._log_title]:
            lbl.configure(text_color=text_primary)
        for lbl in [
            self._video_label,
            self._pages_label,
            self._sort_label,
            self._song_label,
            self._video_limit_label,
            self._comment_limit_label,
            self._output_label,
            self._path_label,
        ]:
            lbl.configure(text_color=text_secondary)
        self.progress_label.configure(text_color=text_secondary)
        self.cookie_status_label.configure(text_color=text_secondary)
        if hasattr(self, "summary_hint_label"):
            self.summary_hint_label.configure(text_color=text_secondary)
        if hasattr(self, "summary_status_label"):
            self.summary_status_label.configure(text_color=text_secondary)

        # 输入框
        for entry in [
            self.video_entry,
            self.max_pages_entry,
            self.song_entry,
            self.video_limit_entry,
            self.comment_limit_entry,
            self.output_dir_entry,
            self.path_entry,
        ]:
            entry.configure(
                fg_color=surface,
                border_color=border,
                text_color=text_primary,
            )
        if hasattr(self, "summary_result_root_entry"):
            self.summary_result_root_entry.configure(
                fg_color=surface,
                border_color=border,
                text_color=text_primary,
            )

        # 开关
        for switch in [self.include_switch, self.ai_switch]:
            switch.configure(
                fg_color=border,
                button_color=surface,
                button_hover_color=border,
                text_color=text_primary,
            )

        # 分段按钮
        self.sort_segment.configure(
            fg_color=border,
            unselected_color=surface,
        )

        # 进度条
        self.progress_bar.configure(fg_color=border)

    def _browse_file(self):
        filename = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV文件", "*.csv"), ("所有文件", "*.*")],
        )
        if filename:
            self.export_path_var.set(filename)

    def _browse_output_dir(self):
        dirname = filedialog.askdirectory()
        if dirname:
            self.output_dir_var.set(dirname)

    def _browse_summary_result_root(self):
        dirname = filedialog.askdirectory()
        if dirname:
            self.summary_result_root_var.set(dirname)

    def _load_text_validation_file(self):
        path = filedialog.askopenfilename(
            filetypes=[("Text files", "*.txt;*.md;*.csv"), ("All files", "*.*")]
        )
        if not path:
            return
        try:
            content = Path(path).read_text(encoding="utf-8")
        except UnicodeDecodeError:
            content = Path(path).read_text(encoding="gb18030", errors="ignore")
        self.text_validation_box.delete("1.0", "end")
        self.text_validation_box.insert("1.0", content)
        self.text_validation_status_var.set(f"已加载: {path}")

    def _format_pending_candidates(self) -> str:
        payload = load_lexicon_candidates()
        rows = []
        for item in payload.get("candidates", []):
            if item.get("status") != "pending":
                continue
            terms = " + ".join(str(term) for term in item.get("terms", []))
            rows.append(
                f"{item.get('candidate_id')} | {item.get('candidate_type')} | "
                f"{item.get('stance')} | {item.get('meaning_label')} | {terms}\n"
                f"  置信度: {item.get('confidence')} | {item.get('reason', '')}"
            )
        return "\n\n".join(rows) if rows else "暂无待审核候选。"

    def _refresh_candidate_box(self):
        if not hasattr(self, "candidate_box"):
            return
        self.candidate_box.delete("1.0", "end")
        self.candidate_box.insert("1.0", self._format_pending_candidates())

    def _accept_pending_candidates(self):
        try:
            result = apply_lexicon_candidates([])
            self._refresh_candidate_box()
            self.text_validation_status_var.set(f"已接受 {result.get('applied_count', 0)} 条候选")
            self._log(f"词库候选已合并: {result}")
        except Exception as e:
            messagebox.showerror("错误", f"接受候选失败:\n{e}")

    def _reject_pending_candidates(self):
        try:
            payload = load_lexicon_candidates()
            ids = [
                str(item.get("candidate_id"))
                for item in payload.get("candidates", [])
                if item.get("status") == "pending"
            ]
            result = reject_lexicon_candidates(ids)
            self._refresh_candidate_box()
            self.text_validation_status_var.set(f"已拒绝 {result.get('rejected_count', 0)} 条候选")
            self._log(f"词库候选已拒绝: {result}")
        except Exception as e:
            messagebox.showerror("错误", f"拒绝候选失败:\n{e}")

    def _credential_status_text(self) -> str:
        config = load_user_config()
        has_cookie = bool(config.get("bilibili_cookie", "").strip())
        openai = config.get("openai", {})
        google = config.get("google", {})
        provider = config.get("ai_provider", "openai")
        has_openai = bool(openai.get("api_key", "").strip())
        has_google = bool(google.get("api_key", "").strip())
        ai_ready = has_google if provider == "google" else has_openai
        return (
            f"B站Cookie: {'已配置' if has_cookie else '未配置'} | "
            f"AI接口({provider}): {'已配置' if ai_ready else '未配置'}"
        )

    def _open_credential_dialog(self):
        config = load_user_config()
        dialog = ctk.CTkToplevel(self.root)
        dialog.title("配置研究凭据")
        dialog.geometry("760x720")
        dialog.transient(self.root)
        dialog.grab_set()
        dialog.grid_columnconfigure(0, weight=1)
        dialog.grid_rowconfigure(1, weight=1)

        title = ctk.CTkLabel(
            dialog,
            text="凭据仅保存到本地 user_config.json，不写入 output 研究产物",
            font=Theme.FONT_SECTION,
            text_color=Theme.get("TEXT_PRIMARY"),
        )
        title.grid(row=0, column=0, sticky="w", padx=18, pady=(16, 8))

        cookie_box = ctk.CTkTextbox(dialog, height=180, font=Theme.FONT_MONO)
        cookie_box.grid(row=1, column=0, sticky="nsew", padx=18, pady=8)
        cookie_box.insert("1.0", config.get("bilibili_cookie", ""))

        form = ctk.CTkFrame(dialog, fg_color="transparent")
        form.grid(row=2, column=0, sticky="we", padx=18, pady=8)
        form.grid_columnconfigure(1, weight=1)

        provider_label = ctk.CTkLabel(form, text="AI Provider", text_color=Theme.get("TEXT_SECONDARY"))
        provider_label.grid(row=0, column=0, sticky="w", padx=(0, 12), pady=6)
        provider_var = ctk.StringVar(value=config.get("ai_provider", "openai"))
        provider_menu = ctk.CTkOptionMenu(
            form,
            variable=provider_var,
            values=["openai", "google"],
            fg_color=Theme.ACCENT,
            button_color=Theme.ACCENT,
            button_hover_color="#1f9bcb",
        )
        provider_menu.grid(row=0, column=1, sticky="w", pady=6)

        openai_config = config.get("openai", {})
        google_config = config.get("google", {})
        labels = [
            ("OpenAI Base URL", "openai_base_url", openai_config.get("base_url", "")),
            ("OpenAI API Key", "openai_api_key", openai_config.get("api_key", "")),
            ("OpenAI Model", "openai_model", openai_config.get("model", "gpt-4o-mini")),
            ("Google Gemini API Key", "google_api_key", google_config.get("api_key", "")),
            ("Google Gemini Model", "google_model", google_config.get("model", "gemini-2.5-flash")),
        ]
        entries = {}
        for row, (label_text, key, value) in enumerate(labels, start=1):
            label = ctk.CTkLabel(form, text=label_text, text_color=Theme.get("TEXT_SECONDARY"))
            label.grid(row=row, column=0, sticky="w", padx=(0, 12), pady=6)
            entry = ctk.CTkEntry(form, font=Theme.FONT_NORMAL, show="*" if key.endswith("api_key") else "")
            entry.insert(0, value)
            entry.grid(row=row, column=1, sticky="we", pady=6)
            entries[key] = entry

        actions = ctk.CTkFrame(dialog, fg_color="transparent")
        actions.grid(row=3, column=0, sticky="e", padx=18, pady=(8, 18))

        def save_and_close():
            new_config = load_user_config()
            new_config["bilibili_cookie"] = cookie_box.get("1.0", "end").strip()
            new_config["ai_provider"] = provider_var.get().strip() or "openai"
            new_config["openai"] = {
                "base_url": entries["openai_base_url"].get().strip(),
                "api_key": entries["openai_api_key"].get().strip(),
                "model": entries["openai_model"].get().strip() or "gpt-4o-mini",
            }
            new_config["google"] = {
                "api_key": entries["google_api_key"].get().strip(),
                "model": entries["google_model"].get().strip() or "gemini-2.5-flash",
            }
            save_user_config(new_config)
            self.cookie_status_var.set(self._credential_status_text())
            dialog.destroy()

        cancel_btn = ctk.CTkButton(actions, text="取消", width=90, command=dialog.destroy)
        cancel_btn.pack(side="left", padx=6)
        save_btn = ctk.CTkButton(
            actions,
            text="保存",
            width=90,
            fg_color=Theme.PRIMARY,
            hover_color="#f25f88",
            command=save_and_close,
        )
        save_btn.pack(side="left", padx=6)

    def _log(self, message: str):
        self.log_console.write(message)

    def _thread_safe_log(self, message: str):
        """线程安全的日志回调"""
        self.root.after(0, lambda m=message: self._update_progress(m))

    def _update_progress(self, message: str):
        if message.startswith("STATUS_JSON "):
            self._update_research_status(message[len("STATUS_JSON "):], write_log=True)
            return
        self.progress_var.set(message)
        self._log(message)

    def _update_research_status(self, payload: str, write_log: bool = True):
        """更新研究模式实时进度。"""
        try:
            status = json.loads(payload)
        except json.JSONDecodeError:
            self.progress_var.set(payload)
            self._log(payload)
            return

        state = status.get("state", "")
        video_index = status.get("video_index", 0)
        total_videos = status.get("total_videos", 0)
        comments_count = status.get("comments_count", 0)
        note = status.get("note", "")

        state_label = {
            "created": "已创建",
            "crawling": "爬取中",
            "saved": "已保存",
            "rate_limited": "风控暂停",
            "rate_limited_skipped": "跳过等待",
            "rate_limited_waiting": "心跳等待",
        }.get(state, state or "运行中")

        if total_videos:
            self.progress_bar.configure(mode="determinate")
            self.progress_bar.set(min(max(video_index / total_videos, 0), 1))

        self.stat_cards["total"].update_value(str(comments_count))
        self.stat_cards["main"].update_value(f"{video_index}/{total_videos}" if total_videos else str(video_index))
        self.stat_cards["replies"].update_value(state_label)
        self.stat_cards["likes"].update_value(status.get("updated_at", "")[-8:])

        message = f"{state_label} | 视频 {video_index}/{total_videos} | 评论 {comments_count}"
        if note:
            message += f" | {note}"
        self.progress_var.set(message)
        if write_log:
            log_key = "|".join([
                str(state),
                str(video_index),
                str(total_videos),
                str(comments_count),
                str(note),
            ])
            if log_key != self._last_status_log_key:
                self._last_status_log_key = log_key
                self._log(message)

    def _schedule_external_progress_poll(self):
        self.root.after(5000, self._poll_external_progress)

    def _poll_external_progress(self):
        """空闲时显示后台 run_research.py 的最新进度文件。"""
        try:
            if not self.is_crawling:
                progress_files = list(Path("output").glob("*/data/crawl_progress.json"))
                if progress_files:
                    latest = max(progress_files, key=lambda p: p.stat().st_mtime)
                    progress_key = f"{latest}|{latest.stat().st_mtime_ns}"
                    if progress_key == self._last_external_progress_key:
                        return
                    self._last_external_progress_key = progress_key
                    status = json.loads(latest.read_text(encoding="utf-8"))
                    status.setdefault("output_dir", str(latest.parents[1]))
                    self._update_research_status(json.dumps(status, ensure_ascii=False), write_log=False)
        except Exception as e:
            logger.debug(f"读取后台进度失败: {e}")
        finally:
            self._schedule_external_progress_poll()

    def _start_text_validation(self):
        text = self.text_validation_box.get("1.0", "end").strip()
        if not text:
            messagebox.showwarning("警告", "请先粘贴文本或上传文本文件")
            return
        expected_stance = self.expected_stance_var.get().strip()
        output_dir = create_text_analysis_output_dir(Path("output"))
        self.run_text_button.configure(state="disabled", fg_color=Theme.get("DISABLED_BG"))
        self.text_validation_status_var.set("正在分析文本...")
        self._log(f"开始文本验证，预期立场: {expected_stance}")

        def worker():
            try:
                result = analyze_text(
                    text=text,
                    output_dir=output_dir,
                    enable_fuzzy=self.text_fuzzy_var.get(),
                    enable_ai=self.text_ai_var.get(),
                    expected_stance=expected_stance,
                    enable_correction_suggestion=self.text_correction_var.get(),
                )
                self.root.after(0, lambda: self._text_validation_finished(result))
            except Exception as e:
                logger.error(f"文本验证异常: {e}", exc_info=True)
                self.root.after(0, lambda: self._text_validation_error(str(e)))

        self.text_analysis_thread = threading.Thread(target=worker, daemon=True)
        self.text_analysis_thread.start()

    def _text_validation_finished(self, result: dict):
        self.run_text_button.configure(state="normal", fg_color=Theme.PRIMARY)
        match_text = ""
        if result.get("expected_stance"):
            match_text = "符合" if result.get("is_expected_match") else "不符合"
            match_text = f" | {match_text}预期: {result.get('expected_stance')}"
        suggestion = result.get("correction_suggestion") or {}
        suggestion_text = f" | 新候选: {suggestion.get('created_count', 0)}" if suggestion else ""
        status = (
            f"立场: {result.get('stance')}{match_text}{suggestion_text}\n"
            f"输出: {result.get('output_dir')}"
        )
        self.text_validation_status_var.set(status)
        self._refresh_candidate_box()
        self._log(status)

    def _text_validation_error(self, error_msg: str):
        self.run_text_button.configure(state="normal", fg_color=Theme.PRIMARY)
        self.text_validation_status_var.set("文本验证失败")
        self._log(f"文本验证失败: {error_msg}")
        messagebox.showerror("文本验证失败", error_msg)

    def _open_text_analysis_dialog(self):
        dialog = ctk.CTkToplevel(self.root)
        dialog.title("上传文本立场分析")
        dialog.geometry("820x720")
        dialog.transient(self.root)
        dialog.grab_set()
        dialog.grid_columnconfigure(0, weight=1)
        dialog.grid_rowconfigure(1, weight=1)

        title = ctk.CTkLabel(
            dialog,
            text="粘贴或上传一段文本，使用当前人工词库进行清洗、模糊匹配和立场分析",
            font=Theme.FONT_SECTION,
            text_color=Theme.get("TEXT_PRIMARY"),
        )
        title.grid(row=0, column=0, sticky="w", padx=18, pady=(16, 8))

        text_box = ctk.CTkTextbox(dialog, height=360, font=Theme.FONT_NORMAL)
        text_box.grid(row=1, column=0, sticky="nsew", padx=18, pady=8)

        options = ctk.CTkFrame(dialog, fg_color="transparent")
        options.grid(row=2, column=0, sticky="we", padx=18, pady=8)
        options.grid_columnconfigure(0, weight=1)

        fuzzy_var = ctk.BooleanVar(value=True)
        fuzzy_switch = ctk.CTkSwitch(
            options,
            text="启用同音/近形模糊匹配",
            variable=fuzzy_var,
            onvalue=True,
            offvalue=False,
            fg_color=Theme.BORDER,
            progress_color=Theme.PRIMARY,
            button_color=Theme.SURFACE,
            text_color=Theme.get("TEXT_PRIMARY"),
            font=Theme.FONT_NORMAL,
        )
        fuzzy_switch.grid(row=0, column=0, sticky="w")

        ai_review_var = ctk.BooleanVar(value=self.ai_enabled_var.get())
        ai_review_switch = ctk.CTkSwitch(
            options,
            text="启用 AI 全文理解加权",
            variable=ai_review_var,
            onvalue=True,
            offvalue=False,
            fg_color=Theme.BORDER,
            progress_color=Theme.PRIMARY,
            button_color=Theme.SURFACE,
            text_color=Theme.get("TEXT_PRIMARY"),
            font=Theme.FONT_NORMAL,
        )
        ai_review_switch.grid(row=1, column=0, sticky="w", pady=(8, 0))

        status_var = ctk.StringVar(value="准备就绪")
        status_label = ctk.CTkLabel(options, textvariable=status_var, text_color=Theme.get("TEXT_SECONDARY"))
        status_label.grid(row=2, column=0, sticky="w", pady=(8, 0))

        actions = ctk.CTkFrame(dialog, fg_color="transparent")
        actions.grid(row=3, column=0, sticky="e", padx=18, pady=(8, 18))

        def load_file():
            path = filedialog.askopenfilename(
                filetypes=[("Text files", "*.txt;*.md;*.csv"), ("All files", "*.*")]
            )
            if not path:
                return
            try:
                content = Path(path).read_text(encoding="utf-8")
            except UnicodeDecodeError:
                content = Path(path).read_text(encoding="gb18030", errors="ignore")
            text_box.delete("1.0", "end")
            text_box.insert("1.0", content)
            status_var.set(f"已加载: {path}")

        def finish(result: dict):
            analyze_btn.configure(state="normal", fg_color=Theme.PRIMARY)
            status_var.set(f"完成: {result.get('output_dir', '')}")
            self._log(
                f"文本分析完成 | 立场: {result.get('stance')} | "
                f"输出: {result.get('output_dir')}"
            )
            acc = result.get("match_accuracy", {})
            messagebox.showinfo(
                "文本分析完成",
                f"综合立场: {result.get('stance')}\n"
                f"命中词条: {acc.get('match_count', 0)}，需复核: {acc.get('needs_review_count', 0)}\n"
                f"输出目录:\n{result.get('output_dir')}",
            )

        def fail(error_msg: str):
            analyze_btn.configure(state="normal", fg_color=Theme.PRIMARY)
            status_var.set("分析失败")
            self._log(f"文本分析失败: {error_msg}")
            messagebox.showerror("文本分析失败", error_msg)

        def analyze_now():
            text = text_box.get("1.0", "end").strip()
            if not text:
                messagebox.showwarning("警告", "请先粘贴文本或上传文本文件")
                return
            analyze_btn.configure(state="disabled", fg_color=Theme.get("DISABLED_BG"))
            status_var.set("正在分析...")
            output_dir = create_text_analysis_output_dir(Path("output"))

            def worker():
                try:
                    result = analyze_text(
                        text=text,
                        output_dir=output_dir,
                        enable_fuzzy=fuzzy_var.get(),
                        enable_ai=ai_review_var.get(),
                    )
                    self.root.after(0, lambda: finish(result))
                except Exception as e:
                    logger.error(f"文本分析异常: {e}", exc_info=True)
                    self.root.after(0, lambda: fail(str(e)))

            self.text_analysis_thread = threading.Thread(target=worker, daemon=True)
            self.text_analysis_thread.start()

        load_btn = ctk.CTkButton(actions, text="上传文本", width=100, fg_color=Theme.ACCENT, command=load_file)
        load_btn.pack(side="left", padx=6)
        cancel_btn = ctk.CTkButton(actions, text="关闭", width=90, command=dialog.destroy)
        cancel_btn.pack(side="left", padx=6)
        analyze_btn = ctk.CTkButton(
            actions,
            text="开始分析",
            width=110,
            fg_color=Theme.PRIMARY,
            hover_color="#f25f88",
            command=analyze_now,
        )
        analyze_btn.pack(side="left", padx=6)

    def _start_crawling(self):
        video_input = self.video_entry.get().strip()
        if not video_input:
            messagebox.showwarning("警告", "请输入视频链接/BV号、动态链接或文章链接")
            return

        self.is_crawling = True
        self.start_button.configure(state="disabled", fg_color="#ccc")
        self.start_research_button.configure(state="disabled", fg_color=Theme.get("DISABLED_BG"))
        self.start_summary_button.configure(state="disabled", fg_color=Theme.get("DISABLED_BG"))
        if hasattr(self, "start_backfill_button"):
            self.start_backfill_button.configure(state="disabled", fg_color=Theme.get("DISABLED_BG"))
        self.stop_button.configure(
            state="normal",
            fg_color=Theme.DANGER,
            hover_color=Theme.DANGER_HOVER,
            text_color="white",
        )
        self.export_button.configure(
            state="disabled",
            fg_color=Theme.get("DISABLED_BG"),
            text_color=Theme.get("DISABLED_FG"),
        )

        # 进度条启动动画
        self.progress_bar.configure(mode="indeterminate")
        self.progress_bar.start()

        self.log_console.clear()
        self.comments = []

        for card in self.stat_cards.values():
            card.update_value("0")

        include_replies = self.include_replies_var.get()
        try:
            max_pages = int(self.max_pages_var.get())
            max_pages = max(1, min(1000, max_pages))
        except ValueError:
            max_pages = 100
        mode = int(self.sort_mode_var.get())

        def crawl_thread():
            try:
                crawler = CommentCrawler(progress_callback=self._thread_safe_log)
                self.crawler = crawler
                comments = crawler.crawl_comments(
                    video_input,
                    include_replies=include_replies,
                    max_pages=max_pages,
                    mode=mode,
                )
                processor = DataProcessor()
                cleaned = processor.clean_comments(comments)
                self.comments = cleaned
                stats = processor.get_statistics(self.comments)
                self.root.after(0, lambda: self._crawl_finished(stats))
            except Exception as e:
                logger.error(f"爬取过程异常: {e}", exc_info=True)
                self.root.after(0, lambda: self._crawl_error(str(e)))

        self.crawler_thread = threading.Thread(target=crawl_thread, daemon=True)
        self.crawler_thread.start()

    def _start_research(self):
        keyword = self.song_entry.get().strip()
        if not keyword:
            messagebox.showwarning("警告", "请输入歌曲名或研究关键词")
            return

        try:
            video_limit = max(1, min(DEFAULT_SEARCH_VIDEO_LIMIT, int(self.video_limit_var.get())))
            comment_limit = max(1, min(DEFAULT_COMMENTS_PER_VIDEO, int(self.comment_limit_var.get())))
        except ValueError:
            messagebox.showwarning("警告", "视频上限和评论上限必须是数字")
            return

        output_dir = self.output_dir_var.get().strip() or DEFAULT_OUTPUT_DIR
        enable_ai = self.ai_enabled_var.get()

        self.is_crawling = True
        self.start_button.configure(state="disabled", fg_color=Theme.get("DISABLED_BG"))
        self.start_research_button.configure(state="disabled", fg_color=Theme.get("DISABLED_BG"))
        self.start_summary_button.configure(state="disabled", fg_color=Theme.get("DISABLED_BG"))
        if hasattr(self, "start_backfill_button"):
            self.start_backfill_button.configure(state="disabled", fg_color=Theme.get("DISABLED_BG"))
        self.stop_button.configure(
            state="normal",
            fg_color=Theme.DANGER,
            hover_color=Theme.DANGER_HOVER,
            text_color="white",
        )
        self.export_button.configure(
            state="disabled",
            fg_color=Theme.get("DISABLED_BG"),
            text_color=Theme.get("DISABLED_FG"),
        )

        self.progress_bar.configure(mode="indeterminate")
        self.progress_bar.start()
        self.log_console.clear()
        self.comments = []
        for card in self.stat_cards.values():
            card.update_value("0")

        def research_thread():
            try:
                pipeline = ResearchPipeline(
                    progress_callback=self._thread_safe_log,
                    output_base_dir=output_dir,
                )
                self.research_pipeline = pipeline
                result = pipeline.run(
                    keyword=keyword,
                    video_limit=video_limit,
                    comments_per_video=comment_limit,
                    enable_ai=enable_ai,
                )
                self.root.after(0, lambda: self._research_finished(result))
            except Exception as e:
                logger.error(f"研究任务异常: {e}", exc_info=True)
                self.root.after(0, lambda: self._crawl_error(str(e)))

        self.research_thread = threading.Thread(target=research_thread, daemon=True)
        self.research_thread.start()

    def _start_deep_summary(self):
        result_root = Path(self.summary_result_root_var.get().strip() or "result")
        if not result_root.exists():
            messagebox.showwarning("警告", "未找到 result 目录，无法进行汇总分析")
            return

        enable_ai = self.summary_ai_var.get()
        output_dir = create_summary_output_dir(result_root)

        self.is_crawling = True
        self.start_button.configure(state="disabled", fg_color=Theme.get("DISABLED_BG"))
        self.start_research_button.configure(state="disabled", fg_color=Theme.get("DISABLED_BG"))
        self.start_summary_button.configure(state="disabled", fg_color=Theme.get("DISABLED_BG"))
        if hasattr(self, "start_backfill_button"):
            self.start_backfill_button.configure(state="disabled", fg_color=Theme.get("DISABLED_BG"))
        self.stop_button.configure(
            state="disabled",
            fg_color=Theme.get("DISABLED_BG"),
            hover_color=Theme.get("DISABLED_BG"),
            text_color=Theme.get("DISABLED_FG"),
        )
        self.export_button.configure(
            state="disabled",
            fg_color=Theme.get("DISABLED_BG"),
            text_color=Theme.get("DISABLED_FG"),
        )

        self.progress_bar.configure(mode="indeterminate")
        self.progress_bar.start()
        self.log_console.clear()
        self.progress_var.set("正在汇总 result 目录中的全部歌曲...")
        self.summary_status_var.set("正在生成汇总报告...")
        self._log(f"开始汇总分析: {result_root}")
        self._log(f"输出目录: {output_dir}")

        def summary_thread():
            try:
                result = run_deep_cleaning_summary(
                    result_root=result_root,
                    output_dir=output_dir,
                    enable_ai=enable_ai,
                )
                self.root.after(0, lambda: self._summary_finished(result))
            except Exception as e:
                logger.error(f"汇总分析异常: {e}", exc_info=True)
                self.root.after(0, lambda: self._crawl_error(str(e)))

        self.summary_thread = threading.Thread(target=summary_thread, daemon=True)
        self.summary_thread.start()

    def _start_backfill_summary(self):
        result_root = Path(self.summary_result_root_var.get().strip() or "result")
        if not result_root.exists():
            messagebox.showwarning("警告", "未找到 result 目录，无法进行回填与汇总")
            return

        enable_ai = self.summary_ai_var.get()
        output_dir = create_summary_output_dir(result_root)

        self.is_crawling = True
        self.start_button.configure(state="disabled", fg_color=Theme.get("DISABLED_BG"))
        self.start_research_button.configure(state="disabled", fg_color=Theme.get("DISABLED_BG"))
        self.start_summary_button.configure(state="disabled", fg_color=Theme.get("DISABLED_BG"))
        self.start_backfill_button.configure(state="disabled", fg_color=Theme.get("DISABLED_BG"))
        self.stop_button.configure(
            state="disabled",
            fg_color=Theme.get("DISABLED_BG"),
            hover_color=Theme.get("DISABLED_BG"),
            text_color=Theme.get("DISABLED_FG"),
        )
        self.export_button.configure(
            state="disabled",
            fg_color=Theme.get("DISABLED_BG"),
            text_color=Theme.get("DISABLED_FG"),
        )

        self.progress_bar.configure(mode="indeterminate")
        self.progress_bar.start()
        self.log_console.clear()
        self.progress_var.set("正在回填每首歌的新版清洗结果，并生成新的 summary...")
        self.summary_status_var.set("正在回填单曲并生成汇总...")
        self._log(f"开始回填单曲并汇总: {result_root}")
        self._log(f"summary 输出目录: {output_dir}")

        def backfill_thread():
            try:
                backfill_result = run_deep_cleaning_backfill(
                    result_root=result_root,
                    enable_ai=enable_ai,
                )
                for song in backfill_result.get("songs", []):
                    self._thread_safe_log(
                        f"已回填: {Path(song.get('song_dir', '')).name} -> {song.get('output_dir', '')}"
                    )
                summary_result = run_deep_cleaning_summary(
                    result_root=result_root,
                    output_dir=output_dir,
                    enable_ai=enable_ai,
                )
                summary_result["backfill_song_count"] = backfill_result.get("song_count", 0)
                self.root.after(0, lambda: self._summary_finished(summary_result))
            except Exception as e:
                logger.error(f"回填与汇总异常: {e}", exc_info=True)
                self.root.after(0, lambda: self._crawl_error(str(e)))

        self.backfill_thread = threading.Thread(target=backfill_thread, daemon=True)
        self.backfill_thread.start()

    def _crawl_finished(self, stats: dict):
        self.is_crawling = False
        self.start_button.configure(state="normal", fg_color=Theme.PRIMARY)
        self.start_research_button.configure(state="normal", fg_color=Theme.PRIMARY)
        self.start_summary_button.configure(state="normal", fg_color=Theme.ACCENT)
        if hasattr(self, "start_backfill_button"):
            self.start_backfill_button.configure(state="normal", fg_color=Theme.PRIMARY)
        self.stop_button.configure(
            state="disabled",
            fg_color=Theme.get("DISABLED_BG"),
            hover_color=Theme.get("DISABLED_BG"),
            text_color=Theme.get("DISABLED_FG"),
        )
        self.export_button.configure(
            state="normal",
            fg_color=Theme.ACCENT,
            hover_color="#1f9bcb",
            text_color="white",
        )

        # 进度条停止并显示完成
        self.progress_bar.stop()
        self.progress_bar.configure(mode="determinate")
        self.progress_bar.set(1.0)
        self.progress_var.set("✅ 爬取完成")

        self.stat_cards["total"].update_value(str(stats["total"]))
        self.stat_cards["main"].update_value(str(stats["main_comments"]))
        self.stat_cards["replies"].update_value(str(stats["replies"]))
        self.stat_cards["likes"].update_value(str(stats["total_likes"]))

        if self.comments:
            messagebox.showinfo("完成", f"成功爬取 {len(self.comments)} 条评论！")
        else:
            messagebox.showwarning("警告", "未获取到任何评论数据")

    def _research_finished(self, result: dict):
        self.is_crawling = False
        self.start_button.configure(state="normal", fg_color=Theme.PRIMARY)
        self.start_research_button.configure(state="normal", fg_color=Theme.PRIMARY)
        self.start_summary_button.configure(state="normal", fg_color=Theme.ACCENT)
        if hasattr(self, "start_backfill_button"):
            self.start_backfill_button.configure(state="normal", fg_color=Theme.PRIMARY)
        self.stop_button.configure(
            state="disabled",
            fg_color=Theme.get("DISABLED_BG"),
            hover_color=Theme.get("DISABLED_BG"),
            text_color=Theme.get("DISABLED_FG"),
        )

        self.progress_bar.stop()
        self.progress_bar.configure(mode="determinate")
        self.progress_bar.set(1.0)
        self.progress_var.set("研究任务完成")

        self.stat_cards["total"].update_value(str(result.get("comment_count", 0)))
        self.stat_cards["main"].update_value(str(result.get("video_count", 0)))
        self.stat_cards["replies"].update_value("0")
        self.stat_cards["likes"].update_value("0")

        output_dir = result.get("output_dir", "")
        self._log(f"研究任务完成，输出目录: {output_dir}")
        messagebox.showinfo(
            "完成",
            f"研究任务完成！\n视频数: {result.get('video_count', 0)}\n"
            f"评论数: {result.get('comment_count', 0)}\n输出目录:\n{output_dir}",
        )

    def _summary_finished(self, result: dict):
        self.is_crawling = False
        self.start_button.configure(state="normal", fg_color=Theme.PRIMARY)
        self.start_research_button.configure(state="normal", fg_color=Theme.PRIMARY)
        self.start_summary_button.configure(state="normal", fg_color=Theme.ACCENT)
        if hasattr(self, "start_backfill_button"):
            self.start_backfill_button.configure(state="normal", fg_color=Theme.PRIMARY)
        self.stop_button.configure(
            state="disabled",
            fg_color=Theme.get("DISABLED_BG"),
            hover_color=Theme.get("DISABLED_BG"),
            text_color=Theme.get("DISABLED_FG"),
        )

        self.progress_bar.stop()
        self.progress_bar.configure(mode="determinate")
        self.progress_bar.set(1.0)
        self.progress_var.set("汇总分析完成")

        self.stat_cards["total"].update_value(str(result.get("comment_count", 0)))
        self.stat_cards["main"].update_value(str(result.get("song_count", 0)))
        self.stat_cards["replies"].update_value(str(result.get("political_comment_count", 0)))
        self.stat_cards["likes"].update_value(str(result.get("clean_term_count", 0)))

        output_dir = result.get("output_dir", "")
        if hasattr(self, "summary_status_var"):
            self.summary_status_var.set(
                f"完成: {output_dir}\n"
                f"歌曲 {result.get('song_count', 0)} | 评论 {result.get('comment_count', 0)} | "
                f"政治历史 {result.get('political_comment_count', 0)}"
            )
        self._log(f"汇总分析完成，输出目录: {output_dir}")
        messagebox.showinfo(
            "完成",
            f"汇总分析完成\n歌曲数: {result.get('song_count', 0)}\n"
            f"评论数: {result.get('comment_count', 0)}\n输出目录:\n{output_dir}",
        )

    def _crawl_error(self, error_msg: str):
        self.is_crawling = False
        self.start_button.configure(state="normal", fg_color=Theme.PRIMARY)
        self.start_research_button.configure(state="normal", fg_color=Theme.PRIMARY)
        self.start_summary_button.configure(state="normal", fg_color=Theme.ACCENT)
        if hasattr(self, "start_backfill_button"):
            self.start_backfill_button.configure(state="normal", fg_color=Theme.PRIMARY)
        self.stop_button.configure(
            state="disabled",
            fg_color=Theme.get("DISABLED_BG"),
            hover_color=Theme.get("DISABLED_BG"),
            text_color=Theme.get("DISABLED_FG"),
        )

        # 进度条停止并重置
        self.progress_bar.stop()
        self.progress_bar.configure(mode="determinate")
        self.progress_bar.set(0)
        self.progress_var.set("❌ 爬取失败")

        self._log(f"错误: {error_msg}")
        messagebox.showerror("错误", f"爬取过程中出现错误:\n{error_msg}")

    def _stop_crawling(self):
        if self.crawler:
            self.crawler.stop()
        if self.research_pipeline:
            self.research_pipeline.stop()
        self._update_progress("正在停止...")

    def _export_csv(self):
        if not self.comments:
            messagebox.showwarning("警告", "没有可导出的数据")
            return

        path_val = self.export_path_var.get().strip()
        if not path_val:
            messagebox.showwarning("警告", "请指定导出文件路径")
            return

        try:
            success = CSVExporter.export(self.comments, path_val)
            if success:
                messagebox.showinfo("成功", f"数据已导出到:\n{path_val}")
            else:
                messagebox.showerror("失败", "导出失败，请查看日志")
        except Exception as e:
            messagebox.showerror("错误", f"导出时出错:\n{str(e)}")

    def _on_closing(self):
        if self.is_crawling:
            if messagebox.askokcancel("退出", "正在爬取中，确定要退出吗？"):
                if self.crawler:
                    self.crawler.stop()
                if self.research_pipeline:
                    self.research_pipeline.stop()
                self.root.destroy()
        else:
            self.root.destroy()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    root = ctk.CTk()
    app = MainWindow(root)
    root.mainloop()


if __name__ == "__main__":
    main()
