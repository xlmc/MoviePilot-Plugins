# -*- coding: utf-8 -*-
"""洗版管家（MoviePilot V2）

功能：
    把「按播放进度停止洗版」「每集最多洗两次」「看完即删」三件事合成一个插件，
    三个功能各自独立开关，共用同一份媒体服务器读取与订阅写库逻辑。

    一、进度洗版（progress_enabled，默认开）
        按媒体服务器（Emby / Jellyfin）上的播放进度决定一个订阅是否还要继续洗版：
          - 电视剧：某个目标集已经看过，就把该集的洗版优先级写成 100
            （宿主定义的「该集洗版完成」口径，app/chain/subscribe.py），
            后续洗版搜索不会再为这一集下载更高优先级的版本；
          - 电影：已经看过就把 current_priority 写成 100，整部停止洗版；
          - 全集洗版（best_version_full）：宿主按整部判断完成，因此只有目标集全部看过才写 100。

    二、两次上限（twice_enabled，默认开）
        在宿主的资源选择环节收窄候选，让每集最多下载两次：
          - 阶段 0（还没有任何版本）：只放行本批优先级最低的一档；
          - 阶段 1（已有首发版本）：只放行本批优先级最高的一档；
          - 阶段 2（已升级完成）：全部拦掉，并把该集写成 episode_priority = 100。
        阶段不靠「选择时就记账」，而是每次用宿主自己维护的下载事实对账
        （app/chain/subscribe.py:__record_subscribe_download_facts），
        所以下载失败会自然退回上一阶段重试，不会被永久卡死。

    三、看完即删（clean_enabled，默认关）
        按播放进度清理已经看完的内容：
          - 标记：把订阅的 start_episode 前移到「连续看完的最后一集 + 1」，
            这是宿主的缺失集窗口口径（app/chain/subscribe.py），
            落在窗口外的已删集不再被判定为缺失，因此不会被重新下载；
          - 清理：按媒体服务器条目的 Path 删除媒体库文件、刮削文件（元数据/图片/字幕）、
            空目录、媒体服务器条目与整理记录，可选删除下载目录源文件（会破坏做种，默认关闭）；
          - 只处理「连续看完」的一段，跳集观看不会把中间没看的集一起放弃。

三个功能的协作：
    - 都从订阅库取「订阅中（state=R）」的订阅，共用一次媒体服务器读取，不会重复请求；
    - 进度洗版先把命中的集写成 100，「看完即删」随后前移开始集数并清理文件，
      两处写入都走同一个 _update_subscribe，写库范围只有
      episode_priority / current_priority / start_episode 三个字段；
    - 「两次上限」在资源选择事件里读到的 100 会直接当成阶段 2（已完成），
      所以进度洗版或看完即删标记过的集不会被两次上限重新放行升级。

设计说明：
    - 判定口径全部可配置：percentage（默认，进度 >= 阈值）/ any_play（进度 > 0）/ played（已看标记）；
    - 支持「试运行」（默认开启）：只写日志，不写库、不删文件、不改候选；
    - 只读媒体服务器、只写订阅表，删除动作全部可单独关闭；
    - 不注册宿主的 get_service，自己在 init_plugin 里起 BackgroundScheduler，
      保存配置后立刻按新配置重新排程，不必重启宿主；
    - 模块导入期不启动任务、不连网、不连库。
"""
import datetime
import json
import shutil
import threading
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote

import pytz
from apscheduler.schedulers.background import BackgroundScheduler

from app.core.config import settings
from app.core.event import Event, eventmanager
from app.db.subscribe_oper import SubscribeOper
from app.db.transferhistory_oper import TransferHistoryOper
from app.helper.mediaserver import MediaServerHelper
from app.helper.service import ServiceConfigHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import ChainEventType, MediaType, MessageChannel
from app.utils.http import RequestUtils

# 宿主定义的「洗版完成」优先级：某集/整部达到该值即不再搜索更高优先级的版本
BEST_VERSION_DONE_PRIORITY = 100
# 宿主对「未设置优先级」的默认取值
UNSET_PRIORITY = 0
# 订阅下载的来源前缀（app/chain/subscribe.py:get_subscribe_source_keyword）
SUBSCRIBE_SOURCE_PREFIX = "Subscribe|"
# 能读取播放进度、并且能删除条目的媒体服务器类型
SUPPORTED_SERVER_TYPES = ("emby", "jellyfin")
# 判定方式：进度百分比 / 有过播放 / 已看标记
MODE_PERCENTAGE = "percentage"
MODE_ANY_PLAY = "any_play"
MODE_PLAYED = "played"
# 每集阶段
STAGE_NEW = 0
STAGE_LOW = 1
STAGE_DONE = 2
# 「不做档位限制」的哨兵值
ALL_PRIORITIES = "all"
# 阶段说明（详情页展示）
STAGE_TEXT = {
    STAGE_NEW: "待首发（只下最低档）",
    STAGE_LOW: "待升级（只下最高档）",
    STAGE_DONE: "已完成（不再洗版）",
}
# 向上清理空目录的最大层数，避免把媒体库根目录一起删掉
MAX_UP_LEVELS = 3
# 路径最少段数：低于该值视为可疑路径，拒绝删除
MIN_PATH_PARTS = 3


class WashManager(_PluginBase):
    """洗版管家插件。

    进度洗版 + 两次上限 + 看完即删，三个功能各自独立开关。
    """

    # 插件名称
    plugin_name = "洗版管家"
    # 插件描述
    plugin_desc = "洗版三合一：按播放进度停止洗版、每集最多洗两次、看完自动清理，三个开关各自独立。"
    # 插件图标
    plugin_icon = "washmanager.svg"
    # 插件版本
    plugin_version = "2.0.1"
    # 插件作者
    plugin_author = "xlmc"
    # 作者主页
    author_url = "https://github.com/xlmc"
    # 插件配置项 ID 前缀
    plugin_config_prefix = "washmanager_"
    # 加载顺序
    plugin_order = 31
    # 可使用的用户级别
    auth_level = 1

    # 日志前缀
    LOG_TAG = "[WashManager] "

    # 通知渠道类型（宿主通知配置的 type）-> 通知渠道枚举
    NOTIFY_TYPE_MAP = {
        "telegram": MessageChannel.Telegram,
        "wechat": MessageChannel.Wechat,
        "feishu": MessageChannel.Feishu,
        "wechatclawbot": MessageChannel.WechatClawBot,
        "slack": MessageChannel.Slack,
        "discord": MessageChannel.Discord,
        "dingtalk": MessageChannel.DingTalk,
        "synologychat": MessageChannel.SynologyChat,
        "vocechat": MessageChannel.VoceChat,
        "web": MessageChannel.Web,
        "webpush": MessageChannel.WebPush,
        "apppush": MessageChannel.AppPush,
        "qqbot": MessageChannel.QQ,
    }

    # 内置刮削文件后缀（元数据 / 图片 / 字幕）
    SCRAP_EXTENSIONS = (
        ".nfo", ".xml",
        ".jpg", ".jpeg", ".png", ".webp", ".tbn", ".fanart", ".gif", ".bmp",
        ".srt", ".ass", ".ssa", ".sub", ".idx", ".vtt", ".sup", ".pgs",
        ".smi", ".rt", ".sbv",
    )
    # 刮削/媒体服务器生成的关联目录后缀
    SCRAP_DIR_SUFFIXES = (".trickplay",)

    def __init__(self):
        """初始化运行时状态，配置项由 init_plugin 注入。"""
        super().__init__()
        # ---- 通用 ----
        # 是否启用
        self._enabled = False
        # 试运行：只记录，不写库、不删文件、不改候选
        self._dry_run = True
        # 保存配置后是否立即运行一次（跑完自动复位）
        self._onlyonce = False
        # 是否发送通知
        self._notify = False
        # 通知渠道（渠道类型，如 telegram / wechat）
        self._notify_channel: List[str] = []
        # 检测间隔（分钟）
        self._interval = 30
        # 参与检测的媒体服务器名称，空表示全部支持的启用服务器
        self._servers: List[str] = []
        # 只统计这些用户名的播放进度，空表示使用服务器配置的用户
        self._usernames = ""
        # 判定方式
        self._mode = MODE_PERCENTAGE
        # 进度阈值（百分比）
        self._threshold = 90
        # 是否处理电视剧
        self._handle_tv = True
        # 是否处理电影
        self._handle_movie = True
        # ---- 进度洗版 ----
        # 是否按播放进度停止洗版
        self._progress_enabled = True
        # ---- 两次上限 ----
        # 是否限制每集最多洗两次
        self._twice_enabled = True
        # 首发是否只下最低档
        self._twice_lowest_first = True
        # 升级完成后是否收尾（写 100，彻底停止该集洗版）
        self._twice_cap_after_upgrade = True
        # 每集阶段：订阅 ID -> 集 key -> 状态
        self._twice_state: Dict[str, Dict[str, Dict[str, Any]]] = {}
        # 订阅标题缓存：订阅 ID -> 标题（详情页展示用）
        self._twice_labels: Dict[str, str] = {}
        # 两次上限最近一次处理摘要
        self._twice_last_run: Dict[str, Any] = {}
        # ---- 看完即删 ----
        # 是否清理已看完的内容
        self._clean_enabled = False
        # 是否标记订阅（前移开始集数 / 写洗版优先级），关闭后会因缺失重新下载
        self._clean_mark_subscribe = True
        # 是否删除媒体库文件
        self._clean_delete_media_files = True
        # 是否删除刮削文件
        self._clean_delete_scrap_infos = True
        # 是否删除空目录
        self._clean_delete_empty_folders = True
        # 是否删除媒体服务器条目
        self._clean_delete_mediaserver = True
        # 是否删除整理记录
        self._clean_delete_history = True
        # 是否删除下载目录源文件（会破坏做种）
        self._clean_delete_source_files = False
        # 延迟清理天数：最后播放时间距今不足该天数时跳过
        self._clean_delay_days = 0
        # ---- 运行时 ----
        # 定时调度器
        self._scheduler: Optional[BackgroundScheduler] = None
        # 互斥锁：避免定时与手动触发同时执行
        self._lock = threading.Lock()
        # 最近一次定时/手动执行结果（详情页与通知用）
        self._last_run: Dict[str, Any] = {}

    def init_plugin(self, config: dict = None):
        """初始化插件：读取配置与上次状态、按需立即运行一次，并启动定时调度。

        :param config: 插件配置字典，来自宿主的插件配置存储
        """
        # 先停掉旧调度器，避免保存配置后出现两份定时任务
        self.stop_service()

        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._dry_run = bool(config.get("dry_run", True))
        self._onlyonce = bool(config.get("onlyonce"))
        self._notify = bool(config.get("notify"))
        self._notify_channel = self._as_list(config.get("notify_channel"))
        self._interval = max(1, self._as_int(config.get("interval_minutes"), 30))
        self._servers = self._as_list(config.get("servers"))
        self._usernames = (config.get("usernames") or "").strip() \
            if isinstance(config.get("usernames"), str) else ""
        self._mode = config.get("mode") or MODE_PERCENTAGE
        self._threshold = min(100, max(1, self._as_int(config.get("threshold"), 90)))
        self._handle_tv = bool(config.get("handle_tv", True))
        self._handle_movie = bool(config.get("handle_movie", True))

        self._progress_enabled = bool(config.get("progress_enabled", True))

        self._twice_enabled = bool(config.get("twice_enabled", True))
        self._twice_lowest_first = bool(config.get("twice_lowest_first", True))
        self._twice_cap_after_upgrade = bool(config.get("twice_cap_after_upgrade", True))

        self._clean_enabled = bool(config.get("clean_enabled"))
        self._clean_mark_subscribe = bool(config.get("clean_mark_subscribe", True))
        self._clean_delete_media_files = bool(config.get("clean_delete_media_files", True))
        self._clean_delete_scrap_infos = bool(config.get("clean_delete_scrap_infos", True))
        self._clean_delete_empty_folders = bool(config.get("clean_delete_empty_folders", True))
        self._clean_delete_mediaserver = bool(config.get("clean_delete_mediaserver", True))
        self._clean_delete_history = bool(config.get("clean_delete_history", True))
        self._clean_delete_source_files = bool(config.get("clean_delete_source_files", False))
        self._clean_delay_days = max(0, self._as_int(config.get("clean_delay_days"), 0))

        # 恢复上次的执行结果与两次上限阶段，重启后不用重新数一遍
        self._last_run = self.get_data("last_run") or {}
        saved = self.get_data("twice_state")
        self._twice_state = saved if isinstance(saved, dict) else {}
        labels = self.get_data("twice_labels")
        self._twice_labels = labels if isinstance(labels, dict) else {}
        self._twice_last_run = self.get_data("twice_last_run") or {}

        if not self._enabled:
            logger.info(f"{self.LOG_TAG}插件未启用")
        else:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            if self._onlyonce:
                run_date = datetime.datetime.now(tz=pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=3)
                self._scheduler.add_job(
                    self._run_once,
                    "date",
                    run_date=run_date,
                    name="洗版管家-立即运行一次",
                )
                logger.info(f"{self.LOG_TAG}已注册「立即运行一次」任务：{run_date}")
            self._scheduler.add_job(
                self._run_once,
                "interval",
                minutes=self._interval,
                name="洗版管家-定时检测",
            )
            self._scheduler.start()
            logger.info(
                f"{self.LOG_TAG}定时检测已启动，间隔 {self._interval} 分钟"
                f"{'（试运行）' if self._dry_run else ''}，"
                f"进度洗版{'开' if self._progress_enabled else '关'}、"
                f"两次上限{'开' if self._twice_enabled else '关'}、"
                f"看完即删{'开' if self._clean_enabled else '关'}"
            )

        # 把规范化后的配置写回，避免表单里残留空值/非法值
        if self._current_config() != config:
            self.update_config(self._current_config())

    def get_state(self) -> bool:
        """返回插件当前是否启用。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """没有远程命令，返回空列表。"""
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """提供执行与状态维护的插件接口。"""
        return [
            {
                "path": "/run",
                "endpoint": self.api_run,
                "methods": ["GET"],
                "summary": "立即执行一次检查",
                "description": "按当前配置扫描一遍订阅与播放进度，返回本轮执行摘要",
            },
            {
                "path": "/status",
                "endpoint": self.api_status,
                "methods": ["GET"],
                "summary": "查看每集洗版阶段",
                "description": "返回两次上限记录过的订阅与每集的当前阶段",
            },
            {
                "path": "/reset",
                "endpoint": self.api_reset,
                "methods": ["GET"],
                "summary": "清空洗版阶段记录",
                "description": "只清空两次上限的阶段记录，不改订阅数据；下次搜索会重新从「首发最低档」开始",
            },
            {
                "path": "/restore",
                "endpoint": self.api_restore,
                "methods": ["GET"],
                "summary": "还原被写成的洗版完成标记",
                "description": "把插件写过的 episode_priority / current_priority=100 还原成写入前的值",
            },
        ]

    def get_page(self) -> Optional[List[dict]]:
        """插件详情页：执行摘要、逐订阅结果、每集洗版阶段。"""
        last = self._last_run or {}
        records = last.get("records") or []

        page: List[dict] = [
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "info" if last.get("success") else "warning",
                                    "variant": "tonal",
                                    "title": "洗版管家",
                                    "text": self._page_summary(last),
                                },
                            }
                        ],
                    }
                ],
            }
        ]

        if records:
            page.append(
                {
                    "component": "VRow",
                    "content": [
                        {
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [
                                {
                                    "component": "VDataTable",
                                    "props": {
                                        "headers": [
                                            {"title": "订阅", "key": "title"},
                                            {"title": "类型", "key": "type"},
                                            {"title": "服务器", "key": "server"},
                                            {"title": "播放进度", "key": "watched"},
                                            {"title": "跳过洗版", "key": "changed"},
                                            {"title": "清理", "key": "deleted"},
                                            {"title": "结果", "key": "status"},
                                        ],
                                        "items": [
                                            {
                                                "title": record.get("title") or "",
                                                "type": record.get("type") or "",
                                                "server": record.get("server") or "",
                                                "watched": record.get("watched") or "",
                                                "changed": self._describe_changed(record.get("changed")),
                                                "deleted": record.get("deleted") or "",
                                                "status": record.get("status") or "",
                                            }
                                            for record in records
                                        ],
                                        "density": "compact",
                                        "hover": True,
                                    },
                                }
                            ],
                        }
                    ],
                }
            )

        rows: List[dict] = []
        for subscribe_id, episodes in (self._twice_state or {}).items():
            if not isinstance(episodes, dict):
                continue
            for key, record in episodes.items():
                if not isinstance(record, dict):
                    continue
                stage = self._as_int(record.get("stage"), STAGE_NEW)
                rows.append(
                    {
                        "title": self._twice_labels.get(subscribe_id) or subscribe_id,
                        "episode": key,
                        "stage": STAGE_TEXT.get(stage, str(stage)),
                        "first": record.get("first") or "",
                        "target": record.get("target") or "",
                    }
                )
        rows.sort(key=lambda item: (item["title"], item["episode"]))

        if rows:
            page.append(
                {
                    "component": "VRow",
                    "content": [
                        {
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [
                                {
                                    "component": "VDataTable",
                                    "props": {
                                        "headers": [
                                            {"title": "订阅", "key": "title"},
                                            {"title": "集", "key": "episode"},
                                            {"title": "阶段", "key": "stage"},
                                            {"title": "首发档", "key": "first"},
                                            {"title": "升级档", "key": "target"},
                                        ],
                                        "items": rows,
                                        "density": "compact",
                                        "hover": True,
                                    },
                                }
                            ],
                        }
                    ],
                }
            )

        page.append(
            {
                "component": "VAlert",
                "props": {
                    "type": "warning",
                    "variant": "tonal",
                    "text": "说明：进度洗版会把命中的集写成订阅的分集洗版优先级 100（宿主口径：该集洗版完成）；"
                            "看完即删只清理「从开始集数起连续看完」的内容，清理前先前移开始集数避免被重新下载；"
                            "两次上限只在订阅搜索的资源选择阶段收窄候选。"
                            "「试运行」下只记日志，不写库、不删文件、不改候选。",
                },
            }
        )
        return page

    def stop_service(self):
        """停用插件时关闭定时调度器。"""
        if self._scheduler:
            try:
                self._scheduler.shutdown(wait=False)
            except Exception as err:
                logger.error(f"{self.LOG_TAG}停止调度器失败：{err}")
        self._scheduler = None

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """插件配置页面（Vuetify JSON 配置），返回 (表单, 默认值)。"""
        # 媒体服务器下拉：仅列出 Emby / Jellyfin
        server_items: List[dict] = []
        try:
            for name, service in (MediaServerHelper().get_services() or {}).items():
                server_type = (getattr(service, "type", "") or "").lower()
                if server_type in SUPPORTED_SERVER_TYPES and name:
                    server_items.append({"title": f"{name}（{server_type}）", "value": name})
        except Exception as err:
            logger.warning(f"{self.LOG_TAG}读取媒体服务器配置失败：{err}")

        # 通知渠道下拉：已启用渠道的类型去重（如 telegram / wechat）
        notify_items: List[dict] = []
        try:
            seen_types = set()
            for conf in (ServiceConfigHelper.get_notification_configs() or []):
                if not getattr(conf, "enabled", False):
                    continue
                conf_type = getattr(conf, "type", "") or ""
                conf_name = getattr(conf, "name", "") or conf_type
                if conf_type and conf_type not in seen_types:
                    seen_types.add(conf_type)
                    notify_items.append({"title": conf_name, "value": conf_type})
        except Exception as err:
            logger.warning(f"{self.LOG_TAG}读取通知渠道配置失败：{err}")

        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "title": "🧼 洗版管家",
                                            "text": "三个功能各自独立开关：进度洗版（已看过的不再洗版）、"
                                                    "两次上限（每集最多洗两次）、看完即删（清理已看完的内容）。"
                                                    "只读媒体服务器，写库只动订阅的分集优先级与开始集数，"
                                                    "删除动作全部可单独关闭。",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "dry_run", "label": "试运行（只记日志）"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "notify", "label": "发送通知"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "onlyonce", "label": "立即运行一次"}}
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "servers",
                                            "label": "媒体服务器",
                                            "placeholder": "留空表示全部已启用的 Emby / Jellyfin",
                                            "items": server_items,
                                            "multiple": True,
                                            "chips": True,
                                            "clearable": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "usernames",
                                            "label": "统计哪些用户",
                                            "placeholder": "留空表示服务器配置的用户，多个用英文逗号分隔",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "mode",
                                            "label": "判定方式",
                                            "items": [
                                                {"title": "播放进度达到阈值", "value": MODE_PERCENTAGE},
                                                {"title": "只要播放过", "value": MODE_ANY_PLAY},
                                                {"title": "标记为已看", "value": MODE_PLAYED},
                                            ],
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "threshold",
                                            "label": "进度阈值（%）",
                                            "placeholder": "仅「播放进度达到阈值」时生效，默认 90",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "interval_minutes",
                                            "label": "检测间隔（分钟）",
                                            "placeholder": "默认 30",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "handle_tv", "label": "处理电视剧"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "handle_movie", "label": "处理电影"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "notify_channel",
                                            "label": "通知渠道",
                                            "placeholder": "留空表示不发送通知",
                                            "items": notify_items,
                                            "multiple": True,
                                            "chips": True,
                                            "clearable": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "primary",
                                            "variant": "tonal",
                                            "density": "compact",
                                            "title": "① 进度洗版",
                                            "text": "已看过的剧集/电影不再继续洗版。只对开启了洗版的订阅生效，"
                                                    "写库只动 episode_priority / current_priority。",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "progress_enabled", "label": "启用进度洗版"},
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "primary",
                                            "variant": "tonal",
                                            "density": "compact",
                                            "title": "② 两次上限",
                                            "text": "每集最多下载两次：首发只下本批最低档，只升级一次到本批最高档，"
                                                    "之后不再洗版。只在订阅搜索的资源选择阶段过滤候选，"
                                                    "不动下载器与搜索逻辑。",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "twice_enabled", "label": "启用两次上限"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "twice_lowest_first", "label": "首发只下最低档"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "twice_cap_after_upgrade", "label": "升级后停止洗版"},
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "warning",
                                            "variant": "tonal",
                                            "density": "compact",
                                            "title": "③ 看完即删",
                                            "text": "会删除文件，默认关闭。清理前先把订阅的开始集数前移到"
                                                    "「连续看完的最后一集 + 1」，避免删掉后被判定为缺失重新下载。",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "clean_enabled", "label": "启用看完即删"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "clean_mark_subscribe", "label": "标记订阅"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "clean_delay_days",
                                            "label": "延迟清理（天）",
                                            "placeholder": "最后播放距今不足该天数时跳过，默认 0",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "clean_delete_media_files", "label": "删除媒体库文件"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "clean_delete_mediaserver", "label": "删除媒体服务器条目"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "clean_delete_scrap_infos", "label": "删除刮削文件"},
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "clean_delete_empty_folders", "label": "删除空目录"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "clean_delete_history", "label": "删除整理记录"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "clean_delete_source_files", "label": "删除下载目录源文件"},
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "error",
                                            "variant": "tonal",
                                            "density": "compact",
                                            "text": "「删除下载目录源文件」会破坏做种（MoviePilot 默认用硬链接整理，"
                                                    "只删媒体库侧不会释放空间）。需要释放空间时再开启，"
                                                    "并确保对应的种子已停止做种。",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], self._current_config()

    def api_run(self) -> Dict[str, Any]:
        """插件 API：立即执行一次检查。"""
        return self.run(force=True)

    # ---- 调度入口 ----

    def _run_once(self):
        """调度入口：执行一次检查，并把「立即运行一次」标记复位。"""
        try:
            self.run()
        except Exception as err:
            logger.error(f"{self.LOG_TAG}执行异常：{err}\n{traceback.format_exc()}")
        finally:
            if self._onlyonce:
                self._onlyonce = False
                try:
                    self.update_config(self._current_config())
                except Exception as err:
                    logger.error(f"{self.LOG_TAG}复位「立即运行一次」失败：{err}")

    # ---- 主流程 ----

    def run(self, force: bool = False) -> Dict[str, Any]:
        """执行一轮检查：进度洗版 + 看完即删。

        :param force: 手动触发时为 True，可绕过「插件未启用」判断（便于先用试运行观察）
        :return: 本轮执行摘要
        """
        if not self._enabled and not force:
            return {"success": False, "message": "插件未启用", "records": []}
        if not (self._progress_enabled or self._clean_enabled):
            return {
                "success": False,
                "message": "「进度洗版」与「看完即删」都已关闭，没有需要执行的检查",
                "records": [],
            }
        if not self._lock.acquire(blocking=False):
            logger.info(f"{self.LOG_TAG}上一轮检查尚未结束，跳过本轮")
            return {"success": False, "message": "上一轮检查尚未结束", "records": []}

        started = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            subscribes = self._get_subscribes()
            servers = self._get_servers()
            if not subscribes:
                summary: Dict[str, Any] = {
                    "success": True,
                    "message": "没有需要检查的订阅",
                    "time": started,
                    "records": [],
                    "changed": 0,
                    "cleaned": 0,
                }
            elif not servers:
                summary = {
                    "success": False,
                    "message": "没有可用的 Emby / Jellyfin 媒体服务器",
                    "time": started,
                    "records": [],
                    "changed": 0,
                    "cleaned": 0,
                }
            else:
                records = []
                for subscribe in subscribes:
                    try:
                        record = self._process(subscribe, servers)
                    except Exception as err:
                        logger.error(
                            f"{self.LOG_TAG}处理订阅 {self._title(subscribe)} 失败："
                            f"{err}\n{traceback.format_exc()}"
                        )
                        continue
                    if record:
                        records.append(record)
                changed = sum(len(record.get("changed") or []) for record in records)
                cleaned = sum(1 for record in records if record.get("deleted"))
                summary = {
                    "success": True,
                    "message": f"检查 {len(records)} 个订阅",
                    "time": started,
                    "records": records,
                    "changed": changed,
                    "cleaned": cleaned,
                }
                logger.info(
                    f"{self.LOG_TAG}{summary['message']}，本轮写入 {changed} 个跳过标记，"
                    f"清理 {cleaned} 个订阅"
                )
                if self._notify and (changed or cleaned):
                    self._send_notify(records)

            self._last_run = summary
            self.save_data("last_run", summary)
            return summary
        except Exception as err:
            logger.error(f"{self.LOG_TAG}检查失败：{err}\n{traceback.format_exc()}")
            return {"success": False, "message": f"检查失败：{err}", "time": started, "records": []}
        finally:
            self._lock.release()

    def _get_subscribes(self) -> List[Any]:
        """取「订阅中」且还有目标集的订阅。"""
        try:
            subscribes = SubscribeOper().list(state="R") or []
        except Exception as err:
            logger.error(f"{self.LOG_TAG}读取订阅失败：{err}")
            return []

        result = []
        for subscribe in subscribes:
            media_type = self._media_type(subscribe)
            if media_type is None:
                continue
            if media_type == MediaType.TV and not self._handle_tv:
                continue
            if media_type == MediaType.MOVIE and not self._handle_movie:
                continue
            if media_type == MediaType.TV:
                # 开始集数已经越过总集数：没有可清理、可标记的目标集
                if not self._target_episodes(subscribe) and self._total_episode(subscribe):
                    continue
            result.append(subscribe)
        return result

    def _process(self, subscribe, servers: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """处理单个订阅：读取一次播放进度，先做进度洗版，再做看完即删。

        :param subscribe: 订阅记录
        :param servers: 可用的媒体服务器列表
        :return: 执行记录（供详情页与通知使用）
        """
        media_type = self._media_type(subscribe)
        record: Dict[str, Any] = {
            "title": self._title(subscribe),
            "type": media_type.value if media_type else "",
            "season": getattr(subscribe, "season", None) or "",
            "server": "",
            "watched": "",
            "changed": [],
            "marked": "",
            "deleted": "",
            "status": "未找到条目",
        }

        for server in servers:
            try:
                playback = self._fetch_playback(server, subscribe, media_type)
            except Exception as err:
                logger.warning(f"{self.LOG_TAG}{record['title']} 读取 {server['name']} 播放进度失败：{err}")
                record["status"] = f"{server['name']} 读取失败：{err}"
                continue
            if not playback or playback.get("status") != "ok":
                continue

            record["server"] = server["name"]
            record["watched"] = self._describe_watched(playback)

            # 1. 进度洗版：命中的集写成洗版完成优先级
            changed = self._apply_progress(subscribe, media_type, playback)
            record["changed"] = changed
            if changed and not self._dry_run:
                # 重新读取订阅：「看完即删」要基于刚写入的分集优先级重算整部优先级
                subscribe = SubscribeOper().get(getattr(subscribe, "id", None)) or subscribe

            # 2. 看完即删：先标记订阅，再清理内容
            if self._clean_enabled:
                hits, new_start, targets = self._plan_clean(subscribe, media_type, playback)
                if targets:
                    if self._clean_mark_subscribe:
                        payload = self._mark_clean(subscribe, media_type, hits, new_start)
                        if payload:
                            self._update_subscribe(subscribe, payload)
                            record["marked"] = self._describe_marked(media_type, hits, payload)
                        else:
                            record["marked"] = "无需标记"
                    else:
                        record["marked"] = "已关闭"
                    record["deleted"] = self._describe_deleted(self._delete_targets(server, targets))

            record["status"] = self._record_status(record)
            return record

        return record

    def _record_status(self, record: Dict[str, Any]) -> str:
        """把一条执行记录整理成一句话结果。"""
        parts = []
        if record.get("changed"):
            parts.append(f"跳过洗版 {self._describe_changed(record['changed'])}")
        if record.get("deleted"):
            parts.append(f"已清理 {record['deleted']}")
        if not parts:
            return "无变化"
        return "；".join(parts) + ("（试运行）" if self._dry_run else "")

    def _update_subscribe(self, subscribe, payload: Dict[str, Any]):
        """写回订阅；试运行时只记录日志。

        :param subscribe: 订阅记录
        :param payload: 要更新的字段
        """
        payload = dict(payload)
        payload.setdefault("last_update", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        title = self._title(subscribe)
        if self._dry_run:
            logger.info(f"{self.LOG_TAG}[试运行] {title} 将写入：{payload}")
            return
        try:
            SubscribeOper().update(subscribe.id, payload)
            logger.info(f"{self.LOG_TAG}{title} 已写入：{payload}")
        except Exception as err:
            logger.error(f"{self.LOG_TAG}{title} 写回订阅失败：{err}")

    # ---- 功能一：进度洗版 ----

    def _apply_progress(self, subscribe, media_type: MediaType, playback: Dict[str, Any]) -> List[Any]:
        """把命中的播放进度写回订阅，返回写入的跳过标记。

        :return: 本次写入的跳过标记（剧集号列表，或 ["整部"]），无变化时返回空列表
        """
        if not getattr(subscribe, "best_version", False):
            # 没开洗版的订阅本来就不会洗版，不必标记
            return []
        if media_type == MediaType.TV:
            return self._apply_progress_tv(subscribe, playback.get("episodes") or {})
        return self._apply_progress_movie(subscribe, playback.get("movie") or {})

    def _apply_progress_tv(self, subscribe, episodes: Dict[int, Dict[str, Any]]) -> List[Any]:
        """电视剧：把已看过的集写成洗版完成优先级。

        :param subscribe: 订阅记录
        :param episodes: 集号 -> 播放进度
        :return: 本次写入的集号列表（全集洗版时为 ["整部"]）
        """
        target = self._target_episodes(subscribe)
        if not target:
            logger.info(f"{self.LOG_TAG}{self._title(subscribe)} 缺少集数信息，跳过进度标记")
            return []

        hit = [episode for episode in target if episode in episodes and self._hit(episodes[episode])]
        if not hit:
            return []

        current_priority = self._as_int(getattr(subscribe, "current_priority", None), UNSET_PRIORITY)
        if getattr(subscribe, "best_version_full", False):
            # 全集洗版：宿主按整部判断完成，只有目标集全部看过才停止洗版
            if len(hit) < len(target) or current_priority >= BEST_VERSION_DONE_PRIORITY:
                return []
            self._update_subscribe(subscribe, {"current_priority": BEST_VERSION_DONE_PRIORITY})
            return ["整部"]

        priorities = self._episode_priorities(subscribe, target)
        changed = []
        for episode in hit:
            if priorities.get(str(episode), UNSET_PRIORITY) < BEST_VERSION_DONE_PRIORITY:
                priorities[str(episode)] = BEST_VERSION_DONE_PRIORITY
                changed.append(episode)
        if not changed:
            return []

        # 按宿主口径重算整部优先级：目标集范围内的最小值
        # （app/chain/subscribe.py:get_best_version_current_priority）
        new_priority = min(
            (priorities.get(str(episode), UNSET_PRIORITY) for episode in target),
            default=UNSET_PRIORITY,
        )
        self._update_subscribe(
            subscribe,
            {"episode_priority": priorities, "current_priority": new_priority},
        )
        return changed

    def _apply_progress_movie(self, subscribe, movie: Dict[str, Any]) -> List[str]:
        """电影：已看过则整部停止洗版。

        :param subscribe: 订阅记录
        :param movie: 电影的播放进度
        :return: 命中时返回 ["整部"]
        """
        if not movie or not self._hit(movie):
            return []
        current_priority = self._as_int(getattr(subscribe, "current_priority", None), UNSET_PRIORITY)
        if current_priority >= BEST_VERSION_DONE_PRIORITY:
            return []
        self._update_subscribe(subscribe, {"current_priority": BEST_VERSION_DONE_PRIORITY})
        return ["整部"]

    # ---- 功能二：两次上限（资源选择事件） ----

    @eventmanager.register(ChainEventType.ResourceSelection)
    def on_resource_selection(self, event: Event):
        """资源选择事件：按「最低档 / 最高档 / 已完成」三态过滤候选。"""
        try:
            self._handle_event(event)
        except Exception as err:
            logger.error(f"{self.LOG_TAG}处理资源选择事件失败：{err}\n{traceback.format_exc()}")

    def _handle_event(self, event: Event):
        """处理一次资源选择事件。"""
        if not self._enabled or not self._twice_enabled:
            return
        data = getattr(event, "event_data", None)
        if not data:
            return
        origin = str(getattr(data, "origin", "") or "")
        if not origin.startswith(SUBSCRIBE_SOURCE_PREFIX):
            # 手动下载 / RSS / 其它来源一律放行
            return
        subscribe_id = self._parse_subscribe_id(origin)
        if subscribe_id is None:
            return
        subscribe = SubscribeOper().get(subscribe_id)
        if not subscribe:
            return

        is_movie = str(getattr(subscribe, "type", "") or "") == MediaType.MOVIE.value
        if is_movie and not self._handle_movie:
            return
        if not is_movie and not self._handle_tv:
            return

        contexts = list(getattr(data, "contexts", None) or [])
        if not contexts:
            return

        kept, notes = self._filter(subscribe, contexts, is_movie)
        self._remember(subscribe, is_movie, contexts, kept, notes)

        if len(kept) == len(contexts):
            return
        if self._dry_run:
            logger.info(
                f"{self.LOG_TAG}[试运行] {self._title(subscribe)} 本批 {len(contexts)} 个候选，"
                f"按规则只保留 {len(kept)} 个：{self._describe_priorities(contexts)}"
            )
            return

        data.updated = True
        data.updated_contexts = kept
        data.source = "洗版管家"
        logger.info(
            f"{self.LOG_TAG}{self._title(subscribe)} 本批 {len(contexts)} 个候选，"
            f"保留 {len(kept)} 个：{self._describe_priorities(kept)}"
        )

    def _filter(self, subscribe, contexts: List[Any], is_movie: bool) -> Tuple[List[Any], List[str]]:
        """按每集阶段过滤候选。

        :param subscribe: 订阅记录
        :param contexts: 本批候选上下文
        :param is_movie: 是否电影
        :return: (保留的上下文列表, 本轮的阶段变化说明)
        """
        subscribe_id = str(getattr(subscribe, "id", ""))
        state = self._twice_state.setdefault(subscribe_id, {})
        season = self._as_int(getattr(subscribe, "season", None), 0)
        target = [1] if is_movie else self._target_episodes(subscribe)
        priorities = self._host_priorities(subscribe, target, is_movie)
        best_version = self._as_int(getattr(subscribe, "best_version", 0), 0) > 0
        # 只有开启了洗版的订阅才收尾：没开洗版的订阅宿主要靠「缺失集」重下，
        # 写 100 或永久拦截会让「删了以后再也下不回来」
        cap = self._twice_cap_after_upgrade and best_version

        entries = self._entries(contexts, target, is_movie)
        priorities_by_episode = self._priorities_by_episode(entries)

        notes: List[str] = []
        # 1) 用宿主的下载事实推进阶段
        done = self._reconcile(state, priorities, target, season, is_movie)
        # 2) 已经没有更高档可升的集，直接收尾
        for episode in target:
            key = self._key(season, episode, is_movie)
            record = state.get(key) or {}
            if self._as_int(record.get("stage"), STAGE_NEW) != STAGE_LOW:
                continue
            first = self._as_int(record.get("first"), UNSET_PRIORITY)
            candidates = priorities_by_episode.get(episode) or []
            if candidates and max(candidates) <= first and episode not in done:
                record["stage"] = STAGE_DONE
                done.append(episode)
        # 3) 收尾：写 100，让宿主不再为这些集搜索
        if done:
            self._mark_done(subscribe, done, state, priorities, target, season, is_movie, cap, notes)

        # 4) 计算本批每集允许的档位
        allowed = self._allowed_priorities(priorities_by_episode, state, season, is_movie, cap)

        # 5) 过滤候选
        kept: List[Any] = []
        for entry in entries:
            episodes = entry.get("episodes")
            if episodes is None:
                # 拿不到集数信息（既没有 allowed_episodes 也没有 episode_list）：
                # 无法判断档位，原样放行，避免整包资源被误拦
                kept.append(entry["context"])
                continue
            if not episodes:
                # 候选不覆盖任何目标集（例如开始集数之外的集），直接丢弃
                continue
            if all(self._accept(entry["pri"], allowed.get(episode)) for episode in episodes):
                kept.append(entry["context"])

        # 6) 记录阶段 1 的升级目标：本批放行的最高档
        for episode in target:
            key = self._key(season, episode, is_movie)
            record = state.get(key) or {}
            if self._as_int(record.get("stage"), STAGE_NEW) != STAGE_LOW:
                continue
            tier = allowed.get(episode)
            if not isinstance(tier, int):
                continue
            if tier > self._as_int(record.get("first"), UNSET_PRIORITY):
                record["target"] = tier

        return kept, notes

    def _entries(self, contexts: List[Any], target: List[int], is_movie: bool) -> List[Dict[str, Any]]:
        """把候选上下文整理成「优先级 + 覆盖集数」。"""
        target_set = set(target)
        entries: List[Dict[str, Any]] = []
        for context in contexts:
            torrent = getattr(context, "torrent_info", None)
            priority = self._as_int(getattr(torrent, "pri_order", UNSET_PRIORITY), UNSET_PRIORITY)
            entries.append(
                {
                    "context": context,
                    "pri": priority,
                    "episodes": self._context_episodes(context, target_set, is_movie),
                }
            )
        return entries

    def _context_episodes(self, context, target_set: Set[int], is_movie: bool) -> Optional[Set[int]]:
        """候选覆盖的集号集合。

        返回空集表示候选不覆盖任何目标集（应丢弃）；返回 None 表示完全拿不到集数信息（放行）。
        """
        if is_movie:
            return {1}

        # 宿主在分集洗版时会用 allowed_episodes 限定本批可下载的集
        allowed = getattr(context, "allowed_episodes", None)
        if allowed:
            return self._intersect(allowed, target_set)

        meta = getattr(context, "meta_info", None)
        episode_list = getattr(meta, "episode_list", None) if meta else None
        if episode_list:
            return self._intersect(episode_list, target_set)
        return None

    @staticmethod
    def _intersect(values: Any, target_set: Set[int]) -> Set[int]:
        """把集号列表换算成目标集子集。"""
        episodes: Set[int] = set()
        for value in values:
            try:
                number = int(value)
            except (TypeError, ValueError):
                continue
            if number in target_set:
                episodes.add(number)
        return episodes

    @staticmethod
    def _priorities_by_episode(entries: List[Dict[str, Any]]) -> Dict[int, List[int]]:
        """本批候选中每个集号出现过的优先级。"""
        result: Dict[int, List[int]] = {}
        for entry in entries:
            episodes = entry.get("episodes")
            if episodes is None:
                continue
            for episode in episodes:
                result.setdefault(episode, []).append(entry["pri"])
        return result

    def _allowed_priorities(
            self,
            priorities_by_episode: Dict[int, List[int]],
            state: Dict[str, Dict[str, Any]],
            season: int,
            is_movie: bool,
            cap: bool,
    ) -> Dict[int, Any]:
        """每集本轮允许的优先级。

        None 表示拦掉该集的全部候选；ALL_PRIORITIES 表示不做档位限制。
        """
        allowed: Dict[int, Any] = {}
        for episode, candidates in priorities_by_episode.items():
            record = state.get(self._key(season, episode, is_movie)) or {}
            stage = self._as_int(record.get("stage"), STAGE_NEW)
            if stage >= STAGE_DONE:
                # 洗版订阅：彻底拦掉，保证每集只下两次；
                # 非洗版订阅：宿主靠缺失集重下，永久拦截会让删掉的文件再也下不回来。
                # 「capped」表示宿主侧已经是 100（进度洗版或看完即删写进去的），同样拦掉。
                allowed[episode] = None if (cap or record.get("capped")) else ALL_PRIORITIES
                continue
            if stage <= STAGE_NEW and self._twice_lowest_first:
                allowed[episode] = min(candidates)
            else:
                allowed[episode] = max(candidates)
        return allowed

    @staticmethod
    def _accept(priority: int, allowed: Any) -> bool:
        """候选是否落在本轮允许的档位上。"""
        if allowed is ALL_PRIORITIES:
            return True
        if allowed is None:
            return False
        return priority == allowed

    def _reconcile(
            self,
            state: Dict[str, Dict[str, Any]],
            priorities: Dict[str, int],
            target: List[int],
            season: int,
            is_movie: bool,
    ) -> List[int]:
        """用宿主的下载事实推进每集阶段，返回本轮进入「已完成」的集。

        宿主的记录口径：
            - 电视剧：下载成功后 episode_priority[集号] = max(旧值, pri_order)
              （app/chain/subscribe.py:__record_subscribe_download_facts）
            - 电影：下载成功后 current_priority = max(pri_order)
              （app/chain/subscribe.py:__update_movie_download_priority）

        这样「下载失败」不会推进阶段：宿主的记录没有变化，下一轮仍按原阶段重试。
        """
        done: List[int] = []
        for episode in target:
            key = self._key(season, episode, is_movie)
            record = state.get(key)
            if not isinstance(record, dict):
                record = {"stage": STAGE_NEW, "first": UNSET_PRIORITY, "target": UNSET_PRIORITY, "prev": None}
                state[key] = record
            current = self._as_int(priorities.get(str(episode)), UNSET_PRIORITY)
            stage = self._as_int(record.get("stage"), STAGE_NEW)

            if current <= UNSET_PRIORITY:
                # 宿主侧没有该集的版本记录：回到阶段 0，允许重新走一次
                if stage != STAGE_NEW:
                    state[key] = {"stage": STAGE_NEW, "first": UNSET_PRIORITY,
                                  "target": UNSET_PRIORITY, "prev": None}
                continue

            if current >= BEST_VERSION_DONE_PRIORITY:
                # 宿主侧已是「洗版完成」（进度洗版 / 看完即删写的，或用户手工设的）：
                # 该集不再参与任何洗版，直接进入阶段 2 并标记 capped
                record["stage"] = STAGE_DONE
                record["capped"] = True
                continue

            if stage <= STAGE_NEW:
                record["stage"] = STAGE_LOW
                record["first"] = current
                continue

            if stage == STAGE_LOW:
                goal = self._as_int(record.get("target"), UNSET_PRIORITY)
                if goal and current >= goal:
                    record["stage"] = STAGE_DONE
                    done.append(episode)
                elif current > self._as_int(record.get("first"), UNSET_PRIORITY):
                    record["first"] = current
        return done

    def _mark_done(
            self,
            subscribe,
            episodes: List[int],
            state: Dict[str, Dict[str, Any]],
            priorities: Dict[str, int],
            target: List[int],
            season: int,
            is_movie: bool,
            cap: bool,
            notes: List[str],
    ):
        """把升级完成的集标记为洗版完成（写 100）。"""
        if not episodes:
            return
        label = self._title(subscribe)
        if not cap:
            notes.append(f"{label} 第 {sorted(episodes)} 集已完成一次升级")
            return

        if is_movie:
            current = self._as_int(getattr(subscribe, "current_priority", None), UNSET_PRIORITY)
            if current >= BEST_VERSION_DONE_PRIORITY:
                return
            record = state.get(self._key(season, 1, True)) or {}
            record["prev"] = current
            state[self._key(season, 1, True)] = record
            self._update_subscribe(subscribe, {"current_priority": BEST_VERSION_DONE_PRIORITY})
            notes.append(f"{label} 整部洗版完成")
            return

        changed: List[int] = []
        for episode in episodes:
            key = self._key(season, episode, is_movie)
            record = state.get(key) or {}
            before = self._as_int(priorities.get(str(episode)), UNSET_PRIORITY)
            if before >= BEST_VERSION_DONE_PRIORITY:
                continue
            record["prev"] = before
            state[key] = record
            priorities[str(episode)] = BEST_VERSION_DONE_PRIORITY
            changed.append(episode)
        if not changed:
            return

        # 按宿主口径重算整部优先级：目标集范围内的最小值
        # （app/chain/subscribe.py:get_best_version_current_priority）
        new_priority = min(
            (self._as_int(priorities.get(str(episode)), UNSET_PRIORITY) for episode in target),
            default=BEST_VERSION_DONE_PRIORITY,
        )
        self._update_subscribe(
            subscribe,
            {"episode_priority": priorities, "current_priority": new_priority},
        )
        notes.append(f"{label} 第 {sorted(changed)} 集洗版完成")

    # ---- 功能三：看完即删 ----

    def _plan_clean(
        self, subscribe, media_type: MediaType, playback: Dict[str, Any]
    ) -> Tuple[List[Any], int, List[Dict[str, Any]]]:
        """算出这一轮要清理哪些内容。

        :return: (命中的集号列表或 ["整部"], 新的开始集数, 要清理的条目列表)
        """
        if media_type == MediaType.TV:
            episodes = playback.get("episodes") or {}
            hits, new_start = self._watched_window(subscribe, episodes)
            targets = [episodes[episode] for episode in hits if episodes.get(episode)]
            return hits, new_start, targets

        movie = playback.get("movie") or {}
        if not movie or not self._hit(movie) or not self._delay_ok(movie):
            return [], 0, []
        return ["整部"], 0, [movie]

    def _watched_window(self, subscribe, episodes: Dict[int, Dict[str, Any]]) -> Tuple[List[int], int]:
        """从开始集数起找出「连续看完」的一段。

        遇到第一集没看完就停止：跳集观看不会把中间没看的集一起放弃。

        :return: (命中的集号列表, 新的开始集数)
        """
        start = self._as_int(getattr(subscribe, "start_episode", None), 1) or 1
        target = self._target_episodes(subscribe)
        if not target:
            # 总集数未知时退化成按实际入库的集号排序
            target = sorted(episodes)

        hits: List[int] = []
        for episode in target:
            progress = episodes.get(episode)
            if not progress or not self._hit(progress) or not self._delay_ok(progress):
                break
            hits.append(episode)

        new_start = hits[-1] + 1 if hits else start
        return hits, new_start

    def _mark_clean(self, subscribe, media_type: MediaType, hits: List[Any], new_start: int) -> Dict[str, Any]:
        """生成「看完即删」要写回订阅的字段。"""
        if media_type == MediaType.TV:
            return self._mark_clean_tv(subscribe, hits, new_start)
        return self._mark_clean_movie(subscribe)

    def _mark_clean_tv(self, subscribe, hits: List[int], new_start: int) -> Dict[str, Any]:
        """电视剧：前移开始集数，洗版订阅额外写洗版完成优先级。"""
        payload: Dict[str, Any] = {}
        total = self._as_int(getattr(subscribe, "total_episode", None), 0)
        start = self._as_int(getattr(subscribe, "start_episode", None), 1) or 1

        # 宿主按 range(start_episode, total_episode + 1) 计算缺失集，
        # 前移开始集数即可让已清理的集落在窗口外
        if new_start and new_start > start:
            payload["start_episode"] = new_start

        if getattr(subscribe, "best_version", False):
            old_target = list(range(start, total + 1)) if total >= start else []
            if old_target:
                priorities = self._episode_priorities(subscribe, old_target)
                touched = False
                for episode in hits:
                    if priorities.get(str(episode), UNSET_PRIORITY) < BEST_VERSION_DONE_PRIORITY:
                        priorities[str(episode)] = BEST_VERSION_DONE_PRIORITY
                        touched = True
                if touched:
                    payload["episode_priority"] = priorities
                # 按宿主口径重算整部优先级：剩余目标集范围内的最小值
                remaining = [episode for episode in old_target if episode >= new_start]
                if not remaining:
                    payload["current_priority"] = BEST_VERSION_DONE_PRIORITY
                else:
                    payload["current_priority"] = min(
                        (priorities.get(str(episode), UNSET_PRIORITY) for episode in remaining),
                        default=UNSET_PRIORITY,
                    )
        return payload

    def _mark_clean_movie(self, subscribe) -> Dict[str, Any]:
        """电影：洗版订阅整部标记完成。"""
        if not getattr(subscribe, "best_version", False):
            return {}
        if self._as_int(getattr(subscribe, "current_priority", None), UNSET_PRIORITY) >= BEST_VERSION_DONE_PRIORITY:
            return {}
        return {"current_priority": BEST_VERSION_DONE_PRIORITY}

    def _delete_targets(self, server: Dict[str, Any], targets: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """按条目逐个清理：媒体服务器条目、媒体库文件、源文件、刮削文件、空目录、整理记录。

        :param server: 媒体服务器信息
        :param targets: 要清理的条目（含 path / id / label）
        :return: 各类已清理项
        """
        result: Dict[str, List[str]] = {
            "item": [], "file": [], "source": [], "scrap": [], "dir": [], "history": [],
        }
        for target in targets:
            label = target.get("label") or ""
            item_id = target.get("id") or ""
            path_text = (target.get("path") or "").strip()
            path = Path(path_text) if path_text else None

            if self._dry_run:
                logger.info(
                    f"{self.LOG_TAG}[试运行] 将清理 {label}："
                    f"条目 {item_id or '-'}，文件 {path_text or '-'}"
                )
                if item_id and self._clean_delete_mediaserver:
                    result["item"].append(label)
                if path and self._clean_delete_media_files:
                    result["file"].append(str(path))
                if path and self._clean_delete_scrap_infos:
                    result["scrap"].append(label)
                if path and self._clean_delete_history:
                    result["history"].append(str(path))
                continue

            # 先删媒体服务器条目：避免宿主扫描到文件缺失后反复刷新
            if item_id and self._clean_delete_mediaserver:
                if self._delete_mediaserver_item(server, item_id, label):
                    result["item"].append(label)

            if path and self._clean_delete_media_files:
                if self._remove_file(path):
                    result["file"].append(str(path))

            if path and self._clean_delete_source_files:
                for source in self._source_paths(path):
                    if self._remove_file(source):
                        result["source"].append(str(source))

            if path and self._clean_delete_scrap_infos:
                result["scrap"].extend(self._delete_scrap_files(path))

            if path and self._clean_delete_empty_folders:
                result["dir"].extend(self._delete_empty_dirs(path))

            if path and self._clean_delete_history:
                if self._delete_history_record(path):
                    result["history"].append(str(path))

        return result

    def _remove_file(self, path: Path) -> bool:
        """删除单个文件，带基本的安全检查。"""
        try:
            if len(path.parts) < MIN_PATH_PARTS:
                logger.warning(f"{self.LOG_TAG}路径过短，拒绝删除：{path}")
                return False
            if not path.exists() or path.is_dir():
                return False
            path.unlink()
            logger.info(f"{self.LOG_TAG}已删除文件：{path}")
            return True
        except OSError as err:
            logger.error(f"{self.LOG_TAG}删除文件失败：{path} - {err}")
            return False

    def _source_paths(self, path: Path) -> List[Path]:
        """从整理记录里找出媒体库文件对应的下载目录源文件。"""
        try:
            history = TransferHistoryOper().get_by_dest(str(path))
        except Exception as err:
            logger.warning(f"{self.LOG_TAG}查询整理记录失败：{path} - {err}")
            return []
        if not history or not getattr(history, "src", None):
            return []
        source = Path(history.src)
        if source == path:
            return []
        return [source]

    def _delete_history_record(self, path: Path) -> bool:
        """删除与媒体库文件对应的整理记录。"""
        try:
            oper = TransferHistoryOper()
            history = oper.get_by_dest(str(path)) or oper.get_by_src(str(path))
            if not history:
                return False
            oper.delete(history.id)
            logger.info(f"{self.LOG_TAG}已删除整理记录：{history.id} - {path}")
            return True
        except Exception as err:
            logger.error(f"{self.LOG_TAG}删除整理记录失败：{path} - {err}")
            return False

    def _is_scrap_file(self, path: Path) -> bool:
        """判断文件是否为刮削文件（元数据 / 图片 / 字幕）。"""
        name = path.name.lower()
        return any(name.endswith(extension) for extension in self.SCRAP_EXTENSIONS)

    @staticmethod
    def _same_media_scrap_name(file: Path, media_stem: str) -> bool:
        """刮削文件是否属于该媒体：名称以媒体名为前缀且紧跟非字母数字边界。"""
        name = file.name
        if not name.startswith(media_stem):
            return False
        if len(name) == len(media_stem):
            return True
        return not name[len(media_stem)].isalnum()

    @classmethod
    def _belongs_to_other_media(cls, scrap_name: str, media_stem: str, other_stems: set) -> bool:
        """刮削文件是否属于同目录中的其他媒体文件。"""
        for stem in other_stems:
            # 同 stem 的其他媒体文件：刮削文件为两者共享，不能清理
            if stem == media_stem:
                return True
            if len(stem) <= len(media_stem):
                continue
            if not stem.startswith(media_stem):
                continue
            # 同目录还有该集的另一个版本（如 "Show - S01E01.x265"）：
            # 基础名的刮削文件可能被两者共用，保守保留
            if not stem[len(media_stem)].isalnum():
                return True
            # 刮削文件明确属于更长的媒体名（如 "Show - S01E011.nfo"）
            if not scrap_name.startswith(stem):
                continue
            if len(scrap_name) == len(stem):
                continue
            if not scrap_name[len(stem)].isalnum():
                return True
        return False

    def _delete_scrap_files(self, path: Path) -> List[str]:
        """清理与 path 同名的刮削文件与刮削目录。"""
        removed: List[str] = []
        parent = path.parent
        if not parent.is_dir():
            return removed

        stem = path.stem
        try:
            other_stems = {
                file.stem
                for file in parent.iterdir()
                if not file.is_dir() and file.name != path.name and not self._is_scrap_file(file)
            }
        except OSError:
            return removed

        try:
            for file in list(parent.iterdir()):
                if not self._same_media_scrap_name(file, stem):
                    continue
                if self._belongs_to_other_media(file.name, stem, other_stems):
                    continue
                if file.is_dir() and file.suffix.lower() in self.SCRAP_DIR_SUFFIXES:
                    shutil.rmtree(file)
                    removed.append(str(file))
                    logger.info(f"{self.LOG_TAG}已删除刮削目录：{file}")
                elif self._is_scrap_file(file):
                    file.unlink()
                    removed.append(str(file))
                    logger.info(f"{self.LOG_TAG}已删除刮削文件：{file}")
        except OSError as err:
            logger.error(f"{self.LOG_TAG}清理刮削文件失败：{path} - {err}")
        return removed

    def _delete_empty_dirs(self, path: Path) -> List[str]:
        """从 path 所在目录向上逐级删除空目录，最多 MAX_UP_LEVELS 层。"""
        removed: List[str] = []
        current = path.parent
        for _ in range(MAX_UP_LEVELS):
            try:
                if not current.is_dir():
                    break
                if any(current.iterdir()):
                    break
                current.rmdir()
            except OSError as err:
                logger.debug(f"{self.LOG_TAG}清理空目录停止：{current} - {err}")
                break
            removed.append(str(current))
            logger.info(f"{self.LOG_TAG}已删除空目录：{current}")
            current = current.parent
        return removed

    # ---- 媒体服务器 ----

    def _get_servers(self) -> List[Dict[str, Any]]:
        """取参与检测的媒体服务器实例。"""
        try:
            services = MediaServerHelper().get_services() or {}
        except Exception as err:
            logger.error(f"{self.LOG_TAG}读取媒体服务器失败：{err}")
            return []

        result = []
        for name, service in services.items():
            server_type = (getattr(service, "type", "") or "").lower()
            if server_type not in SUPPORTED_SERVER_TYPES:
                continue
            if self._servers and name not in self._servers:
                continue
            client = getattr(service, "instance", None)
            if client is None:
                continue

            host = str(getattr(client, "_host", "") or "").strip()
            apikey = str(getattr(client, "_apikey", "") or "").strip()
            if not host or not apikey:
                conf = getattr(service, "config", None)
                conf_data = getattr(conf, "config", None) or {}
                host = host or str(conf_data.get("host") or "").strip()
                apikey = apikey or str(conf_data.get("apikey") or "").strip()
            if host and not host.endswith("/"):
                host = f"{host}/"

            result.append(
                {
                    "name": name,
                    "type": server_type,
                    "client": client,
                    "base_url": host,
                    "apikey": apikey,
                    # Emby 的接口路径带 emby/ 前缀，Jellyfin 直接是根路径
                    "prefix": "emby/" if server_type == "emby" else "",
                }
            )

        if not result:
            logger.warning(
                f"{self.LOG_TAG}没有匹配到可用的 Emby / Jellyfin 服务器（当前配置：{self._servers or '全部'}）"
            )
        return result

    def _fetch_playback(self, server: Dict[str, Any], subscribe, media_type: MediaType) -> Dict[str, Any]:
        """读取订阅对应条目在媒体服务器上的播放进度与文件路径。

        :param server: 媒体服务器信息（name / type / client / prefix）
        :param subscribe: 订阅记录
        :param media_type: 媒体类型
        :return: {"status": "ok"|"not_found"|"no_user", "episodes"|"movie": ...}
        """
        client = server["client"]
        prefix = server["prefix"]
        user_ids = self._resolve_users(client)
        if not user_ids:
            return {"status": "no_user"}

        item = self._find_item(client, prefix, user_ids[0], subscribe, media_type)
        if not item or not item.get("Id"):
            return {"status": "not_found"}

        if media_type == MediaType.MOVIE:
            movie: Dict[str, Any] = {}
            for user_id in user_ids:
                data = self._get_json(
                    client,
                    f"[HOST]{prefix}Users/{user_id}/Items/{item['Id']}?Fields=UserData,Path&api_key=[APIKEY]",
                )
                if not data:
                    continue
                progress = self._progress_of(data)
                progress["id"] = data.get("Id") or item["Id"]
                progress["path"] = data.get("Path") or ""
                progress["label"] = f"电影 {data.get('Name') or ''}".strip()
                if not movie \
                        or progress["percentage"] > float(movie.get("percentage") or 0) \
                        or (progress["played"] and not movie.get("played")):
                    movie = progress
            return {"status": "ok", "movie": movie}

        episodes: Dict[int, Dict[str, Any]] = {}
        for user_id in user_ids:
            data = self._get_json(
                client,
                f"[HOST]{prefix}Users/{user_id}/Items?ParentId={item['Id']}&Recursive=true"
                f"&IncludeItemTypes=Episode&Fields=UserData,IndexNumber,ParentIndexNumber,Path&api_key=[APIKEY]",
            )
            for episode_item in (data or {}).get("Items") or []:
                if not self._season_match(subscribe, episode_item):
                    continue
                number = self._as_int(episode_item.get("IndexNumber"), 0)
                if number <= 0:
                    continue
                progress = self._progress_of(episode_item)
                progress["id"] = episode_item.get("Id") or ""
                progress["path"] = episode_item.get("Path") or ""
                progress["label"] = f"E{number}"
                exists = episodes.get(number)
                if not exists or progress["percentage"] > exists["percentage"] \
                        or (progress["played"] and not exists["played"]):
                    episodes[number] = progress
        return {"status": "ok", "episodes": episodes}

    def _find_item(self, client, prefix: str, user_id, subscribe, media_type: MediaType) -> Optional[Dict[str, Any]]:
        """在媒体服务器上按名称/年份/TMDB ID 找订阅对应的剧集或电影条目。"""
        name = (getattr(subscribe, "name", "") or "").strip()
        if not name:
            return None

        item_type = "Series" if media_type == MediaType.TV else "Movie"
        data = self._get_json(
            client,
            f"[HOST]{prefix}Users/{user_id}/Items?Recursive=true&IncludeItemTypes={item_type}"
            f"&SearchTerm={quote(name)}&Fields=ProductionYear,ProviderIds&Limit=20&api_key=[APIKEY]",
        )
        items = (data or {}).get("Items") or []
        if not items:
            return None

        year = self._as_int(getattr(subscribe, "year", None), 0)
        tmdbid = self._tmdbid(subscribe)
        best, best_score = None, 0
        for item in items:
            score = 0
            if (item.get("Name") or "").strip() == name:
                score += 4
            if year and self._as_int(item.get("ProductionYear"), 0) == year:
                score += 2
            if tmdbid and str(((item.get("ProviderIds") or {}).get("Tmdb")) or "") == str(tmdbid):
                score += 8
            if score > best_score:
                best, best_score = item, score
        return best if best_score > 0 else None

    def _resolve_users(self, client) -> List[Any]:
        """解析要统计的用户 ID 列表；未配置用户名时用服务器配置的用户。"""
        names = [name.strip() for name in (self._usernames or "").split(",") if name.strip()]
        if not names:
            user_id = getattr(client, "user", None)
            return [user_id] if user_id else []

        user_ids = []
        for name in names:
            try:
                user_id = client.get_user(name)
            except Exception as err:
                logger.warning(f"{self.LOG_TAG}解析媒体服务器用户 {name} 失败：{err}")
                user_id = None
            if user_id:
                user_ids.append(user_id)
        return user_ids

    @staticmethod
    def _get_json(client, url: str) -> Optional[Dict[str, Any]]:
        """发起一次媒体服务器 API 请求并解析 JSON。"""
        try:
            response = client.get_data(url)
        except Exception as err:
            logger.debug(f"{WashManager.LOG_TAG}请求媒体服务器失败：{err}")
            return None
        if response is None:
            return None
        try:
            return response.json()
        except Exception:
            return None

    @staticmethod
    def _delete_mediaserver_item(server: Dict[str, Any], item_id: str, label: str) -> bool:
        """删除单个媒体服务器条目。"""
        base_url = server.get("base_url") or ""
        apikey = server.get("apikey") or ""
        if not base_url or not apikey:
            logger.warning(
                f"{WashManager.LOG_TAG}媒体服务器 {server.get('name')} 缺少地址或密钥，跳过条目清理"
            )
            return False
        url = f"{base_url}{server.get('prefix') or ''}Items/{item_id}"
        try:
            response = RequestUtils().delete_res(url, params={"api_key": apikey})
        except Exception as err:
            logger.error(f"{WashManager.LOG_TAG}删除媒体服务器条目 {item_id} 出错：{err}")
            return False
        status_code = getattr(response, "status_code", None)
        if status_code in (200, 204):
            logger.info(
                f"{WashManager.LOG_TAG}已删除媒体服务器 {server.get('name')} 的条目：{item_id}（{label}）"
            )
            return True
        logger.error(f"{WashManager.LOG_TAG}删除媒体服务器条目 {item_id} 失败：{status_code or '无响应'}")
        return False

    @staticmethod
    def _progress_of(item: Dict[str, Any]) -> Dict[str, Any]:
        """把 Emby/Jellyfin 的 UserData 归一化成 {played, percentage, last_played}。"""
        user_data = (item or {}).get("UserData") or {}
        played = bool(user_data.get("Played"))
        percentage = user_data.get("PlayedPercentage")
        if percentage is None:
            position = user_data.get("PlaybackPositionTicks") or 0
            runtime = (item or {}).get("RunTimeTicks") or 0
            percentage = (position / runtime * 100) if runtime else 0
        try:
            percentage = float(percentage or 0)
        except (TypeError, ValueError):
            percentage = 0.0
        if played:
            percentage = 100.0
        return {
            "played": played,
            "percentage": round(percentage, 1),
            "last_played": user_data.get("LastPlayedDate"),
        }

    def _hit(self, progress: Dict[str, Any]) -> bool:
        """按配置的判定方式判断这一集/这部电影是否算「看过」。"""
        played = bool(progress.get("played"))
        percentage = float(progress.get("percentage") or 0)
        if self._mode == MODE_PLAYED:
            return played
        if self._mode == MODE_ANY_PLAY:
            return played or percentage > 0
        return played or percentage >= self._threshold

    def _delay_ok(self, progress: Dict[str, Any]) -> bool:
        """最后播放时间是否已经超过配置的延迟天数。"""
        if self._clean_delay_days <= 0:
            return True
        text = progress.get("last_played")
        if not text:
            return True
        try:
            stamp = datetime.datetime.fromisoformat(str(text).replace("Z", "+00:00"))
        except ValueError:
            return True
        if stamp.tzinfo:
            stamp = stamp.astimezone(pytz.timezone(settings.TZ)).replace(tzinfo=None)
        return datetime.datetime.now() - stamp >= datetime.timedelta(days=self._clean_delay_days)

    # ---- 订阅数据口径（与宿主 app/chain/subscribe.py 保持一致） ----

    def _total_episode(self, subscribe) -> int:
        """订阅的总集数（0 表示未知）。"""
        return self._as_int(getattr(subscribe, "total_episode", None), 0)

    def _target_episodes(self, subscribe) -> List[int]:
        """订阅的目标集号范围（宿主口径：range(start_episode or 1, total_episode + 1)）。"""
        start = self._as_int(getattr(subscribe, "start_episode", None), 1) or 1
        total = self._total_episode(subscribe)
        if total < start:
            return []
        return list(range(start, total + 1))

    def _episode_priorities(self, subscribe, target: List[int]) -> Dict[str, int]:
        """取分集洗版优先级，并按宿主口径补齐。

        宿主在 episode_priority 为空且非全集洗版时，会用 current_priority 回退填充所有目标集
        （app/chain/subscribe.py:get_episode_priority）。这里保持一致：
        否则一旦写入非空 map，宿主的回退失效，未写入的集会被当成 0，反而放宽洗版准入。
        """
        stored = getattr(subscribe, "episode_priority", None) or {}
        priorities: Dict[str, int] = {}
        for key, value in stored.items():
            try:
                priorities[str(int(key))] = self._as_int(value, UNSET_PRIORITY)
            except (TypeError, ValueError):
                continue

        if not priorities and not getattr(subscribe, "best_version_full", False):
            current = getattr(subscribe, "current_priority", None)
            if current is not None:
                priorities = {str(episode): self._as_int(current, UNSET_PRIORITY) for episode in target}
        return priorities

    def _host_priorities(self, subscribe, target: List[int], is_movie: bool) -> Dict[str, int]:
        """宿主视角的「该集当前已有版本的优先级」。"""
        if is_movie:
            return {"1": self._as_int(getattr(subscribe, "current_priority", None), UNSET_PRIORITY)}
        return self._episode_priorities(subscribe, target)

    @staticmethod
    def _media_type(subscribe) -> Optional[MediaType]:
        """订阅的媒体类型。"""
        value = getattr(subscribe, "type", None)
        if value == MediaType.TV.value:
            return MediaType.TV
        if value == MediaType.MOVIE.value:
            return MediaType.MOVIE
        return None

    @staticmethod
    def _tmdbid(subscribe) -> Optional[int]:
        """订阅的 TMDB ID（V3 用 media_source + media_id，V2 用 tmdbid）。"""
        media_source = getattr(subscribe, "media_source", None)
        value = getattr(subscribe, "media_id", None)
        if media_source not in (None, "", "themoviedb"):
            value = None
        if not value:
            value = getattr(subscribe, "tmdbid", None)
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _season_match(self, subscribe, item: Dict[str, Any]) -> bool:
        """按订阅的季号过滤分集（未指定季时不限）。"""
        season = self._as_int(getattr(subscribe, "season", None), 0)
        if season <= 0:
            return True
        return self._as_int(item.get("ParentIndexNumber"), 0) == season

    # ---- 状态存取（两次上限） ----

    @staticmethod
    def _key(season: int, episode: int, is_movie: bool) -> str:
        """每集状态 key：电影固定 m-1，电视剧 S{季}E{集}。"""
        if is_movie:
            return "m-1"
        return f"s{season}e{episode}"

    @staticmethod
    def _parse_subscribe_id(origin: str) -> Optional[int]:
        """从订阅来源字符串里取订阅 ID。"""
        try:
            info = json.loads(origin.split("|", 1)[1])
        except (IndexError, ValueError, TypeError):
            return None
        if not isinstance(info, dict):
            return None
        try:
            return int(info.get("id"))
        except (TypeError, ValueError):
            return None

    def _remember(self, subscribe, is_movie: bool, contexts: List[Any], kept: List[Any], notes: List[str]):
        """记录最近一次处理摘要并落盘状态。"""
        subscribe_id = str(getattr(subscribe, "id", ""))
        self._twice_labels[subscribe_id] = self._title(subscribe)
        self._twice_last_run = {
            "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "title": self._title(subscribe),
            "type": "电影" if is_movie else "电视剧",
            "total": len(contexts),
            "kept": len(kept),
            "notes": notes,
        }
        try:
            self.save_data("twice_state", self._twice_state)
            self.save_data("twice_labels", self._twice_labels)
            self.save_data("twice_last_run", self._twice_last_run)
        except Exception as err:
            logger.error(f"{self.LOG_TAG}保存状态失败：{err}")

    # ---- 插件接口实现 ----

    def api_status(self) -> Dict[str, Any]:
        """查看每集阶段。"""
        episodes: Dict[str, Dict[str, Any]] = {}
        for subscribe_id, records in (self._twice_state or {}).items():
            if not isinstance(records, dict):
                continue
            label = self._twice_labels.get(subscribe_id) or subscribe_id
            for key, record in records.items():
                if not isinstance(record, dict):
                    continue
                stage = self._as_int(record.get("stage"), STAGE_NEW)
                episodes.setdefault(label, {})[key] = {
                    "stage": stage,
                    "stage_text": STAGE_TEXT.get(stage, str(stage)),
                    "first": record.get("first"),
                    "target": record.get("target"),
                }
        return {
            "enabled": self._enabled,
            "dry_run": self._dry_run,
            "progress_enabled": self._progress_enabled,
            "twice_enabled": self._twice_enabled,
            "clean_enabled": self._clean_enabled,
            "last_run": self._last_run,
            "twice_last_run": self._twice_last_run,
            "episodes": episodes,
        }

    def api_reset(self) -> Dict[str, Any]:
        """清空两次上限的阶段记录（不改订阅数据）。"""
        count = sum(len(v) for v in (self._twice_state or {}).values() if isinstance(v, dict))
        self._twice_state = {}
        self._twice_labels = {}
        self._twice_last_run = {}
        self.save_data("twice_state", self._twice_state)
        self.save_data("twice_labels", self._twice_labels)
        self.save_data("twice_last_run", self._twice_last_run)
        logger.info(f"{self.LOG_TAG}已清空 {count} 条洗版阶段记录")
        return {"success": True, "cleared": count}

    def api_restore(self) -> Dict[str, Any]:
        """把插件写过的 100 还原成写入前的值。"""
        restored: List[str] = []
        for subscribe_id, records in (self._twice_state or {}).items():
            if not isinstance(records, dict):
                continue
            try:
                subscribe = SubscribeOper().get(int(subscribe_id))
            except (TypeError, ValueError):
                subscribe = None
            if not subscribe:
                continue
            is_movie = str(getattr(subscribe, "type", "") or "") == MediaType.MOVIE.value
            if is_movie:
                record = records.get("m-1") or {}
                prev = record.get("prev")
                if prev is None:
                    continue
                if self._as_int(getattr(subscribe, "current_priority", None), UNSET_PRIORITY) \
                        != BEST_VERSION_DONE_PRIORITY:
                    continue
                self._update_subscribe(subscribe, {"current_priority": self._as_int(prev, UNSET_PRIORITY)})
                restored.append(f"{self._title(subscribe)} 整部")
                continue

            stored = getattr(subscribe, "episode_priority", None) or {}
            priorities: Dict[str, int] = {}
            for key, value in stored.items():
                try:
                    priorities[str(int(key))] = self._as_int(value, UNSET_PRIORITY)
                except (TypeError, ValueError):
                    continue
            changed: List[int] = []
            for key, record in records.items():
                if not isinstance(record, dict):
                    continue
                prev = record.get("prev")
                if prev is None:
                    continue
                try:
                    episode = int(str(key).split("e")[-1])
                except (TypeError, ValueError):
                    continue
                if self._as_int(priorities.get(str(episode)), UNSET_PRIORITY) != BEST_VERSION_DONE_PRIORITY:
                    continue
                priorities[str(episode)] = self._as_int(prev, UNSET_PRIORITY)
                changed.append(episode)
            if not changed:
                continue
            target = self._target_episodes(subscribe)
            new_priority = min(
                (self._as_int(priorities.get(str(episode)), UNSET_PRIORITY) for episode in target),
                default=UNSET_PRIORITY,
            )
            self._update_subscribe(
                subscribe,
                {"episode_priority": priorities, "current_priority": new_priority},
            )
            restored.append(f"{self._title(subscribe)} 第 {sorted(changed)} 集")

        self._twice_state = {}
        self._twice_labels = {}
        self.save_data("twice_state", self._twice_state)
        self.save_data("twice_labels", self._twice_labels)
        logger.info(f"{self.LOG_TAG}已还原：{restored or '无'}")
        return {"success": True, "restored": restored}

    # ---- 展示与通知 ----

    def _title(self, subscribe) -> str:
        """订阅的展示名：名称（年份）第 N 季。"""
        name = getattr(subscribe, "name", "") or ""
        year = getattr(subscribe, "year", None)
        title = f"{name}（{year}）" if year else name
        if self._media_type(subscribe) == MediaType.TV:
            season = self._as_int(getattr(subscribe, "season", None), 0)
            if season > 0:
                title += f" 第 {season} 季"
        return title

    def _describe_watched(self, playback: Dict[str, Any]) -> str:
        """把读取到的进度整理成一行文字。"""
        movie = playback.get("movie")
        if movie is not None:
            flag = "已看" if movie.get("played") else "未看完"
            return f"{flag} {movie.get('percentage')}%"
        episodes = playback.get("episodes") or {}
        if not episodes:
            return "无播放记录"
        parts = []
        for episode in sorted(episodes):
            progress = episodes[episode]
            parts.append(f"E{episode}({progress.get('percentage')}%)")
        return " ".join(parts)

    @staticmethod
    def _describe_changed(changed) -> str:
        """把写入的跳过标记整理成文字。"""
        if not changed:
            return ""
        if changed == ["整部"]:
            return "整部"
        return "、".join(f"E{episode}" for episode in changed)

    def _describe_marked(self, media_type: MediaType, hits: List[Any], payload: Dict[str, Any]) -> str:
        """把「看完即删」写回订阅的动作整理成文字。"""
        parts = []
        if media_type == MediaType.TV:
            if payload.get("start_episode"):
                parts.append(f"开始集数 → {payload['start_episode']}")
            if payload.get("episode_priority"):
                parts.append(f"洗版完成 {self._describe_hits(hits)}")
        elif payload.get("current_priority"):
            parts.append("整部洗版完成")
        return "，".join(parts) if parts else "无需标记"

    @staticmethod
    def _describe_hits(hits: List[Any]) -> str:
        """把命中的集号整理成文字。"""
        if not hits:
            return ""
        if hits == ["整部"]:
            return "整部"
        return "、".join(f"E{episode}" for episode in hits)

    @staticmethod
    def _describe_deleted(result: Dict[str, List[str]]) -> str:
        """把清理结果整理成文字。"""
        if not result:
            return ""
        parts = []
        for key, label in (
            ("item", "条目"),
            ("file", "文件"),
            ("source", "源文件"),
            ("scrap", "刮削"),
            ("dir", "目录"),
            ("history", "记录"),
        ):
            count = len(result.get(key) or [])
            if count:
                parts.append(f"{label}{count}")
        return "、".join(parts)

    @staticmethod
    def _describe_priorities(contexts: List[Any]) -> str:
        """把候选的优先级列出来，便于核对档位。"""
        values = []
        for context in contexts:
            torrent = getattr(context, "torrent_info", None)
            values.append(str(WashManager._as_int(getattr(torrent, "pri_order", 0), 0)))
        return "[" + ", ".join(values) + "]"

    def _page_summary(self, last: Dict[str, Any]) -> str:
        """详情页顶部摘要。"""
        switches = [
            f"进度洗版{'开' if self._progress_enabled else '关'}",
            f"两次上限{'开' if self._twice_enabled else '关'}",
            f"看完即删{'开' if self._clean_enabled else '关'}",
        ]
        parts = ["已启用" if self._enabled else "未启用", "、".join(switches)]
        if self._dry_run:
            parts.append("试运行（只记日志）")
        if not last:
            parts.append("尚未执行过检查")
            return "；".join(parts)
        parts.append(f"最近一次执行：{last.get('time') or '-'}")
        if last.get("message"):
            parts.append(str(last["message"]))
        if last.get("changed"):
            parts.append(f"写入 {last['changed']} 个跳过标记")
        if last.get("cleaned"):
            parts.append(f"清理 {last['cleaned']} 个订阅")
        return "；".join(parts)

    def _send_notify(self, records: List[Dict[str, Any]]):
        """把本轮结果发到配置的通知渠道。"""
        lines = []
        for record in records:
            details = []
            if record.get("changed"):
                details.append(f"跳过洗版 {self._describe_changed(record['changed'])}")
            if record.get("deleted"):
                details.append(f"清理 {record['deleted']}")
            if not details:
                continue
            lines.append(f"{record.get('title')}（{record.get('type')}）：" + "，".join(details))
        if not lines:
            return

        text = "\n".join(lines)
        for channel in (self._notify_channel or []):
            # 配置里存的是宿主通知配置的 type，宿主的消息模型只认通知渠道枚举
            notify_channel = self.NOTIFY_TYPE_MAP.get(str(channel).lower())
            if not notify_channel:
                logger.warning(f"{self.LOG_TAG}未知的通知渠道 {channel}，跳过")
                continue
            try:
                self.post_message(
                    channel=notify_channel,
                    title="【洗版管家】已按播放进度处理洗版",
                    text=text,
                )
            except Exception as err:
                logger.error(f"{self.LOG_TAG}发送通知失败（{channel}）：{err}")

    # ---- 配置辅助 ----

    def _current_config(self) -> Dict[str, Any]:
        """当前配置的规范化快照（用于回写与表单默认值）。"""
        return {
            "enabled": self._enabled,
            "dry_run": self._dry_run,
            "onlyonce": self._onlyonce,
            "notify": self._notify,
            "notify_channel": self._notify_channel,
            "interval_minutes": self._interval,
            "servers": self._servers,
            "usernames": self._usernames,
            "mode": self._mode,
            "threshold": self._threshold,
            "handle_tv": self._handle_tv,
            "handle_movie": self._handle_movie,
            "progress_enabled": self._progress_enabled,
            "twice_enabled": self._twice_enabled,
            "twice_lowest_first": self._twice_lowest_first,
            "twice_cap_after_upgrade": self._twice_cap_after_upgrade,
            "clean_enabled": self._clean_enabled,
            "clean_mark_subscribe": self._clean_mark_subscribe,
            "clean_delete_media_files": self._clean_delete_media_files,
            "clean_delete_scrap_infos": self._clean_delete_scrap_infos,
            "clean_delete_empty_folders": self._clean_delete_empty_folders,
            "clean_delete_mediaserver": self._clean_delete_mediaserver,
            "clean_delete_history": self._clean_delete_history,
            "clean_delete_source_files": self._clean_delete_source_files,
            "clean_delay_days": self._clean_delay_days,
        }

    @staticmethod
    def _as_int(value: Any, default: int) -> int:
        """把配置/数据库里的值安全转成整数。"""
        if isinstance(value, bool):
            return int(value)
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _as_list(value: Any) -> List[str]:
        """把配置值转成字符串列表（兼容逗号分隔的字符串）。"""
        if not value:
            return []
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        if isinstance(value, (list, tuple, set)):
            return [str(item) for item in value if str(item).strip()]
        return []
