# -*- coding: utf-8 -*-
"""看完即删（MoviePilot V3）

功能：
    按媒体服务器（Emby / Jellyfin）上的播放进度，自动清理已经看完的剧集与电影。

    - 判定：某个目标集（或电影）已经看过（判定方式见配置 mode），即视为「可以清理」；
    - 标记：把订阅的 start_episode 前移到「连续看完的最后一集 + 1」。这是宿主的缺失集窗口口径
      （app/chain/subscribe/completion.py）：落在窗口外的已删集不再被判定为缺失，
      因此不会被重新下载；洗版订阅额外把命中集写成洗版优先级 100（宿主口径：该集洗版完成）；
    - 清理：按媒体服务器条目的 Path 删除媒体库文件、刮削文件（元数据/图片/字幕）、空目录、
      媒体服务器条目与整理记录，可选删除下载目录源文件（会破坏做种，默认关闭）。

工作方式：
    1. 定时（默认 30 分钟）或手动触发一次；
    2. 从订阅库取「订阅中（state=R）」的订阅；
    3. 用 MediaServerHelper 取启用的 Emby / Jellyfin 实例，按订阅名称/年份/TMDB ID 找到对应条目；
    4. 读取每一集的 UserData（Played / PlaybackPositionTicks / PlayedPercentage）与 Path；
    5. 从 start_episode 开始逐集判断，遇到第一集没看完就停止，保证只清理「连续看完」的一段；
    6. 先标记订阅（避免删掉文件后被判缺失重新下载），再按配置删除内容；
    7. 可选发送通知，并在插件详情页展示最近一次执行结果。

设计说明：
    - 判定口径全部可配置：percentage（默认，进度 >= 阈值）/ any_play（进度 > 0）/ played（已看标记）；
    - 支持「试运行」：只写日志、不动文件、不写库，确认口径无误后再正式启用；
    - 支持「延迟清理」：最后播放时间距今不足 N 天时跳过，避免播放器自动标记已看导致误删；
    - 只处理「连续看完」的一段，跳集观看不会把中间没看的集一起放弃；
    - 删除能力在本插件内独立实现（不依赖其他插件），媒体服务器条目删除复用 Emby / Jellyfin 的
      DELETE /Items/{id} 接口；
    - 不注册宿主的 get_service，自己在 init_plugin 里起 BackgroundScheduler，
      保存配置后立刻按新配置重新排程，不必重启宿主。
"""
import datetime
import shutil
import threading
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import pytz
from apscheduler.schedulers.background import BackgroundScheduler

from app.db.oper.subscribe import SubscribeOper
from app.db.oper.transferhistory import TransferHistoryOper
from app.schemas.types import MediaType
from app.sdk.config import settings
from app.sdk.logging import logger
from app.sdk.network import RequestUtils
from app.sdk.plugin import _PluginBase
from app.sdk.services import MediaServerHelper, ServiceConfigHelper

# 宿主定义的「洗版完成」优先级：某集/整部达到该值即不再搜索更高优先级的版本
BEST_VERSION_DONE_PRIORITY = 100
# 宿主对「未设置优先级」的默认取值
UNSET_PRIORITY = 0
# 本插件能读取播放进度并删除条目的媒体服务器类型
SUPPORTED_SERVER_TYPES = ("emby", "jellyfin")
# 判定方式：进度百分比 / 有过播放 / 已看标记
MODE_PERCENTAGE = "percentage"
MODE_ANY_PLAY = "any_play"
MODE_PLAYED = "played"
# 向上清理空目录的最大层数，避免把媒体库根目录一起删掉
MAX_UP_LEVELS = 3
# 路径最少段数：低于该值视为可疑路径，拒绝删除
MIN_PATH_PARTS = 3


class WatchedCleaner(_PluginBase):
    """看完即删插件。

    按媒体服务器上的播放进度，清理已看完的剧集/电影，并前移订阅开始集数避免重复下载。
    """

    # 插件名称
    plugin_name = "看完即删"
    # 插件描述
    plugin_desc = "按媒体服务器的播放进度自动清理已看完的剧集/电影：前移订阅开始集数避免重复下载，" \
                   "可选删除媒体库文件、刮削文件、空目录、媒体服务器条目与整理记录。"
    # 插件图标
    plugin_icon = "watchedcleaner.svg"
    # 插件版本
    plugin_version = "3.1.0"
    # 插件作者
    plugin_author = "xlmc"
    # 作者主页
    author_url = "https://github.com/xlmc"
    # 插件配置项 ID 前缀
    plugin_config_prefix = "watchedcleaner_"
    # 加载顺序
    plugin_order = 32
    # 可使用的用户级别
    auth_level = 1

    # 日志前缀
    LOG_TAG = "[WatchedCleaner] "

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
        # 是否启用
        self._enabled = False
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
        # 是否标记订阅（前移开始集数 / 写洗版优先级），关闭后会因缺失重新下载
        self._mark_subscribe = True
        # 是否删除媒体库文件
        self._delete_media_files = True
        # 是否删除刮削文件
        self._delete_scrap_infos = True
        # 是否删除空目录
        self._delete_empty_folders = True
        # 是否删除媒体服务器条目
        self._delete_mediaserver = True
        # 是否删除整理记录
        self._delete_history = True
        # 是否删除下载目录源文件（会破坏做种）
        self._delete_source_files = False
        # 延迟清理天数：最后播放时间距今不足该天数时跳过
        self._delay_days = 0
        # 试运行：只记录不删除、不写库
        self._dry_run = False
        # 定时调度器
        self._scheduler: Optional[BackgroundScheduler] = None
        # 互斥锁：避免定时与手动触发同时执行
        self._lock = threading.Lock()
        # 最近一次执行结果（详情页与通知用）
        self._last_run: Dict[str, Any] = {}

    # ---- 合并迁移提示（三插件并入「洗版管家」后新增）----

    MIGRATION_TEXT = (
        "本插件已并入「洗版管家」（WashManager）：播放进度洗版、洗版两次、看完即删三项功能合到一个插件里。\n"
        "请到插件市场安装「洗版管家」，把本插件的配置在新插件里重新设置一遍，确认无误后再卸载本插件。\n"
        "本插件保持可用，但不再更新，后续修复与新功能只在「洗版管家」里发布。"
    )

    def _notify_migration(self):
        """升级后首次加载时发一次迁移提示，之后不再打扰。"""
        try:
            if self.get_data("migration_notice"):
                return
            self.post_message(
                title="【迁移提示】本插件已并入「洗版管家」",
                text=self.MIGRATION_TEXT,
            )
            self.save_data("migration_notice", True)
            logger.info(f"{self.LOG_TAG}已发送合并迁移提示")
        except Exception as err:
            logger.error(f"{self.LOG_TAG}发送合并迁移提示失败：{err}")

    def init_plugin(self, config: dict = None):
        """初始化插件：读取配置、按需立即运行一次，并启动定时调度。

        :param config: 插件配置字典，来自宿主的插件配置存储
        """
        # 先停掉旧调度器，避免保存配置后出现两份定时任务
        self.stop_service()

        # 合并迁移提示：升级后首次加载发一次
        self._notify_migration()

        config = config or {}
        self._enabled = bool(config.get("enabled"))
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
        self._mark_subscribe = bool(config.get("mark_subscribe", True))
        self._delete_media_files = bool(config.get("delete_media_files", True))
        self._delete_scrap_infos = bool(config.get("delete_scrap_infos", True))
        self._delete_empty_folders = bool(config.get("delete_empty_folders", True))
        self._delete_mediaserver = bool(config.get("delete_mediaserver", True))
        self._delete_history = bool(config.get("delete_history", True))
        self._delete_source_files = bool(config.get("delete_source_files", False))
        self._delay_days = max(0, self._as_int(config.get("delay_days"), 0))
        self._dry_run = bool(config.get("dry_run"))

        # 恢复上次执行结果，让详情页在重启后仍有内容
        self._last_run = self.get_data("last_run") or {}

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
                    name="看完即删-立即运行一次",
                )
                logger.info(f"{self.LOG_TAG}已注册「立即运行一次」任务：{run_date}")
            self._scheduler.add_job(
                self._run_once,
                "interval",
                minutes=self._interval,
                name="看完即删-定时检测",
            )
            self._scheduler.start()
            logger.info(f"{self.LOG_TAG}定时检测已启动，间隔 {self._interval} 分钟")

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
        """提供「立即执行一次」的插件接口，便于在详情页手动验证口径。"""
        return [
            {
                "path": "/run",
                "endpoint": self.api_run,
                "methods": ["GET"],
                "summary": "立即执行一次检查",
                "description": "按当前配置扫描一遍订阅与播放进度，返回本轮执行摘要",
            }
        ]

    def get_page(self) -> Optional[List[dict]]:
        """插件详情页：展示最近一次执行摘要与逐订阅结果。"""
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
                                    "title": "看完即删",
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
                                            {"title": "标记", "key": "marked"},
                                            {"title": "清理", "key": "deleted"},
                                            {"title": "结果", "key": "status"},
                                        ],
                                        "items": [
                                            {
                                                "title": record.get("title") or "",
                                                "type": record.get("type") or "",
                                                "server": record.get("server") or "",
                                                "watched": record.get("watched") or "",
                                                "marked": record.get("marked") or "",
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

        page.append(
            {
                "component": "VAlert",
                "props": {
                    "type": "warning",
                    "variant": "tonal",
                    "text": "说明：只会清理「从开始集数起连续看完」的内容，遇到第一集没看完就停止。"
                            "清理前会先把订阅的开始集数前移，避免删掉后被判定为缺失而重新下载。"
                            "建议先开启「试运行」只观察日志。",
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
                                            "type": "warning",
                                            "variant": "tonal",
                                            "title": "🗑 看完即删",
                                            "text": "按媒体服务器的播放进度清理已看完的内容。清理前会先把订阅的开始集数"
                                                    "前移到「连续看完的最后一集 + 1」，避免删掉后被判定为缺失重新下载。"
                                                    "本插件会删除文件，建议先开启「试运行」确认口径。",
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
                                    {"component": "VSwitch", "props": {"model": "notify", "label": "发送通知"}}
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
                                        "component": "VTextField",
                                        "props": {
                                            "model": "delay_days",
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
                                    {"component": "VSwitch", "props": {"model": "mark_subscribe", "label": "标记订阅"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "delete_media_files", "label": "删除媒体库文件"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "delete_mediaserver", "label": "删除媒体服务器条目"}}
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
                                    {"component": "VSwitch", "props": {"model": "delete_scrap_infos", "label": "删除刮削文件"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "delete_empty_folders", "label": "删除空目录"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {"component": "VSwitch", "props": {"model": "delete_history", "label": "删除整理记录"}}
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
                                    {"component": "VSwitch", "props": {"model": "delete_source_files", "label": "删除下载目录源文件"}}
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
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
        """执行一轮检查。

        :param force: 手动触发时为 True，可绕过「插件未启用」判断（便于先用试运行观察）
        :return: 本轮执行摘要
        """
        if not self._enabled and not force:
            return {"success": False, "message": "插件未启用", "records": []}
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
                    "cleaned": 0,
                }
            elif not servers:
                summary = {
                    "success": False,
                    "message": "没有可用的 Emby / Jellyfin 媒体服务器",
                    "time": started,
                    "records": [],
                    "cleaned": 0,
                }
            else:
                records = []
                for subscribe in subscribes:
                    try:
                        record = self._process(subscribe, servers)
                    except Exception as err:
                        logger.error(f"{self.LOG_TAG}处理订阅 {self._title(subscribe)} 失败：{err}\n{traceback.format_exc()}")
                        continue
                    if record:
                        records.append(record)
                cleaned = sum(1 for record in records if record.get("deleted"))
                summary = {
                    "success": True,
                    "message": f"检查 {len(records)} 个订阅",
                    "time": started,
                    "records": records,
                    "cleaned": cleaned,
                }
                logger.info(f"{self.LOG_TAG}{summary['message']}，本轮清理 {cleaned} 个订阅")
                if self._notify and cleaned:
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
                start = self._as_int(getattr(subscribe, "start_episode", None), 1) or 1
                total = self._as_int(getattr(subscribe, "total_episode", None), 0)
                # 开始集数已经越过总集数：没有可清理的目标集
                if total and start > total:
                    continue
            result.append(subscribe)
        return result

    def _process(self, subscribe, servers: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """处理单个订阅：找到媒体服务器条目、读取进度、命中则标记并清理。

        :param subscribe: 订阅记录
        :param servers: 可用的媒体服务器列表
        :return: 执行记录（供详情页与通知使用）
        """
        media_type = self._media_type(subscribe)
        record: Dict[str, Any] = {
            "title": self._title(subscribe),
            "type": media_type.value if media_type else "",
            "server": "",
            "watched": "",
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

            hits, new_start, targets = self._plan(subscribe, media_type, playback)
            if not targets:
                record["status"] = "无变化"
                return record

            # 1. 先标记订阅：删掉文件后宿主才不会把它判成缺失重新下载
            if self._mark_subscribe:
                payload = self._mark(subscribe, media_type, hits, new_start)
                if payload:
                    self._update_subscribe(subscribe, payload)
                    record["marked"] = self._describe_marked(media_type, hits, payload)
                else:
                    record["marked"] = "无需标记"
            else:
                record["marked"] = "已关闭"

            # 2. 再清理内容
            deleted = self._delete_targets(server, targets)
            record["deleted"] = self._describe_deleted(deleted)
            if record["deleted"]:
                record["status"] = "已清理" + ("（试运行）" if self._dry_run else "")
            else:
                record["status"] = "无变化"
            return record

        return record

    def _plan(
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

    # ---- 标记订阅 ----

    def _mark(self, subscribe, media_type: MediaType, hits: List[Any], new_start: int) -> Dict[str, Any]:
        """生成要写回订阅的字段。"""
        if media_type == MediaType.TV:
            return self._mark_tv(subscribe, hits, new_start)
        return self._mark_movie(subscribe)

    def _mark_tv(self, subscribe, hits: List[int], new_start: int) -> Dict[str, Any]:
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

    def _mark_movie(self, subscribe) -> Dict[str, Any]:
        """电影：洗版订阅整部标记完成。"""
        if not getattr(subscribe, "best_version", False):
            return {}
        if self._as_int(getattr(subscribe, "current_priority", None), UNSET_PRIORITY) >= BEST_VERSION_DONE_PRIORITY:
            return {}
        return {"current_priority": BEST_VERSION_DONE_PRIORITY}

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

    # ---- 删除 ----

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
                if item_id and self._delete_mediaserver:
                    result["item"].append(label)
                if path and self._delete_media_files:
                    result["file"].append(str(path))
                if path and self._delete_scrap_infos:
                    result["scrap"].append(label)
                if path and self._delete_history:
                    result["history"].append(str(path))
                continue

            # 先删媒体服务器条目：避免宿主扫描到文件缺失后反复刷新
            if item_id and self._delete_mediaserver:
                if self._delete_mediaserver_item(server, item_id, label):
                    result["item"].append(label)

            if path and self._delete_media_files:
                if self._remove_file(path):
                    result["file"].append(str(path))

            if path and self._delete_source_files:
                for source in self._source_paths(path):
                    if self._remove_file(source):
                        result["source"].append(str(source))

            if path and self._delete_scrap_infos:
                result["scrap"].extend(self._delete_scrap_files(path))

            if path and self._delete_empty_folders:
                result["dir"].extend(self._delete_empty_dirs(path))

            if path and self._delete_history:
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

    # ---- 刮削文件与空目录 ----

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
                    # Emby 的接口路径带 emby 前缀，Jellyfin 直接是根路径
                    "prefix": "emby/" if server_type == "emby" else "",
                }
            )

        if not result:
            logger.warning(f"{self.LOG_TAG}没有匹配到可用的 Emby / Jellyfin 服务器（当前配置：{self._servers or '全部'}）")
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
                    f"[HOST]{prefix}/Users/{user_id}/Items/{item['Id']}?Fields=UserData,Path&api_key=[APIKEY]",
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
                f"[HOST]{prefix}/Users/{user_id}/Items?ParentId={item['Id']}&Recursive=true"
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
            f"[HOST]{prefix}/Users/{user_id}/Items?Recursive=true&IncludeItemTypes={item_type}"
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
            logger.debug(f"{WatchedCleaner.LOG_TAG}请求媒体服务器失败：{err}")
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
            logger.warning(f"{WatchedCleaner.LOG_TAG}媒体服务器 {server.get('name')} 缺少地址或密钥，跳过条目清理")
            return False
        url = f"{base_url}{server.get('prefix') or ''}Items/{item_id}"
        try:
            response = RequestUtils().delete_res(url, params={"api_key": apikey})
        except Exception as err:
            logger.error(f"{WatchedCleaner.LOG_TAG}删除媒体服务器条目 {item_id} 出错：{err}")
            return False
        status_code = getattr(response, "status_code", None)
        if status_code in (200, 204):
            logger.info(f"{WatchedCleaner.LOG_TAG}已删除媒体服务器 {server.get('name')} 的条目：{item_id}（{label}）")
            return True
        logger.error(f"{WatchedCleaner.LOG_TAG}删除媒体服务器条目 {item_id} 失败：{status_code or '无响应'}")
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
        """按配置的判定方式判断这一集/这部电影是否算「看完」。"""
        played = bool(progress.get("played"))
        percentage = float(progress.get("percentage") or 0)
        if self._mode == MODE_PLAYED:
            return played
        if self._mode == MODE_ANY_PLAY:
            return played or percentage > 0
        return played or percentage >= self._threshold

    def _delay_ok(self, progress: Dict[str, Any]) -> bool:
        """最后播放时间是否已经超过配置的延迟天数。"""
        if self._delay_days <= 0:
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
        return datetime.datetime.now() - stamp >= datetime.timedelta(days=self._delay_days)

    # ---- 订阅数据口径（与宿主 app/chain/subscribe/completion.py 保持一致） ----

    def _target_episodes(self, subscribe) -> List[int]:
        """订阅的目标集号范围（宿主口径：range(start_episode or 1, total_episode + 1)）。"""
        start = self._as_int(getattr(subscribe, "start_episode", None), 1) or 1
        total = self._as_int(getattr(subscribe, "total_episode", None), 0)
        if total < start:
            return []
        return list(range(start, total + 1))

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

    def _episode_priorities(self, subscribe, target: List[int]) -> Dict[str, int]:
        """取分集洗版优先级，并按宿主口径补齐。

        宿主在 episode_priority 为空且非全集洗版时，会用 current_priority 回退填充所有目标集
        （app/application/subscription/priority.py:get_episode_priority）。这里保持一致：
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

    # ---- 展示与通知 ----

    def _title(self, subscribe) -> str:
        """订阅的展示名：名称（年份）Sxx。"""
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

    def _describe_marked(self, media_type: MediaType, hits: List[Any], payload: Dict[str, Any]) -> str:
        """把写回订阅的动作整理成文字。"""
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

    def _page_summary(self, last: Dict[str, Any]) -> str:
        """详情页顶部摘要。"""
        if not last:
            return "尚未执行过检查。"
        parts = [f"最近一次执行：{last.get('time') or '-'}"]
        if last.get("message"):
            parts.append(str(last["message"]))
        if last.get("cleaned"):
            parts.append(f"清理 {last['cleaned']} 个订阅")
        if self._dry_run:
            parts.append("当前为试运行，未删除任何内容")
        return "，".join(parts)

    def _send_notify(self, records: List[Dict[str, Any]]):
        """把本轮清理结果发到配置的通知渠道。"""
        lines = []
        for record in records:
            if not record.get("deleted"):
                continue
            lines.append(
                f"{record.get('title')}（{record.get('type')}）：清理 {record.get('deleted')}"
                f"，标记 {record.get('marked') or '无'}"
            )
        if not lines:
            return

        text = "\n".join(lines)
        for channel in (self._notify_channel or []):
            try:
                self.post_message(
                    channel=channel,
                    title="【看完即删】已清理看完的内容",
                    text=text,
                )
            except Exception as err:
                logger.error(f"{self.LOG_TAG}发送通知失败（{channel}）：{err}")

    # ---- 配置辅助 ----

    def _current_config(self) -> Dict[str, Any]:
        """当前配置的规范化快照（用于回写与表单默认值）。"""
        return {
            "enabled": self._enabled,
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
            "mark_subscribe": self._mark_subscribe,
            "delete_media_files": self._delete_media_files,
            "delete_scrap_infos": self._delete_scrap_infos,
            "delete_empty_folders": self._delete_empty_folders,
            "delete_mediaserver": self._delete_mediaserver,
            "delete_history": self._delete_history,
            "delete_source_files": self._delete_source_files,
            "delay_days": self._delay_days,
            "dry_run": self._dry_run,
        }

    @staticmethod
    def _as_int(value: Any, default: int) -> int:
        """把配置值转成整数，失败时返回默认值。"""
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
