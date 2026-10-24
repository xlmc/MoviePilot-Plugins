# -*- coding: utf-8 -*-
"""播放进度洗版守卫（MoviePilot V3）

功能：
    按媒体服务器（Emby / Jellyfin）上的播放进度，决定一个订阅是否还要继续洗版。

    - 电视剧：某个目标集已经看过（判定方式见配置 mode），就把该集的洗版优先级写成 100，
      这是宿主定义的「该集洗版完成」口径（app/application/subscription/priority.py），
      后续洗版搜索不会再为这一集下载更高优先级的版本；
    - 电影：已经看过就把 current_priority 写成 100，整部停止洗版；
    - 全集洗版（best_version_full）：宿主按整部判断完成，因此只有目标集全部看过才写 100。

    这样「已经看完的内容」不会被反复升级替换：省带宽、省磁盘，也不会把用户已经看过的
    剧集重洗成另一个版本。

工作方式：
    1. 定时（默认 30 分钟）或手动触发一次；
    2. 从订阅库取「订阅中（state=R）+ 已开启洗版」的订阅；
    3. 用 MediaServerHelper 取启用的 Emby/Jellyfin 实例，按订阅名称/年份/TMDB ID 在服务器上找到对应条目；
    4. 读取条目下每一集的 UserData（Played / PlaybackPositionTicks / PlayedPercentage）；
    5. 命中的集写回订阅的 episode_priority（保留既有值），并按宿主口径重算 current_priority；
    6. 可选发送通知，并在插件详情页展示最近一次执行结果。

设计说明：
    - 判定口径全部可配置：percentage（默认，进度 >= 阈值）/ any_play（进度 > 0）/ played（已看标记）；
    - 支持「试运行」：只写日志不写库，确认口径无误后再正式启用；
    - 只读媒体服务器、只写订阅表，不删除、不下载任何内容；
    - 不注册宿主的 get_service，自己在 init_plugin 里起 BackgroundScheduler，
      保存配置后立刻按新配置重新排程，不必重启宿主。
"""
import datetime
import threading
import traceback
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import pytz
from apscheduler.schedulers.background import BackgroundScheduler

from app.db.oper.subscribe import SubscribeOper
from app.schemas.types import MediaType
from app.sdk.config import settings
from app.sdk.logging import logger
from app.sdk.plugin import _PluginBase
from app.sdk.services import MediaServerHelper, ServiceConfigHelper

# 宿主定义的「洗版完成」优先级：某集/整部达到该值即不再搜索更高优先级的版本
BEST_VERSION_DONE_PRIORITY = 100
# 宿主对「未设置优先级」的默认取值
UNSET_PRIORITY = 0
# 本插件能读取播放进度的媒体服务器类型（客户端都提供 get_data / get_user）
SUPPORTED_SERVER_TYPES = ("emby", "jellyfin")
# 判定方式：进度百分比 / 有过播放 / 已看标记
MODE_PERCENTAGE = "percentage"
MODE_ANY_PLAY = "any_play"
MODE_PLAYED = "played"


class PlaybackAwareBestVersion(_PluginBase):
    """播放进度洗版守卫插件。

    按媒体服务器上的播放进度决定订阅是否继续洗版：已看过的剧集/电影不再重复洗版。
    """

    # 插件名称
    plugin_name = "播放进度洗版守卫"
    # 插件描述
    plugin_desc = "按媒体服务器的播放进度决定订阅是否继续洗版：已看过的剧集/电影不再重复洗版。"
    # 插件图标
    plugin_icon = "playbackawarebestversion.svg"
    # 插件版本
    plugin_version = "3.0.0"
    # 插件作者
    plugin_author = "xlmc"
    # 作者主页
    author_url = "https://github.com/xlmc"
    # 插件配置项 ID 前缀
    plugin_config_prefix = "playbackawarebestversion_"
    # 加载顺序
    plugin_order = 31
    # 可使用的用户级别
    auth_level = 1

    # 日志前缀
    LOG_TAG = "[PlaybackAwareBestVersion] "

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
        # 试运行：只记录不写库
        self._dry_run = False
        # 定时调度器
        self._scheduler: Optional[BackgroundScheduler] = None
        # 互斥锁：避免定时与手动触发同时执行
        self._lock = threading.Lock()
        # 最近一次执行结果（详情页与通知用）
        self._last_run: Dict[str, Any] = {}

    def init_plugin(self, config: dict = None):
        """初始化插件：读取配置、按需立即运行一次，并启动定时调度。

        :param config: 插件配置字典，来自宿主的插件配置存储
        """
        # 先停掉旧调度器，避免保存配置后出现两份定时任务
        self.stop_service()

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
                    name="播放进度洗版守卫-立即运行一次",
                )
                logger.info(f"{self.LOG_TAG}已注册「立即运行一次」任务：{run_date}")
            self._scheduler.add_job(
                self._run_once,
                "interval",
                minutes=self._interval,
                name="播放进度洗版守卫-定时检测",
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
                                    "title": "播放进度洗版守卫",
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
                                            {"title": "季", "key": "season"},
                                            {"title": "服务器", "key": "server"},
                                            {"title": "播放进度", "key": "watched"},
                                            {"title": "跳过洗版", "key": "changed"},
                                            {"title": "结果", "key": "status"},
                                        ],
                                        "items": [
                                            {
                                                "title": record.get("title") or "",
                                                "type": record.get("type") or "",
                                                "season": record.get("season") or "",
                                                "server": record.get("server") or "",
                                                "watched": record.get("watched") or "",
                                                "changed": self._describe_changed(record.get("changed")),
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
                    "text": "说明：命中的集会被写入订阅的分集洗版优先级 100（宿主口径：该集洗版完成），"
                            "后续洗版搜索不再为它下载更高优先级的版本。可先开启「试运行」只观察日志。",
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

    def get_form(self) -> List[dict]:
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
                                            "title": "⏯ 播放进度洗版守卫",
                                            "text": "按媒体服务器的播放进度决定订阅是否继续洗版：已看过的剧集不再为它洗版，"
                                                    "已看过的电影整部停止洗版。只读媒体服务器、只写订阅表，"
                                                    "不删除、不下载任何内容。",
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
            subscribes = self._get_best_version_subscribes()
            servers = self._get_servers()
            if not subscribes:
                summary: Dict[str, Any] = {
                    "success": True,
                    "message": "没有需要检查的洗版订阅",
                    "time": started,
                    "records": [],
                    "changed": 0,
                }
            elif not servers:
                summary = {
                    "success": False,
                    "message": "没有可用的 Emby / Jellyfin 媒体服务器",
                    "time": started,
                    "records": [],
                    "changed": 0,
                }
            else:
                records = []
                for subscribe in subscribes:
                    record = self._process(subscribe, servers)
                    if record:
                        records.append(record)
                changed = sum(len(record.get("changed") or []) for record in records)
                summary = {
                    "success": True,
                    "message": f"检查 {len(records)} 个订阅",
                    "time": started,
                    "records": records,
                    "changed": changed,
                }
                logger.info(f"{self.LOG_TAG}{summary['message']}，本轮共写入 {changed} 个跳过标记")
                if self._notify and changed:
                    self._send_notify(records)

            self._last_run = summary
            self.save_data("last_run", summary)
            return summary
        except Exception as err:
            logger.error(f"{self.LOG_TAG}检查失败：{err}\n{traceback.format_exc()}")
            return {"success": False, "message": f"检查失败：{err}", "time": started, "records": []}
        finally:
            self._lock.release()

    def _get_best_version_subscribes(self) -> List[Any]:
        """取「订阅中 + 已开启洗版 + 尚未完成」的订阅。"""
        try:
            subscribes = SubscribeOper().list(state="R") or []
        except Exception as err:
            logger.error(f"{self.LOG_TAG}读取订阅失败：{err}")
            return []

        result = []
        for subscribe in subscribes:
            if not getattr(subscribe, "best_version", False):
                continue
            media_type = self._media_type(subscribe)
            if media_type is None:
                continue
            if media_type == MediaType.TV and not self._handle_tv:
                continue
            if media_type == MediaType.MOVIE and not self._handle_movie:
                continue
            if self._is_done(subscribe, media_type):
                continue
            result.append(subscribe)
        return result

    def _process(self, subscribe, servers: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """处理单个订阅：找到媒体服务器条目、读取进度、命中则写库。

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
            changed = self._apply(subscribe, media_type, playback)
            if changed:
                record["changed"] = changed
                record["status"] = "已跳过" + ("（试运行）" if self._dry_run else "")
            else:
                record["status"] = "无变化"
            return record

        return record

    def _apply(self, subscribe, media_type: MediaType, playback: Dict[str, Any]) -> List[Any]:
        """把命中结果写回订阅。

        :return: 本次写入的跳过标记（剧集号列表，或 ["整部"]），无变化时返回空列表
        """
        if media_type == MediaType.TV:
            return self._apply_tv(subscribe, playback.get("episodes") or {})
        return self._apply_movie(subscribe, playback.get("movie") or {})

    def _apply_tv(self, subscribe, episodes: Dict[int, Dict[str, Any]]) -> List[int]:
        """电视剧：把已看过的集写成洗版完成优先级。

        :param subscribe: 订阅记录
        :param episodes: 集号 -> 播放进度
        :return: 本次写入的集号列表
        """
        target = self._target_episodes(subscribe)
        if not target:
            logger.info(f"{self.LOG_TAG}{self._title(subscribe)} 缺少集数信息，跳过")
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
        # （app/application/subscription/priority.py:get_best_version_current_priority）
        new_priority = min(
            (priorities.get(str(episode), UNSET_PRIORITY) for episode in target),
            default=UNSET_PRIORITY,
        )
        self._update_subscribe(
            subscribe,
            {"episode_priority": priorities, "current_priority": new_priority},
        )
        return changed

    def _apply_movie(self, subscribe, movie: Dict[str, Any]) -> List[str]:
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

    # ---- 媒体服务器读取 ----

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
            result.append({"name": name, "type": server_type, "client": client})

        if not result:
            logger.warning(f"{self.LOG_TAG}没有匹配到可用的 Emby / Jellyfin 服务器（当前配置：{self._servers or '全部'}）")
        return result

    def _fetch_playback(self, server: Dict[str, Any], subscribe, media_type: MediaType) -> Dict[str, Any]:
        """读取订阅对应条目在媒体服务器上的播放进度。

        :param server: 媒体服务器信息（name / type / client）
        :param subscribe: 订阅记录
        :param media_type: 媒体类型
        :return: {"status": "ok"|"not_found"|"no_user", "episodes"|"movie": ...}
        """
        client = server["client"]
        # Emby 客户端的内部路径带 emby/ 前缀，Jellyfin 不带
        prefix = "emby" if server["type"] == "emby" else ""
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
                    f"[HOST]{prefix}/Users/{user_id}/Items/{item['Id']}?Fields=UserData&api_key=[APIKEY]",
                )
                progress = self._progress_of(data or {})
                if progress["percentage"] > float(movie.get("percentage") or 0) \
                        or (progress["played"] and not movie.get("played")):
                    movie = progress
            return {"status": "ok", "movie": movie}

        episodes: Dict[int, Dict[str, Any]] = {}
        for user_id in user_ids:
            data = self._get_json(
                client,
                f"[HOST]{prefix}/Users/{user_id}/Items?ParentId={item['Id']}&Recursive=true"
                f"&IncludeItemTypes=Episode&Fields=UserData,IndexNumber,ParentIndexNumber&api_key=[APIKEY]",
            )
            for episode_item in (data or {}).get("Items") or []:
                if not self._season_match(subscribe, episode_item):
                    continue
                number = self._as_int(episode_item.get("IndexNumber"), 0)
                if number <= 0:
                    continue
                progress = self._progress_of(episode_item)
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
            logger.debug(f"{PlaybackAwareBestVersion.LOG_TAG}请求媒体服务器失败：{err}")
            return None
        if response is None:
            return None
        try:
            return response.json()
        except Exception:
            return None

    @staticmethod
    def _progress_of(item: Dict[str, Any]) -> Dict[str, Any]:
        """把 Emby/Jellyfin 的 UserData 归一化成 {played, percentage}。"""
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
        return {"played": played, "percentage": round(percentage, 1)}

    def _hit(self, progress: Dict[str, Any]) -> bool:
        """按配置的判定方式判断这一集/这部电影是否算「看过」。"""
        played = bool(progress.get("played"))
        percentage = float(progress.get("percentage") or 0)
        if self._mode == MODE_PLAYED:
            return played
        if self._mode == MODE_ANY_PLAY:
            return played or percentage > 0
        return played or percentage >= self._threshold

    # ---- 订阅数据口径（与宿主 app/application/subscription/priority.py 保持一致） ----

    def _target_episodes(self, subscribe) -> List[int]:
        """订阅的目标集号范围（宿主口径：range(start_episode or 1, total_episode + 1)）。"""
        start = self._as_int(getattr(subscribe, "start_episode", None), 1) or 1
        total = self._as_int(getattr(subscribe, "total_episode", None), 0)
        if total < start:
            return []
        return list(range(start, total + 1))

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

    def _is_done(self, subscribe, media_type: MediaType) -> bool:
        """订阅是否已经洗版完成（完成的不必再检查）。"""
        if media_type == MediaType.TV and not getattr(subscribe, "best_version_full", False):
            target = self._target_episodes(subscribe)
            if not target:
                # 宿主在拿不到目标集时回退成整部优先级判断
                return self._as_int(getattr(subscribe, "current_priority", None), UNSET_PRIORITY) \
                    >= BEST_VERSION_DONE_PRIORITY
            priorities = self._episode_priorities(subscribe, target)
            return all(
                priorities.get(str(episode), UNSET_PRIORITY) >= BEST_VERSION_DONE_PRIORITY
                for episode in target
            )
        return self._as_int(getattr(subscribe, "current_priority", None), UNSET_PRIORITY) \
            >= BEST_VERSION_DONE_PRIORITY

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

    @staticmethod
    def _describe_changed(changed) -> str:
        """把写入的跳过标记整理成文字。"""
        if not changed:
            return ""
        if changed == ["整部"]:
            return "整部"
        return "、".join(f"E{episode}" for episode in changed)

    def _page_summary(self, last: Dict[str, Any]) -> str:
        """详情页顶部摘要。"""
        if not last:
            return "尚未执行过检查。"
        parts = [f"最近一次执行：{last.get('time') or '-'}"]
        if last.get("message"):
            parts.append(str(last["message"]))
        if last.get("changed"):
            parts.append(f"写入 {last['changed']} 个跳过标记")
        if self._dry_run:
            parts.append("当前为试运行，未写入任何订阅")
        return "，".join(parts)

    def _send_notify(self, records: List[Dict[str, Any]]):
        """把本轮写入的跳过标记发到配置的通知渠道。"""
        lines = []
        for record in records:
            changed = record.get("changed") or []
            if not changed:
                continue
            lines.append(
                f"{record.get('title')}（{record.get('type')}）：已跳过 "
                f"{self._describe_changed(changed)} 的洗版"
            )
        if not lines:
            return

        text = "\n".join(lines)
        for channel in (self._notify_channel or []):
            try:
                self.post_message(
                    channel=channel,
                    title="【播放进度洗版守卫】已跳过已看内容的洗版",
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
