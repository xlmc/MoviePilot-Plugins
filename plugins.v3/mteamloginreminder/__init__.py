"""
馒头登录提醒插件（MoviePilot V3）。

M-Team（馒头）的账号删除规则是「未封存账号连续 40 天不登录将被删除」，且站点公告明确
说明「使用第三方工具间接存取不算登入」——MoviePilot 的自动签到、用户数据采集都不能
代替真人登录。本插件调用 M-Team 开放 API 读取账号的真实登录时间（memberStatus.lastLogin），
按用户设定的周期提前提醒登录，避免忘记登录导致掉号。

提醒逻辑：
1. 以账号最近一次真实登录时间为周期起点，默认 30 天为一个周期；
2. 距上次登录达到「周期 - 提前天数」（默认 30 - 7 = 23 天）后，每天早上 8 点提醒一次；
3. 检测到新的登录记录后，发送一条结束通知（告知下一轮提醒日期），本轮不再提醒；
4. 新登录即开启下一个周期，如此循环。
"""

import datetime
import threading
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler

from app.sdk.config import settings
from app.sdk.logging import logger
from app.sdk.network import RequestUtils
from app.sdk.plugin import _PluginBase
from app.schemas.types import NotificationChannel


class MTeamLoginReminder(_PluginBase):
    """
    馒头登录提醒插件。

    读取 M-Team 账号的真实登录时间（lastLogin），按周期提前提醒登录，
    并在检测到新登录后结束本轮提醒。
    """

    plugin_name = "馒头登录提醒"
    plugin_desc = "读取 M-Team 账号的真实登录时间，按周期提前提醒登录：默认 30 天周期、提前 7 天开始，每天早上 8 点提醒一次，检测到新登录后发送结束通知并进入下一周期（自动签到/采集不算登入，必须真人浏览器登录）。"
    plugin_icon = "signin.png"
    plugin_version = "1.0.0"
    plugin_author = "xlmc"
    author_url = "https://github.com/xlmc"
    project_url = "https://github.com/xlmc/MoviePilot-Plugins"
    plugin_config_prefix = "mteamloginreminder_"
    plugin_order = 30
    auth_level = 1

    LOG_TAG = "[馒头登录提醒] "

    # M-Team 站点域名（读取 MoviePilot 站点配置里的 ApiKey）与开放 API
    _MTEAM_DOMAIN = "m-team.cc"
    _PROFILE_API = "https://api.m-team.cc/api/member/profile"
    # 站点规则：未封存账号连续 40 天不登录将被删除（仅用于提醒文案展示）
    _RULE_DAYS = 40
    # 周期起点记录键：保存上一轮周期基于的登录时间，用于识别「新登录」
    _BASELINE_KEY = "cycle_baseline"
    _LAST_NOTIFY_KEY = "last_notify_day"

    # ---- 配置项默认值 ----
    _enabled = False
    _onlyonce = False
    # 已选择的通知渠道类型（如 telegram / wechat），留空表示不发通知
    _notify_channel: List[str] = []
    # 周期天数：距上次登录达到该天数后进入下一轮（默认 30）
    _cycle_days = 30
    # 提前天数：在周期结束前多少天开始提醒（默认 7，即第 23 天起）
    _ahead_days = 7
    # 每天提醒时间（小时，默认 8 点）
    _remind_hour = 8

    # 通知渠道类型（MoviePilot 通知配置的 type）-> NotificationChannel 枚举
    _NOTIFY_TYPE_MAP = {
        "telegram": NotificationChannel.Telegram,
        "wechat": NotificationChannel.Wechat,
        "feishu": NotificationChannel.Feishu,
        "dingding": NotificationChannel.Dingding,
        "wecom": NotificationChannel.Wecom,
        "pushplus": NotificationChannel.PushPlus,
        "serverchan": NotificationChannel.ServerChan,
        "qqbot": NotificationChannel.QQ,
        "bark": NotificationChannel.Bark,
        "ntfy": NotificationChannel.Ntfy,
        "gotify": NotificationChannel.Gotify,
        "slack": NotificationChannel.Slack,
        "synologychat": NotificationChannel.SynologyChat,
        "vocechat": NotificationChannel.VoceChat,
        "webpush": NotificationChannel.WebPush,
    }

    def __init__(self):
        """初始化插件运行态。"""
        super().__init__()
        self._scheduler: Optional[BackgroundScheduler] = None
        self._running = False
        self._last_result = ""

    def init_plugin(self, config: Optional[dict] = None):
        """读取配置并启动每日定时提醒任务。"""
        self.stop_plugin()
        config = config or self._current_config()
        self._enabled = bool(config.get("enabled"))
        self._onlyonce = bool(config.get("onlyonce"))
        self._notify_channel = self._normalize_channels(config.get("notify_channel"))
        self._cycle_days = max(self._to_int(config.get("cycle_days"), 30), 1)
        self._ahead_days = min(max(self._to_int(config.get("ahead_days"), 7), 0), self._cycle_days)
        self._remind_hour = min(max(self._to_int(config.get("remind_hour"), 8), 0), 23)

        # 手动运行一次：立即执行一轮检测
        if self._onlyonce:
            self._onlyonce = False
            self.update_config(self._current_config())
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self._run,
                trigger="date",
                run_date=datetime.datetime.now(tz=pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=3),
                kwargs={"manual": True},
                name="立即检测馒头登录时间",
            )
            self._scheduler.start()

        # 启用插件：每天指定整点提醒一次
        if self._enabled:
            self._scheduler = self._scheduler or BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self._run,
                trigger="cron",
                hour=self._remind_hour,
                minute=0,
                kwargs={"manual": False},
                name="每日馒头登录提醒",
            )
            if not self._scheduler.running:
                self._scheduler.start()

    def get_state(self) -> bool:
        """返回插件启用状态。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """本插件不提供远程命令。"""
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """本插件不提供 API。"""
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        插件设置表单：
        第一行：启用插件 / 立即运行一次 / 发送通知（多选渠道）；
        第二行：周期天数 / 提前天数 / 每天提醒时间（小时）。
        """
        notify_items = [{"title": title, "value": value} for value, title in self._notify_channel_options()]
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "enabled", "label": "启用插件"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "onlyonce", "label": "立即运行一次"},
                                    }
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
                                            "label": "发送通知",
                                            "items": notify_items,
                                            "multiple": True,
                                            "chips": True,
                                            "clearable": True,
                                            "placeholder": "留空表示不发送通知",
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
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cycle_days",
                                            "label": "周期天数",
                                            "placeholder": "默认 30 天",
                                            "type": "number",
                                            "min": 1,
                                            "step": 1,
                                            "hide-spin-buttons": True,
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
                                            "model": "ahead_days",
                                            "label": "提前提醒天数",
                                            "placeholder": "默认 7 天",
                                            "type": "number",
                                            "min": 0,
                                            "step": 1,
                                            "hide-spin-buttons": True,
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
                                            "model": "remind_hour",
                                            "label": "每天提醒时间（小时）",
                                            "placeholder": "默认 8（即每天 8:00）",
                                            "type": "number",
                                            "min": 0,
                                            "max": 23,
                                            "step": 1,
                                            "hide-spin-buttons": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], self._current_config()

    def _current_config(self) -> Dict[str, Any]:
        """返回当前配置，供表单回填。"""
        return {
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "notify_channel": self._notify_channel,
            "cycle_days": self._cycle_days,
            "ahead_days": self._ahead_days,
            "remind_hour": self._remind_hour,
        }

    def stop_plugin(self):
        """停止调度任务。"""
        if self._scheduler:
            try:
                if self._scheduler.running:
                    self._scheduler.shutdown(wait=False)
            except Exception as err:
                logger.error(f"{self.LOG_TAG}停止定时任务失败：{err}")
            self._scheduler = None

    # ---------------------------------------------------------------- 核心逻辑

    def _run(self, manual: bool = False):
        """执行一轮检测：读取真实登录时间，判断是否提醒 / 是否结束本轮提醒。"""
        if self._running:
            logger.info(f"{self.LOG_TAG}上一轮检测仍在执行，跳过本次")
            return
        self._running = True
        try:
            apikey = self._resolve_apikey()
            if not apikey:
                self._last_result = "未在 MoviePilot 站点配置中找到 M-Team 的 ApiKey"
                logger.error(f"{self.LOG_TAG}{self._last_result}（设定 → 站点 → 馒头 → ApiKey）")
                return
            profile = self._fetch_profile(apikey)
            if not profile:
                self._last_result = "调用 M-Team API 失败，本轮跳过"
                logger.error(f"{self.LOG_TAG}{self._last_result}")
                return
            member_status = profile.get("memberStatus") or {}
            last_login = str(member_status.get("lastLogin") or "").strip()
            if not last_login:
                self._last_result = "M-Team 返回中没有 lastLogin 字段"
                logger.error(f"{self.LOG_TAG}{self._last_result}")
                return
            last_browse = str(member_status.get("lastBrowse") or "").strip()
            self._evaluate(last_login, last_browse, manual=manual)
        except Exception as err:
            logger.error(f"{self.LOG_TAG}检测异常：{err}")
        finally:
            self._running = False

    def _evaluate(self, last_login: str, last_browse: str, manual: bool = False):
        """比对周期起点，决定发提醒、发结束通知还是静默。"""
        now = datetime.datetime.now(tz=pytz.timezone(settings.TZ))
        baseline = str(self.get_data(self._BASELINE_KEY) or "").strip()
        today = now.strftime("%Y-%m-%d")

        # 第一次运行：以当前登录时间作为周期起点
        if not baseline:
            self.save_data(self._BASELINE_KEY, last_login)
            days = self._days_since(last_login, now)
            self._last_result = f"已记录周期起点：{last_login}（距今 {days} 天）"
            logger.info(f"{self.LOG_TAG}{self._last_result}")
            if days >= self._remind_from_day():
                self._notify_remind(last_login, last_browse, days, now)
            return

        # 检测到新登录：结束本轮提醒，并告知下一轮提醒日期
        if last_login != baseline:
            self.save_data(self._BASELINE_KEY, last_login)
            self.save_data(self._LAST_NOTIFY_KEY, "")
            days = self._days_since(last_login, now)
            next_day = self._next_remind_date(last_login)
            self._last_result = f"检测到新登录 {last_login}，本轮提醒结束"
            logger.info(f"{self.LOG_TAG}{self._last_result}（下一轮提醒起始日 {next_day}）")
            self._notify_recovered(last_login, next_day, now)
            return

        days = self._days_since(last_login, now)
        if days < self._remind_from_day():
            self._last_result = f"距上次登录 {days} 天，未到提醒时间（第 {self._remind_from_day()} 天起提醒）"
            logger.info(f"{self.LOG_TAG}{self._last_result}")
            return

        # 每天只提醒一次（定时任务本身每天执行一次，这里再兜一层防止重复运行）
        if str(self.get_data(self._LAST_NOTIFY_KEY) or "") == today and not manual:
            self._last_result = f"今日已提醒过（距上次登录 {days} 天）"
            logger.info(f"{self.LOG_TAG}{self._last_result}")
            return
        self.save_data(self._LAST_NOTIFY_KEY, today)
        self._notify_remind(last_login, last_browse, days, now)

    def _remind_from_day(self) -> int:
        """返回开始提醒的天数阈值（周期天数 - 提前天数）。"""
        return max(self._cycle_days - self._ahead_days, 0)

    def _next_remind_date(self, last_login: str) -> str:
        """按给定登录时间推算下一轮提醒的起始日期。"""
        try:
            start = datetime.datetime.strptime(last_login, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return "未知"
        return (start + datetime.timedelta(days=self._remind_from_day())).strftime("%Y-%m-%d")

    @staticmethod
    def _days_since(last_login: str, now: datetime.datetime) -> int:
        """返回距上次登录经过的天数（解析失败返回 0）。"""
        try:
            start = datetime.datetime.strptime(last_login, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return 0
        return max((now.replace(tzinfo=None) - start).days, 0)

    def _fetch_profile(self, apikey: str) -> Optional[dict]:
        """调用 M-Team 开放 API 获取账号资料。"""
        try:
            res = RequestUtils(
                headers={"x-api-key": apikey, "Content-Type": "application/json"}, timeout=30
            ).post_json(
                url=self._PROFILE_API, json={}
            )
        except Exception as err:
            logger.error(f"{self.LOG_TAG}调用 M-Team API 异常：{err}")
            return None
        if not isinstance(res, dict):
            logger.error(f"{self.LOG_TAG}M-Team API 返回非 JSON 数据")
            return None
        if str(res.get("code")) != "0":
            logger.error(f"{self.LOG_TAG}M-Team API 返回错误：{res.get('message')}")
            return None
        data = res.get("data")
        return data if isinstance(data, dict) else None

    def _resolve_apikey(self) -> Optional[str]:
        """从 MoviePilot 站点配置中读取 M-Team 的 ApiKey。"""
        try:
            from app.db.oper.site import SiteOper

            site = SiteOper().get_by_domain(self._MTEAM_DOMAIN)
            apikey = str(getattr(site, "apikey", "") or "").strip()
            if apikey:
                return apikey
        except Exception as err:
            logger.error(f"{self.LOG_TAG}读取站点配置失败：{err}")
        return None

    # ---------------------------------------------------------------- 通知

    def _notify_remind(self, last_login: str, last_browse: str, days: int, now: datetime.datetime):
        """发送每日提醒。"""
        remain = max(self._cycle_days - days, 0)
        overdue = days - self._cycle_days
        if overdue > 0:
            tail = f"已超过 {self._cycle_days} 天周期 {overdue} 天"
        else:
            tail = f"距 {self._cycle_days} 天周期还剩 {remain} 天"
        browse_line = f"（最近浏览 {last_browse}）" if last_browse else ""
        text = (
            f"**馒头**账号上次真实登录：**{last_login}**，距今 **{days}** 天，{tail}。\n\n"
            f"站点规则：未封存账号连续 **{self._RULE_DAYS}** 天不登录将被删除；"
            f"自动签到/第三方工具访问**不算登入**，请用浏览器登录一次。"
            f"{browse_line}"
        )
        self._last_result = f"已发送提醒（距上次登录 {days} 天，{tail}）"
        logger.info(f"{self.LOG_TAG}{self._last_result}")
        self._send("【馒头登录提醒】该登录了", text)

    def _notify_recovered(self, last_login: str, next_day: str, now: datetime.datetime):
        """发送「已检测到新登录，本轮不再通知」的结束通知。"""
        text = (
            f"已检测到馒头账号的新登录记录：**{last_login}**。\n\n"
            f"本轮提醒结束，**不再通知**。\n"
            f"下一个 {self._cycle_days} 天周期将于 **{next_day}** 起再次提醒"
            f"（提前 {self._ahead_days} 天）。"
        )
        self._last_result = f"已发送结束通知（新登录 {last_login}）"
        logger.info(f"{self.LOG_TAG}{self._last_result}")
        self._send("【馒头登录提醒】已恢复，不再通知", text)

    def _send(self, title: str, text: str):
        """按已选渠道发送通知。"""
        channels = self._notify_channel
        if not channels:
            logger.info(f"{self.LOG_TAG}未选择通知渠道，仅记录日志")
            return
        link = settings.MP_DOMAIN("#/plugins?tab=installed&id=MTeamLoginReminder")
        for channel in channels:
            notify_channel = self._NOTIFY_TYPE_MAP.get(channel)
            if not notify_channel:
                continue
            try:
                self.post_message(channel=notify_channel, title=title, text=text, link=link)
            except Exception as err:
                logger.error(f"{self.LOG_TAG}发送通知失败（{channel}）：{err}")

    # ---------------------------------------------------------------- 工具

    @staticmethod
    def _notify_channel_options() -> List[Tuple[str, str]]:
        """返回可选通知渠道（类型, 展示名）。"""
        return [
            ("telegram", "Telegram"),
            ("wechat", "微信"),
            ("feishu", "飞书"),
            ("dingding", "钉钉"),
            ("wecom", "企业微信"),
            ("pushplus", "PushPlus"),
            ("serverchan", "Server酱"),
            ("qqbot", "QQ"),
            ("bark", "Bark"),
            ("ntfy", "Ntfy"),
            ("gotify", "Gotify"),
            ("slack", "Slack"),
            ("synologychat", "Synology Chat"),
            ("vocechat", "VoceChat"),
            ("webpush", "WebPush"),
        ]

    @staticmethod
    def _normalize_channels(value: Any) -> List[str]:
        """归一化通知渠道配置为去重后的字符串列表。"""
        if not value:
            return []
        if isinstance(value, str):
            value = [value]
        result: List[str] = []
        for item in value:
            name = str(item or "").strip()
            if name and name not in result:
                result.append(name)
        return result

    @staticmethod
    def _to_int(value: Any, default: int) -> int:
        """把配置值转换为整数，非法值返回默认值。"""
        if isinstance(value, bool):
            return default
        try:
            return int(str(value).strip())
        except Exception:
            return default
