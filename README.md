# xlmc 的 MoviePilot 插件

[MoviePilot](https://github.com/jxxghp/MoviePilot) 插件仓库，同时提供 **V2 与 V3 两套实现**与对应的插件市场索引。

## 插件列表

| 插件 | V2 版本 | V3 版本 | 简介 |
| --- | --- | --- | --- |
| <img src="icons/Qbittorrent_A.png" width="26" align="top"/> **[QB上传限速](#qb上传限速-qbuploadlimiter)** | v2.0.0 | v3.1.0 | 种子分享率达到阈值后自动限制上传速度，支持 AI 智能限速 |
| <img src="icons/clean.png" width="26" align="top"/> **[源文件联动清理](#源文件联动清理-scrapefileclean)** | v1.0.7（官方仓库） | v2.0.0 | 手动删除源文件后，自动联动清理硬链接、刮削文件与转移记录 |
| <img src="icons/Emos_A.svg" width="26" align="top"/> **[EMOS上传](#emos上传-emosupload)** | v2.1.0 | v3.0.0 | MoviePilot 下载完成后自动上传源文件到 EMOS 资源站 |
| <img src="icons/playbackbestversion.svg" width="26" align="top"/> **[播放进度洗版](#播放进度洗版-playbackbestversion)** | v2.0.0 | v3.0.0 | 按媒体服务器上的分集播放进度决定是否继续洗版，看过的集不再搜索更高版本 |

## 安装

在 MoviePilot 中把本仓库添加为插件源，并把它**绑定**给需要的插件：

1. 进入 **设定 → 插件市场 → 更换插件源**
2. 在候选列表里选择 **xlmc/MoviePilot-Plugins**，确认更换
3. 刷新插件市场，即可安装或更新

仓库地址：https://github.com/xlmc/MoviePilot-Plugins

> **为什么要「绑定」**：宿主判断某个插件有没有更新时，只比较「本地已安装版本」与**绑定仓库**索引里的版本。如果插件绑定的是别的仓库（例如官方仓库），本仓库发布的更新不会被提示——需要在「更换插件源」里把绑定切到本仓库。
>
> 「添加仓库」与「绑定插件」是两件事：添加只让市场能列出候选，绑定才决定从谁那里取更新。同一个插件在不同仓库有不同版本号时，尤其容易踩到这一点。

## 版本号规则

- V2 实现使用 **2.x**，V3 实现使用 **3.x**（分别对应 plugins.v2/ 与 plugins.v3/ 目录）
- **V3 主版本 = V2 主版本 + 1**：官方仓库门禁 check_plugin_versions.py 按此校验跨代实现的版本跃迁，本仓库同样遵循
- 索引里的 version、插件类里的 plugin_version 与 history 最新一条，三者必须一致

## 目录结构

    MoviePilot-Plugins/
    ├── plugins.v2/            # V2 专用实现（V2 宿主读取）
    ├── plugins.v3/            # V3 专用实现（V3 宿主读取，基于 app.sdk）
    ├── package.v2.json        # V2 插件市场索引
    ├── package.v3.json        # V3 插件市场索引
    ├── icons/                 # 插件图标
    └── README.md

## V2 / V3 双代说明

V3 宿主（MoviePilot >= 3.0.0）读取 package.v3.json 与 plugins.v3/；V2 宿主读取 package.v2.json 与 plugins.v2/。

- V3 实现统一迁移到 **app.sdk** 稳定接口，并通过 **app.db.oper** 访问宿主数据，不再依赖 SessionFactory、宿主 Model 与旧导入路径
- V2 索引里带 **"v3": false** 的条目表示「该插件另有 V3 专用实现」，V3 宿主不会回退使用它——V2 宿主也不会看到 V3 的版本
- 源文件联动清理的 V2 实现由官方插件市场仓库维护，本仓库只分发 V3 实现

## QB上传限速 QbUploadLimiter

仅处理 MoviePilot 已整理入库成功的种子：分享率达到全局或站点单独阈值后，自动限制该种子的上传速度（支持 qBittorrent / Transmission），停用/卸载自动恢复不限速。

主要特性：

- **入库成功门禁**：种子被 MoviePilot 整理入库成功后才参与限速流程，此前完全不干预
- 分享率阈值支持全局与按站点单独设置（最多 1 位小数），可与站点筛选组合使用
- 联动 qBittorrent 全局上传限速，自动取较小值
- **限速有效性判定**：按种子「未被限速时的上传峰值」判断限速是否有意义——连峰值都够不到限速值的种子直接不限速（限速对它是空操作），已限速的会恢复不限速并每轮重新判断，峰值涨上去会被重新限速。刻意不看瞬时速度，避免种子在一段低需求期被误放行后满速跑
- 停用/卸载自动恢复不限速；下载器离线时自动兜底重试
- 状态变化通知（限速 / AI 接管 / AI 取消接管），支持多选通知渠道；同一颗种子的首次限速通知只发送一次（持久化记录，保存配置、更新插件、重启都不会重复推送）
- **AI 智能限速**（可选）：复用 MoviePilot 系统设置里的大模型，按种子分享率、上传活跃度与站点账号分享率逐种子决策限速值；限速值收敛到「该种子实测上传能力砍 2~8 成」的区间并叠加合规下限，上限再与 qB 全局限速、线路实测带宽取小，支持每轮复核加限 / 减限 / 解限；大模型未配置或调用失败时自动回退阈值规则
- AI 生效后点击插件卡片可进入种子状态详情页（状态统计 + 每种子明细）

详细功能与配置说明见 [plugins.v2/qbuploadlimiter/README.md](plugins.v2/qbuploadlimiter/README.md)（V2）与 [plugins.v3/qbuploadlimiter/README.md](plugins.v3/qbuploadlimiter/README.md)（V3）。

## 源文件联动清理 ScrapeFileClean

为手动清理下载目录设计：手动删除源文件（下载目录中的原始媒体文件）后，自动联动清理媒体库中对应的硬链接文件、刮削文件（元数据、图片、字幕）与转移记录，保持媒体库与下载目录状态一致。

主要特性：

- 通过 (dev, inode) 识别硬链接关系，删除源文件后自动清理媒体库中所有对应硬链接
- 联动清理同名刮削文件（.nfo / 图片 / 字幕等，后缀可自定义）
- 联动删除对应转移记录，避免历史记录残留；可选联动删除种子（配合下载器助手）
- 自动清理只剩刮削文件或完全为空的目录
- 支持延迟删除（防止媒体重整理误删）、排除目录、过滤关键字与通知

> V2 版本已收录进 MoviePilot 官方插件市场（[jxxghp/MoviePilot-Plugins](https://github.com/jxxghp/MoviePilot-Plugins)）；V3 版本（v2.0.0，基于官方 1.0.7 生成）由本仓库 plugins.v3/scrapefileclean/ 提供，V3 宿主请从本仓库安装。

详细功能与配置说明见 [plugins.v3/scrapefileclean/README.md](plugins.v3/scrapefileclean/README.md)（本仓库只分发 V3 实现，V2 实现由官方仓库维护）。

## EMOS上传 EmosUpload

MoviePilot 入库完成后自动将源文件上传到 EMOS（Emby 资源站上传分发系统）。通过 EMOS 服务器端接口识别文件并匹配剧集信息，上传前自动对比已有资源版本（分辨率与文件大小），相同版本不重复上传。

主要特性：

- 入库自动触发：监听 MoviePilot 整理完成事件，自动上传源文件
- 服务器端识别：调用 EMOS 识别接口，自动匹配剧集信息
- 版本对比：上传前检查已有资源版本（分辨率 + 文件大小），相同版本跳过
- 分片并发上传：多分片并发上传到 EMOS 存储，最大化吞吐
- 两种上传模式：ask（询问确认）/ silent（静默自动上传）
- 上传历史记录：记录最近 50 条上传记录
- 通知集成：上传结果通过 MoviePilot 通知渠道推送

详细功能与配置说明见 [plugins.v2/emosupload/README.md](plugins.v2/emosupload/README.md)（V2）与 [plugins.v3/emosupload/README.md](plugins.v3/emosupload/README.md)（V3）。

## 播放进度洗版 PlaybackBestVersion

按媒体服务器（Emby / Jellyfin）上的**分集播放进度**决定订阅是否继续洗版：某集已经看过，就不必再为它升级替换。

洗版（best_version）的完成判定由宿主的「分集优先级」驱动——某集的 `episode_priority` 等于 100 即视为该集洗版完成、不再搜索更高版本（电影看 `current_priority`）。插件读取播放进度，把看过的集写成 100，宿主的洗版流程自然跳过它们。插件**只读**媒体服务器、**只写**订阅表：不删除文件、不下载、不触发整理。

主要特性：

- 三种判定方式：播放进度达到阈值（默认 90%）、只要播放过、标记为已看
- 电视剧按集写入（`episode_priority`），电影按整部写入（`current_priority`）
- 只改命中的集并保留既有优先级，同时按宿主口径把整部优先级重算为目标集最小值，不会放宽洗版准入
- 多服务器按顺序匹配（第一个找到条目的服务器生效），同一服务器上的多个用户进度合并取更高
- 试运行模式：只写日志不写库，先看清会发生什么
- 可选通知与定时检测（默认 30 分钟），支持手动「立即运行一次」

详细功能与配置说明见 [plugins.v2/playbackbestversion/README.md](plugins.v2/playbackbestversion/README.md)（V2）与 [plugins.v3/playbackbestversion/README.md](plugins.v3/playbackbestversion/README.md)（V3）。

## 许可

[GPL-3.0](LICENSE)
