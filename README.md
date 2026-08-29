# xlmc 的 MoviePilot 插件

[MoviePilot](https://github.com/jxxghp/MoviePilot) V2 插件市场结构的插件仓库，目前包含两个插件：

| 插件 | 版本 | 简介 |
| --- | --- | --- |
| <img src="icons/Qbittorrent_A.png" width="26" align="top"/> **[QB上传限速](#qb上传限速-qbuploadlimiter)** | v1.3.19 | 种子分享率达到阈值后自动限制上传速度，支持 AI 智能限速 |
| <img src="icons/clean.png" width="26" align="top"/> **[源文件联动清理](#源文件联动清理-scrapefileclean)** | v1.0.7 | 手动删除源文件后，自动联动清理硬链接、刮削文件与转移记录 |

## 安装

在 MoviePilot V2 中进入 **设定 → 插件 → 插件市场**，添加本仓库地址后刷新即可安装：

```
https://github.com/xlmc/MoviePilot-Plugins
```

## QB上传限速 QbUploadLimiter

仅处理 MoviePilot 已整理入库成功的种子：分享率达到全局或站点单独阈值后，自动限制该种子的上传速度（支持 qBittorrent / Transmission），停用/卸载自动恢复不限速。

主要特性：

- 入库成功门禁：种子被 MoviePilot 整理入库成功后才参与限速流程，此前完全不干预
- 分享率阈值支持全局与按站点单独设置（最多 1 位小数），可与站点筛选组合使用
- 联动 qBittorrent 全局上传限速，自动取较小值
- 监控超时自动取消并恢复不限速；停用/卸载自动恢复；下载器离线自动兜底重试
- 状态变化通知（限速 / 取消限速 / AI 接管 / AI 取消接管），支持多选通知渠道
- AI 智能限速（可选）：复用 MoviePilot 系统设置的大模型，按种子分享率、上传活跃度与站点账号分享率逐种子智能决策限速，支持每轮复核加限/减限/解限；未配置或调用失败自动回退阈值规则
- AI 生效后点击插件卡片可进入种子状态详情页（统计 + 每种子明细）

详细功能与配置说明见 [plugins.v2/qbuploadlimiter/README.md](plugins.v2/qbuploadlimiter/README.md)。

## 源文件联动清理 ScrapeFileClean

为手动清理下载目录设计：手动删除源文件（下载目录中的原始媒体文件）后，自动联动清理媒体库中对应的硬链接文件、刮削文件（元数据、图片、字幕）与转移记录，保持媒体库与下载目录状态一致。

主要特性：

- 通过 `(dev, inode)` 识别硬链接关系，删除源文件后自动清理媒体库中所有对应硬链接
- 联动清理同名刮削文件（.nfo / 图片 / 字幕等，后缀可自定义）
- 联动删除对应转移记录，避免历史记录残留；可选联动删除种子（配合下载器助手）
- 自动清理只剩刮削文件或完全为空的目录
- 支持延迟删除（防止媒体重整理误删）、排除目录、过滤关键字与通知

> 本插件已收录进 MoviePilot 官方插件市场（[jxxghp/MoviePilot-Plugins](https://github.com/jxxghp/MoviePilot-Plugins)），建议直接从官方市场安装以获取最新版本。

详细功能与配置说明见 [plugins.v2/scrapefileclean/README.md](plugins.v2/scrapefileclean/README.md)。
