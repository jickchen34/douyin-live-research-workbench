# 抖音直播素材研究台

> 个人研究自用项目。禁止传播、转售、部署为公开服务或用于批量爬取、骚扰、侵权、绕过平台限制等用途。
>
> 本仓库公开仅用于个人备份和代码管理，不授予任何第三方使用、复制、分发或商用授权。仓库中不包含密钥、Cookie、数据库、视频、音频、日志或采集结果。

这个项目用于把“对标账号视频 -> 作品数据和高赞评论 -> 视频转写 -> AI 爆款分析 -> 本地素材库”跑成一个本地工作台。

## 当前完整度

项目主链路已经可以本地运行：

- 输入抖音主页链接，程序化采集账号作品、统计数据、高赞评论和媒体文件。
- 支持按账号分类、最低点赞数、点赞 Top N、高赞评论数量等参数采集；页数和每页数量默认使用 100/100 以尽量拉完整作品列表。
- 采集页支持选择已有分类，也支持自定义分类。
- 可分别勾选“转录”“分析”，采集完成后自动对本次采集视频先转录，再按需分析。
- 使用 `ffmpeg` 抽取音频，并调用火山豆包语音 ASR 转录视频口播。
- 调用火山方舟豆包模型，使用 `VOLCENGINE_ENDPOINT_ID` 作为模型/接入点 ID 做爆款分析。
- 前端支持工作台、数据查看、信息页面、失败任务和运行日志。
- 支持批量转录、批量分析、失败手动重试、标记无口播、单条/批量删除。
- 信息页面可查看 AI 分析、高赞评论、转录文本；直播可聊点按“主题 / 观点”展示。
- 将视频、评论、转写、分析结果写入本地 SQLite 数据库。

更完整的从零安装说明见 [SETUP.md](SETUP.md)。

## 重要限制

- 不提供、不鼓励、不支持绕过登录、验证码、签名、风控或平台限制。
- 只应处理你有权查看和研究的公开内容或授权内容。
- 不要将本项目部署为公开服务。
- 不要提交 `.env`、Cookie、数据库、视频、音频、日志或采集结果。
- 公开仓库无法技术上阻止他人下载，所以请不要传播仓库链接。

## 目录结构

```text
douyin_live_research/
  app_logging.py              # 本地日志系统
  cli.py                      # 旧命令入口和导出入口
  db.py                       # SQLite 表结构和读写
  doubao_asr.py               # 火山豆包语音 ASR
  douyin_creator_harvest.py   # 抖音账号采集
  pipeline.py                 # 单条视频旧流水线
  server.py                   # 本地前端/API 服务
  volcengine.py               # 火山方舟调用封装
  web/index.html              # 本地前端控制台
  scripts/check_setup.py      # 本地环境检查脚本
  SETUP.md                    # 从零安装和配置说明
  .env.example                # 配置模板，不含真实密钥
  requirements.txt            # 本项目 Python 依赖
```

运行时会生成这些本地目录，但它们不会提交到 Git：

```text
data/                         # 数据库、视频、音频、ASR 结果、分析结果
logs/                         # 运行日志
external/Douyin_TikTok_Download_API/  # 本地克隆的外部采集依赖
web/library.json              # 前端素材缓存
```

## 外部依赖

账号采集依赖本地克隆的开源项目：

```bash
mkdir -p external
git clone https://github.com/Evil0ctal/Douyin_TikTok_Download_API.git external/Douyin_TikTok_Download_API
```

克隆后还需要安装它自己的依赖：

```bash
pip install -r external/Douyin_TikTok_Download_API/requirements.txt
```

该目录已被 `.gitignore` 忽略，不会进入本仓库。

系统依赖：

- Python 3.10+
- `git`
- `curl`
- `ffmpeg`
- 可访问 GitHub、抖音公开网页接口、火山方舟、豆包语音 ASR、音频上传服务的网络环境

macOS 可用 Homebrew 安装系统依赖：

```bash
brew install ffmpeg
```

## 配置

复制配置模板：

```bash
cp .env.example .env
```

在 `.env` 中填写你自己的配置：

```bash
VOLCENGINE_API_KEY='你的火山方舟 API Key'
VOLCENGINE_ENDPOINT_ID='你的火山方舟接入点 ID'
DOUBAO_ASR_API_KEY='你的豆包语音 ASR API Key'
```

豆包 ASR 默认使用标准录音文件识别模式：

```bash
DOUBAO_ASR_API_MODE=standard
DOUBAO_ASR_UPLOAD_PROVIDER=mp3tourl
```

这意味着系统会先把抽取出的 MP3 上传到第三方临时音频托管服务，再把公网 URL 提交给豆包 ASR。如果该服务不可用，可改用：

```bash
DOUBAO_ASR_UPLOAD_PROVIDER=bashupload
```

或自行配置公网音频地址：

```bash
DOUBAO_ASR_PUBLIC_BASE_URL=https://your-public-host.example.com/audio
```

如果你的豆包语音 ASR 账号使用旧版认证，需要配置：

```bash
DOUBAO_ASR_AUTH_MODE=legacy
DOUBAO_ASR_APP_KEY=your_app_key
DOUBAO_ASR_ACCESS_KEY=your_access_key
```

不要把真实 key 写进代码或提交到 Git。

批量转录和批量分析默认使用保守并发，避免豆包 ASR 或模型接口被打满。需要调整时可在 `.env` 中配置，服务端会限制在 1 到 4 之间：

```bash
BATCH_TRANSCRIBE_CONCURRENCY=2
BATCH_ANALYZE_CONCURRENCY=2
```

## 安装和启动

```bash
python3 -m venv .venv-douyin-api
. .venv-douyin-api/bin/activate
pip install -r requirements.txt
mkdir -p external
git clone https://github.com/Evil0ctal/Douyin_TikTok_Download_API.git external/Douyin_TikTok_Download_API
pip install -r external/Douyin_TikTok_Download_API/requirements.txt
python3 scripts/check_setup.py
python3 server.py --host 127.0.0.1 --port 8791
```

然后访问：

```text
http://127.0.0.1:8791/
```

## 使用流程

1. 在工作台输入抖音主页链接或 `sec_user_id`。
2. 选择已有分类，或选择“自定义分类”后输入新分类。
3. 设置点赞 Top N、最低点赞、高赞评论数等参数。
4. 如需采集完成后自动处理，勾选“转录”或“分析”。这些选项会自动保证“下载视频”被勾选。
5. 点击“开始采集”。
6. 在批量处理区按账号、分类、状态筛选候选视频。
7. 执行批量转录、批量分析、标记无口播或删除。
8. 在数据查看页筛选素材。
9. 在信息页面查看 AI 分析、高赞评论、转录文本和任务详情。
10. 无口播视频可标记为“无口播”，后续批量任务会跳过。

## 环境检查

运行：

```bash
python3 scripts/check_setup.py
```

它会检查：

- Python 版本
- `.env` 是否存在
- 必要 API 配置是否缺失
- `ffmpeg`、`git`、`curl` 是否可用
- 外部 `Douyin_TikTok_Download_API` 是否已克隆
- 外部依赖是否大致可导入
- 本地运行目录是否可创建

## 常见问题

### 只执行 `pip install -r requirements.txt` 可以吗？

不够。账号采集依赖 `external/Douyin_TikTok_Download_API`，还需要执行：

```bash
pip install -r external/Douyin_TikTok_Download_API/requirements.txt
```

### 为什么转录失败？

常见原因：

- 视频没有口播声音。
- 未安装 `ffmpeg`。
- `DOUBAO_ASR_API_KEY` 或旧版认证配置错误。
- 默认音频上传服务 `mp3tourl` 不可用。
- 网络无法访问豆包 ASR 或音频公网地址。

### 为什么采集失败？

常见原因：

- 未克隆 `external/Douyin_TikTok_Download_API`。
- 未安装外部项目依赖。
- 抖音公开接口临时变化或风控。
- 当前网络访问抖音不稳定。
- 输入的主页链接或 `sec_user_id` 无效。

### 可以只用主页链接吗？

可以。推荐直接使用抖音主页链接。系统会从链接解析 `sec_user_id`。

## 安全说明

本项目是本地个人工具，不是通用爬虫服务。请遵守平台规则、版权规则和隐私边界。任何滥用行为都不属于本项目目标。
