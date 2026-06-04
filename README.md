# 抖音直播素材研究台

> 个人研究自用项目。禁止传播、转售、部署为公开服务或用于批量爬取、骚扰、侵权、绕过平台限制等用途。
>
> 本仓库公开仅用于个人备份和代码管理，不授予任何第三方使用、复制、分发或商用授权。仓库中不包含密钥、Cookie、数据库、视频、音频、日志或采集结果。

这个项目用于把“对标账号视频 -> 作品数据和高赞评论 -> 视频转写 -> AI 爆款分析 -> 本地素材库”跑成一个本地工作台。

## 能力范围

- 输入抖音主页链接，程序化采集账号作品、统计数据、高赞评论和媒体文件。
- 采集时支持最低点赞数、点赞 Top N、页数、每页数量、高赞评论数量等参数。
- 使用 `ffmpeg` 抽取音频，并调用火山豆包语音 ASR 转录视频口播。
- 调用火山方舟豆包模型，使用 `VOLCENGINE_ENDPOINT_ID` 作为模型/接入点 ID 做爆款分析。
- 前端支持工作台、数据查看、信息页面、失败任务和运行日志。
- 支持批量转录、批量分析、失败手动重试、标记无口播、单条/批量删除。
- 将视频、评论、转写、分析结果写入本地 SQLite 数据库。

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
  .env.example                # 配置模板，不含真实密钥
  requirements.txt            # Python 依赖
```

运行时会生成这些本地目录，但它们不会提交到 Git：

```text
data/      # 数据库、视频、音频、ASR 结果、分析结果
logs/      # 运行日志
external/  # 本地克隆的 Douyin_TikTok_Download_API
```

## 外部依赖

账号采集依赖本地克隆的开源项目：

```bash
mkdir -p external
git clone https://github.com/Evil0ctal/Douyin_TikTok_Download_API.git external/Douyin_TikTok_Download_API
```

该目录已被 `.gitignore` 忽略，不会进入本仓库。

系统依赖：

- Python 3.10+
- `ffmpeg`
- 可访问火山方舟和豆包语音 ASR 的网络环境

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

不要把真实 key 写进代码或提交到 Git。

## 启动

```bash
python3 -m venv .venv-douyin-api
. .venv-douyin-api/bin/activate
pip install -r requirements.txt
python3 server.py --host 127.0.0.1 --port 8791
```

然后访问：

```text
http://127.0.0.1:8791/
```

## 使用流程

1. 在工作台输入抖音主页链接。
2. 设置分类、页数、点赞 Top N、最低点赞、高赞评论数等参数。
3. 点击开始采集。
4. 在批量处理区按账号、分类、状态筛选候选视频。
5. 执行批量转录和批量分析。
6. 在数据查看页筛选素材。
7. 在信息页面查看 AI 分析、高赞评论、转录文本和任务详情。
8. 无口播视频可标记为“无口播”，后续批量任务会跳过。

## 安全说明

本项目是本地个人工具，不是通用爬虫服务。请遵守平台规则、版权规则和隐私边界。任何滥用行为都不属于本项目目标。
