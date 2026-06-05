# 从零安装和运行

本文档面向“从 GitHub 新 clone 下来”的本地部署流程。

## 1. 克隆项目

```bash
git clone https://github.com/jickchen34/douyin-live-research-workbench.git
cd douyin-live-research-workbench
```

如果你是在本机已有工作区中使用，进入项目目录即可。

## 2. 准备系统依赖

需要：

- Python 3.10 或更高版本
- git
- curl
- ffmpeg

macOS 示例：

```bash
brew install ffmpeg
```

确认命令可用：

```bash
python3 --version
git --version
curl --version
ffmpeg -version
```

## 3. 创建 Python 虚拟环境

```bash
python3 -m venv .venv-douyin-api
. .venv-douyin-api/bin/activate
python -m pip install --upgrade pip
```

Windows PowerShell 可参考：

```powershell
python -m venv .venv-douyin-api
.\.venv-douyin-api\Scripts\Activate.ps1
python -m pip install --upgrade pip
```

## 4. 安装本项目依赖

```bash
pip install -r requirements.txt
```

## 5. 克隆并安装外部采集依赖

本项目不把 `Douyin_TikTok_Download_API` 直接提交进仓库，需要本地克隆：

```bash
mkdir -p external
git clone https://github.com/Evil0ctal/Douyin_TikTok_Download_API.git external/Douyin_TikTok_Download_API
pip install -r external/Douyin_TikTok_Download_API/requirements.txt
```

如果该项目依赖以后变化，以它仓库内的 README 和 requirements 为准。

## 6. 配置环境变量

复制模板：

```bash
cp .env.example .env
```

最少需要填写：

```bash
VOLCENGINE_API_KEY=your_api_key_here
VOLCENGINE_ENDPOINT_ID=ep_xxx
DOUBAO_ASR_API_KEY=your_asr_api_key_here
```

### 火山方舟配置

- `VOLCENGINE_API_KEY`：火山方舟 API Key。
- `VOLCENGINE_ENDPOINT_ID`：你的豆包模型接入点 ID。
- `VOLCENGINE_BASE_URL`：默认是 `https://ark.cn-beijing.volces.com/api/v3/chat/completions`，一般不用改。

### 豆包语音 ASR 配置

默认配置：

```bash
DOUBAO_ASR_AUTH_MODE=api_key
DOUBAO_ASR_API_MODE=standard
DOUBAO_ASR_UPLOAD_PROVIDER=mp3tourl
DOUBAO_ASR_RESOURCE_ID=volc.seedasr.auc
```

说明：

- `standard` 模式需要一个公网可访问的音频 URL。
- 默认 `mp3tourl` 会尝试上传本地 MP3 文件并得到公网 URL。
- 如果上传服务不可用，可改为 `bashupload`，或配置自己的公网音频服务。

可选配置：

```bash
DOUBAO_ASR_UPLOAD_PROVIDER=bashupload
DOUBAO_ASR_PUBLIC_BASE_URL=https://your-public-host.example.com/audio
DOUBAO_ASR_AUDIO_URL_TEMPLATE=https://your-public-host.example.com/audio/{filename}
```

如果使用旧版认证：

```bash
DOUBAO_ASR_AUTH_MODE=legacy
DOUBAO_ASR_APP_KEY=your_app_key
DOUBAO_ASR_ACCESS_KEY=your_access_key
```

批量任务并发默认较保守，推荐先保持默认：

```bash
BATCH_TRANSCRIBE_CONCURRENCY=2
BATCH_ANALYZE_CONCURRENCY=2
```

服务端会把并发限制在 1 到 4 之间。

## 7. 检查环境

```bash
python3 scripts/check_setup.py
```

如果输出存在 `FAIL`，先按提示处理。`WARN` 不一定阻塞启动，但可能影响采集、转录或分析。

## 8. 启动本地服务

```bash
python3 server.py --host 127.0.0.1 --port 8791
```

浏览器打开：

```text
http://127.0.0.1:8791/
```

## 9. 最小测试流程

建议第一次只采集少量数据：

1. 打开工作台。
2. 输入一个抖音主页链接。
3. `点赞 Top N` 填 `1` 或 `3`。
4. 保持 `下载视频` 勾选。
5. 点击 `开始采集`。
6. 如果采集成功，再对单条视频执行转录。
7. 转录成功后执行 AI 分析。

如果视频没有口播，ASR 可能返回空文本，这种情况可以标记为“无口播”。

## 10. 自动处理流程

采集页可以分别勾选 `转录` 和 `分析`。勾选后：

1. 先采集账号作品。
2. 只处理本次采集返回的 `saved_video_ids`。
3. 如果勾选转录，后端按受控并发批量转录。
4. 如果勾选分析，全部转录阶段结束后，再按受控并发批量分析。
5. 全程显示全局 loading 和进度，避免误操作。
6. 失败任务不会自动重试，会进入失败任务栏等待手动处理。

## 11. 本地数据位置

运行后会生成：

```text
data/library.sqlite3          # SQLite 数据库
data/douyin_media/            # 下载视频
data/asr_audio/               # 抽取音频
data/asr_results/             # ASR 原始结果
logs/                          # 运行日志
web/library.json               # 前端缓存
```

这些文件都已在 `.gitignore` 中排除，不应提交。

## 12. 常见故障定位

### `ModuleNotFoundError: crawlers`

说明没有克隆外部依赖，或目录位置不对。确认存在：

```text
external/Douyin_TikTok_Download_API/crawlers
```

### `ModuleNotFoundError: Cryptodome` 或 `gmssl`

说明没有安装外部项目依赖：

```bash
pip install -r external/Douyin_TikTok_Download_API/requirements.txt
```

### `ffmpeg` 命令失败

确认已安装：

```bash
ffmpeg -version
```

### 豆包 ASR 提示无有效语音

通常是视频没有口播。可以在前端标记为“无口播”。

### 豆包 ASR 上传失败

默认上传服务可能不稳定。可切换：

```bash
DOUBAO_ASR_UPLOAD_PROVIDER=bashupload
```

或配置自己的公网音频地址。

### AI 分析失败

检查：

- `VOLCENGINE_API_KEY` 是否正确。
- `VOLCENGINE_ENDPOINT_ID` 是否是已开通的接入点。
- 当前网络是否能访问火山方舟 API。
