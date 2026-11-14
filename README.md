# 观澜 Horizon · 锋睿全球舆情雷达

网站：https://sonia16-creator.github.io/guanlan/

## 目录

| 路径 | 作用 |
| --- | --- |
| `index.html` | 网站本体（单页，多视图切换） |
| `pipeline/sources.json` | 免费信源配置：加信源、改关键词、停用信源都在这里 |
| `pipeline/run.py` | 采集 → 去重 → DeepSeek 中文摘要与红黄绿初判 → 写出 `data/feed.json` |
| `.github/workflows/radar.yml`（本地文件名 `github-workflow-radar.yml`） | GitHub Actions 定时任务：首尔时间每天 08:07、20:07 自动运行 |
| `data/feed.json` | 自动生成，不要手改；网站「实时信源」页读取它 |

## 第一次上线（只做一次）

1. **上传文件**：把本文件夹里的 `index.html`、`README.md`、`pipeline/` 上传到 guanlan 仓库根目录。
   定时任务文件在本文件夹里叫 `github-workflow-radar.yml`（电脑上不能直接建 `.github` 文件夹）。上传时在仓库里点 Add file → Create new file，
   文件名填 `.github/workflows/radar.yml`，把 `github-workflow-radar.yml` 的内容整段粘进去，Commit。这个文件不用再单独上传。
2. **填 DeepSeek 密钥**：仓库 Settings → Secrets and variables → Actions → New repository secret，
   名称填 `DEEPSEEK_API_KEY`，值填你的 DeepSeek API Key。不填也能跑，只是不做摘要和分级。
3. **允许写入**：Settings → Actions → General → Workflow permissions，选 Read and write permissions，保存。
4. **手动跑第一次**：Actions → 观澜信源采集 → Run workflow。约 3–5 分钟后仓库里出现 `data/feed.json`，
   网站「实时信源」页就有数据了。

## 日常维护

- **看哪些信源坏了**：网站「实时信源」页底部的「信源健康」表。
- **加信源**：在 `sources.json` 加一行。类型有 `rss`（填 url）、`gnews`（Google News 关键词，填 q 和 lang）、`gdelt`（填 q）。
- **停用信源**：给那一行加 `"disabled": true`。
- **换模型**：Settings → Secrets and variables → Actions → Variables，新建 `DEEPSEEK_MODEL`。
- **控制成本**：每次最多送 160 条给 AI（`run.py` 里的 `MAX_AI`），已经分析过的条目不会重复分析。

## 局势推演与到期对账

- **推演**：每次采集给新的红色事件自动生成局势推演（三种情景、验证节点、到期对账日），存入 `data/sims.json`。档案只增不删，网站「局势推演」页的「推演档案」可查看全部。
- **到期对账**：到了对账日，系统检索这段时间的新闻，由 AI 判断实际更接近哪种情景：情景 A 发生 = 成立，B 或 C 发生 = 未成立，证据不足 = 顺延 30 天（最多两次，之后记为无法判定）。
- **人工确认**：每条 AI 判断会在仓库 **Issues** 里自动开一张「[对账] …」单。打开它，在右侧 Labels 加一个标签即可（手机上也能操作）：
  - `确认`：同意 AI 初判
  - `改判-成立` / `改判-未成立` / `改判-无法判定`：以人工判断为准，可在评论里写理由
  下次自动采集时会写回档案并关闭这张单。网站上未确认的会标「AI 初判 · 待人工确认」，命中率旁边也会注明。
- 这份带时间戳、错了也留痕的推演记录，是观澜最难被复制的资产，请保持对账单及时确认。

## 说明

- 红黄绿为 AI 初判，未经人工复核；摘要为 AI 对原文的中文整理，以原文为准。
- 当前全部为免费公开信源。付费信源（Factiva、路透等）需企业数据授权并确认包含 AI 使用权后再接入。
