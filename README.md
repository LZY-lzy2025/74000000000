# 74001.tv

## 运行方式（拆分 Playwright 进程）

- `app.py`：仅提供 Flask API，不再在主进程里跑 Playwright。
- `scraper.py`：独立抓取脚本，负责完整采集与解密流程。

### 本地启动

```bash
python app.py
```

### 手动触发抓取

```bash
curl "http://127.0.0.1:5000/trigger"
```

> `/trigger` 会异步拉起 `scraper.py` 子进程；脚本内部用 `output/scrape_job.lock` 防重入。

## Cloudflare Worker Cron 调度（每 12 分钟）

可以用 Worker 定时访问你的服务 `/trigger`，把调度从 Flask 进程中移出去。

`wrangler.toml` 示例：

```toml
name = "tv-trigger-cron"
main = "src/index.js"
compatibility_date = "2025-01-01"

[triggers]
crons = ["*/12 * * * *"]
```

`src/index.js` 示例：

```js
export default {
  async scheduled(event, env, ctx) {
    const resp = await fetch(env.TRIGGER_URL, {
      method: "GET",
      headers: { "User-Agent": "cf-worker-cron/1.0" },
    });

    if (!resp.ok) {
      const text = await resp.text();
      console.error("trigger failed", resp.status, text);
    }
  },
};
```

Worker Secret/Var：
- `TRIGGER_URL=https://你的域名/trigger`

## Sealos 部署（推荐）

### 1) 构建并推送镜像

```bash
docker build -t <你的镜像仓库>/74001-tv:latest .
docker push <你的镜像仓库>/74001-tv:latest
```

### 2) 在 Sealos 创建应用

- 镜像地址填：`<你的镜像仓库>/74001-tv:latest`
- 端口填：`5000`
- 启动命令使用默认（`python -u app.py`）
- 建议开启一个持久化目录挂载到 `/app/output`（用于保留抓取结果和状态）

### 3) 暴露公网访问

- 给应用绑定一个公网域名，例如：`https://tv.example.com`
- 确认可以访问：`https://tv.example.com/` 和 `https://tv.example.com/trigger`

### 4) 配置 Cloudflare Worker Cron

- Worker 环境变量 `TRIGGER_URL` 设置为：`https://tv.example.com/trigger`
- Cron 表达式：`*/12 * * * *`

### 5) 验证链路

1. 手动访问 `/trigger` 返回 `ok: true`
2. 约 10~30 秒后访问 `/`，`scrape_status` 应出现 `running/success/skipped`
3. 访问 `/m3u` 能拿到列表内容
