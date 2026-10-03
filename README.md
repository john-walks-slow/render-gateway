# gateway (merged)

One Render Free service serving both halves (single `$PORT`):

**Browser** (`BROWSER_TOKEN`: `?token=` / `Authorization: Bearer` / `X-Token`)
- `POST /fetch` → `{url, format}` → `{url, http_status, content}`
- `ALL /mcp` → Streamable-HTTP MCP (per-client browsing session)
- `GET /json/*` → CDP discovery; `WS /` → CDP browser endpoint

**Tunnel** (frps, own `auth.token`)
- `WS /~!frp` → frp control channel
- everything else → frps HTTP vhost (Host preserved)

**Infra** (open): `GET /health`, `GET /info`,
`GET /debug?token=$FRP_TOKEN` (upstreams + recent requests).

MCP for agents:

```json
{ "mcpServers": { "browser": {
  "type": "streamable-http",
  "url": "https://<svc>.onrender.com/mcp?token=$BROWSER_TOKEN"
} } }
```

CDP: `chromium.connectOverCDP('wss://<svc>.onrender.com/?token=' + TOKEN)`.

Limits (free tier): 512MB/0.1CPU, serialized fetches, sleep after
15 min idle, no screenshots/persistent login, datacenter IP.
