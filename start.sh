#!/bin/sh
# Merged gateway: frps + lightpanda (mcp/serve) + public router.
: "${FRP_TOKEN:?FRP_TOKEN env required}"
cat > /app/frps.toml <<EOF
bindPort = 18080
vhostHTTPPort = 18080
auth.token = "$FRP_TOKEN"
EOF
/opt/frp/frps -c /app/frps.toml &
lightpanda mcp --port 8899 --host 127.0.0.1 &
lightpanda serve --host 127.0.0.1 --port 9222 --cdp-max-connections 4 &
exec python3 /app/gateway.py
