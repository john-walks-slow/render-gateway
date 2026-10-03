#!/usr/bin/env python3
"""Merged public gateway: browser capability + tunnel server, one PORT.

Browser side (BROWSER_TOKEN: ?token= / Bearer / X-Token):
  POST /fetch           -> {url, format} => page content
  ALL  /mcp...          -> Streamable-HTTP MCP (lightpanda, per-client session)
  GET  /json/*          -> CDP discovery (lightpanda serve)
  WS   /                -> CDP browser endpoint (connectOverCDP here)

Tunnel side (frps, own auth.token at protocol level):
  WS   /~!frp           -> frp control channel (frpc dials in)
  everything else       -> frps HTTP vhost (Host preserved; kandev etc.)

Infra:
  GET  /health          -> {"ok": true} (Render + keepalive, open)
  GET  /info            -> capability list (open)
  GET  /debug?token=FRP_TOKEN -> upstreams + recent request log

Only stdlib is used; lightpanda comes from pip, frps from the image.
"""
import http.client
import json
import os
import shutil
import subprocess
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, unquote

BROWSER_TOKEN = os.environ.get("BROWSER_TOKEN", "")
FRP_TOKEN = os.environ.get("FRP_TOKEN", os.environ.get("DEBUG_TOKEN", ""))
MCP_HOST, MCP_PORT = "127.0.0.1", int(os.environ.get("MCP_PORT", "8899"))
CDP_HOST, CDP_PORT = "127.0.0.1", int(os.environ.get("CDP_PORT", "9222"))
FRPS_HOST, FRPS_PORT = "127.0.0.1", int(os.environ.get("FRPS_PORT", "18080"))
FETCH_TIMEOUT = int(os.environ.get("FETCH_TIMEOUT", "120"))
MAX_BYTES = int(os.environ.get("DUMP_MAX_BYTES", "400000"))
FETCH_FORMATS = ("markdown", "text", "html", "semantic_tree_text")

fetch_lock = threading.Lock()
REQLOG = deque(maxlen=50)
REQLOCK = threading.Lock()


def note(entry):
    entry["ts"] = time.strftime("%H:%M:%S")
    with REQLOCK:
        REQLOG.append(entry)


def browser_auth(h):
    if not BROWSER_TOKEN:
        return True
    q = parse_qs(urlparse(h.path).query)
    if q.get("token", [""])[0] == BROWSER_TOKEN:
        return True
    hdrs = h.headers
    if hdrs.get("Authorization", "") == "Bearer " + BROWSER_TOKEN:
        return True
    return hdrs.get("X-Token", "") == BROWSER_TOKEN


def is_ws(h):
    return h.headers.get("Upgrade", "").lower() == "websocket"


class H(BaseHTTPRequestHandler):
    server_version = "gateway/3.0"

    def log_message(self, fmt, *args):
        pass

    def _send(self, code, obj):
        try:
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    # ---- routing ----
    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/debug" and self._debug_ok():
            return self._serve_debug()
        if is_ws(self):
            note({"m": "GET", "p": path, "ws": True,
                  "ua": self.headers.get("User-Agent", "")[:40]})
            if unquote(path) == "/~!frp":
                return self._bridge_ws("/~!frp", FRPS_HOST, FRPS_PORT,
                                       origin=self.headers.get("Origin"))
            if not browser_auth(self):
                return self._send(401, {"error": "unauthorized"})
            return self._bridge_ws(path, CDP_HOST, CDP_PORT)
        if path in ("/health", "/healthz"):
            return self._send(200, {"ok": True, "service": "gateway"})
        if path == "/info":
            return self._send(200, {
                "service": "gateway",
                "browser": ["POST /fetch", "ALL /mcp", "GET /json/*", "WS /"],
                "tunnel": ["WS /~!frp", "all other paths -> frps vhost"],
            })
        if path.startswith("/mcp"):
            if not browser_auth(self):
                return self._send(401, {"error": "unauthorized"})
            return self._proxy_mcp()
        if path.startswith("/json"):
            if not browser_auth(self):
                return self._send(401, {"error": "unauthorized"})
            return self._proxy_http_to(CDP_HOST, CDP_PORT, strip_query=True)
        note({"m": "GET", "p": path,
              "ua": self.headers.get("User-Agent", "")[:40]})
        return self._proxy_frps()

    def do_POST(self):
        path = urlparse(self.path).path
        if path == "/fetch":
            return self._handle_fetch()
        note({"m": "POST", "p": path,
              "up": self.headers.get("Upgrade", ""),
              "cl": self.headers.get("Content-Length", ""),
              "te": self.headers.get("Transfer-Encoding", ""),
              "ua": self.headers.get("User-Agent", "")[:40]})
        if is_ws(self):
            if unquote(path) == "/~!frp":
                return self._bridge_ws("/~!frp", FRPS_HOST, FRPS_PORT,
                                       origin=self.headers.get("Origin"))
            if not browser_auth(self):
                return self._send(401, {"error": "unauthorized"})
            return self._bridge_ws(path, CDP_HOST, CDP_PORT)
        if path.startswith("/mcp"):
            if not browser_auth(self):
                return self._send(401, {"error": "unauthorized"})
            return self._proxy_mcp()
        return self._proxy_frps()

    def do_PUT(self):
        path = urlparse(self.path).path
        if path.startswith("/json"):
            if not browser_auth(self):
                return self._send(401, {"error": "unauthorized"})
            return self._proxy_http_to(CDP_HOST, CDP_PORT, strip_query=True)
        return self._proxy_frps()

    def do_DELETE(self):
        path = urlparse(self.path).path
        if path.startswith("/mcp"):
            if not browser_auth(self):
                return self._send(401, {"error": "unauthorized"})
            return self._proxy_mcp()
        if path.startswith("/json"):
            if not browser_auth(self):
                return self._send(401, {"error": "unauthorized"})
            return self._proxy_http_to(CDP_HOST, CDP_PORT, strip_query=True)
        return self._proxy_frps()

    def _debug_ok(self):
        q = urlparse(self.path).query
        return bool(FRP_TOKEN) and ("token=" + FRP_TOKEN) in q

    def _serve_debug(self):
        import socket as _s
        ups = {}
        for name, host, port in (("frps", FRPS_HOST, FRPS_PORT),
                                 ("mcp", MCP_HOST, MCP_PORT),
                                 ("cdp", CDP_HOST, CDP_PORT)):
            try:
                c = _s.create_connection((host, port), timeout=5)
                c.close()
                ups[name] = "open"
            except Exception as e:
                ups[name] = "closed: %s" % e
        with REQLOCK:
            log = list(REQLOG)
        return self._send(200, {"upstreams": ups, "recent": log})

    # ---- browser: /fetch ----
    def _handle_fetch(self):
        if not browser_auth(self):
            return self._send(401, {"error": "unauthorized"})
        try:
            req = json.loads(self._read_body() or b"{}")
        except Exception:
            return self._send(400, {"error": "bad json body"})
        url = req.get("url", "")
        if not url.startswith(("http://", "https://")):
            return self._send(400, {"error": "url must start with http(s)://"})
        fmt = req.get("format", "markdown")
        if fmt not in FETCH_FORMATS:
            return self._send(400, {"error": "format must be one of %s" % (FETCH_FORMATS,)})
        with fetch_lock:
            try:
                r = subprocess.run(
                    ["lightpanda", "fetch", url, "--dump", fmt,
                     "--dump-max-bytes", str(MAX_BYTES), "--json"],
                    capture_output=True, timeout=FETCH_TIMEOUT)
            except subprocess.TimeoutExpired:
                return self._send(504, {"error": "fetch timeout"})
        try:
            data = json.loads(r.stdout or b"{}")
        except Exception:
            return self._send(502, {
                "error": "browser failed",
                "detail": (r.stderr or b"")[-500:].decode(errors="replace"),
            })
        if not isinstance(data, dict) or data.get("error"):
            return self._send(502, {"error": "browser failed", "detail": data})
        return self._send(200, {
            "url": url,
            "format": fmt,
            "http_status": data.get("http_status"),
            "content": data.get("content", ""),
        })

    # ---- helpers ----
    def _read_body(self):
        te = self.headers.get("Transfer-Encoding", "")
        if "chunked" in te.lower():
            chunks = []
            while True:
                line = self.rfile.readline().strip().split(b";")[0]
                try:
                    n = int(line, 16)
                except ValueError:
                    break
                if n == 0:
                    self.rfile.readline()
                    break
                chunks.append(self.rfile.read(n))
                self.rfile.readline()
            return b"".join(chunks)
        length = self.headers.get("Content-Length")
        if length:
            try:
                return self.rfile.read(int(length))
            except ValueError:
                return None
        return None

    def _proxy_mcp(self):
        length = self.headers.get("Content-Length")
        body = self.rfile.read(int(length)) if length else None
        fwd = {k: v for k, v in self.headers.items()
               if k.lower() not in ("host", "content-length", "connection")}
        conn = http.client.HTTPConnection(MCP_HOST, MCP_PORT, timeout=150)
        try:
            conn.request(self.command, self.path, body=body, headers=fwd)
            resp = conn.getresponse()
            self.send_response(resp.status, resp.reason)
            for k, v in resp.getheaders():
                if k.lower() not in ("connection", "transfer-encoding", "content-length"):
                    self.send_header(k, v)
            self.send_header("Connection", "close")
            self.end_headers()
            shutil.copyfileobj(resp, self.wfile)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:
            self._send(502, {"error": "mcp upstream failed: %s" % e})
        finally:
            conn.close()

    def _proxy_http_to(self, host, port, strip_query=False):
        # Plain HTTP reverse proxy. strip_query keeps auth tokens off backends.
        target = urlparse(self.path).path if strip_query else self.path
        body = self._read_body()
        fwd_host = self.headers.get("Host", "%s:%d" % (host, port))
        conn = http.client.HTTPConnection(host, port, timeout=120)
        try:
            conn.putrequest(self.command, target, skip_host=True,
                            skip_accept_encoding=True)
            conn.putheader("Host", fwd_host)
            for k, v in self.headers.items():
                if k.lower() not in ("host", "content-length", "connection",
                                     "accept-encoding"):
                    conn.putheader(k, v)
            if body is not None:
                conn.putheader("Content-Length", str(len(body)))
            conn.endheaders(body)
            resp = conn.getresponse()
            payload = resp.read()
            self.send_response(resp.status, resp.reason)
            for k, v in resp.getheaders():
                if k.lower() not in ("connection", "transfer-encoding", "content-length"):
                    self.send_header(k, v)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:
            self._send(502, {"error": "upstream failed: %s" % e})
        finally:
            conn.close()

    def _proxy_frps(self):
        # Tunnel traffic: preserve original Host (frps routes by it).
        return self._proxy_http_to(FRPS_HOST, FRPS_PORT)

    def _bridge_ws(self, target, host, port, origin=None):
        # Terminate client WS here, open our own handshake upstream,
        # then relay raw frames both ways.
        import base64
        import hashlib
        import os as _os
        import select
        import socket as _socket
        GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
        ckey = self.headers.get("Sec-WebSocket-Key", "")
        if not ckey:
            return self._send(400, {"error": "missing Sec-WebSocket-Key"})
        accept = base64.b64encode(
            hashlib.sha1((ckey + GUID).encode()).digest()).decode()
        try:
            self.connection.sendall(
                ("HTTP/1.1 101 Switching Protocols\r\n"
                 "Upgrade: websocket\r\n"
                 "Connection: Upgrade\r\n"
                 "Sec-WebSocket-Accept: %s\r\n\r\n" % accept).encode("latin-1"))
        except (BrokenPipeError, ConnectionResetError):
            return
        backend_head = b""
        try:
            backend = _socket.create_connection((host, port), timeout=15)
            backend.settimeout(10)
            bkey = base64.b64encode(_os.urandom(16)).decode()
            hs = [("GET %s HTTP/1.1" % target),
                  ("Host: %s:%d" % (host, port)),
                  "Upgrade: websocket",
                  "Connection: Upgrade",
                  ("Sec-WebSocket-Key: %s" % bkey),
                  "Sec-WebSocket-Version: 13"]
            if origin is None and (host, port) == (FRPS_HOST, FRPS_PORT):
                # frps's WS handler requires an Origin header (any value).
                origin = "http://%s:%d" % (host, port)
            if origin:
                hs.append("Origin: %s" % origin)
            backend.sendall(("\r\n".join(hs) + "\r\n\r\n").encode("latin-1"))
            head = b""
            try:
                while b"\r\n\r\n" not in head:
                    chunk = backend.recv(4096)
                    if not chunk:
                        break
                    head += chunk
            except Exception:
                pass
            backend_head = head[:100]
            if b" 101 " not in head.split(b"\r\n", 1)[0]:
                raise ConnectionError("backend refused: %s" % backend_head)
            note({"m": "WS-BRIDGE", "p": target, "r": "101 established"})
        except Exception as e:
            try:
                backend.close()
            except Exception:
                pass
            note({"m": "WS-BRIDGE", "p": target,
                  "r": "backend failed: %s | saw: %s" % (e, backend_head)})
            return
        try:
            backend.setblocking(False)
            self.connection.setblocking(False)
            while True:
                r, _, _ = select.select([self.connection, backend], [], [], 300)
                if not r:
                    break
                for src in r:
                    dst = backend if src is self.connection else self.connection
                    try:
                        data = src.recv(65536)
                    except BlockingIOError:
                        continue
                    if not data:
                        raise ConnectionError("closed")
                    dst.sendall(data)
        except Exception:
            pass
        finally:
            try:
                backend.close()
            except Exception:
                pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    srv = ThreadingHTTPServer(("0.0.0.0", port), H)
    print("gateway on 0.0.0.0:%d (browser+mcp+cdp, tunnel->frps %d)" % (port, FRPS_PORT), flush=True)
    srv.serve_forever()
