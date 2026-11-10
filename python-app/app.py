
import os
import logging
import random
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST

import psycopg


REQUEST_COUNT = Counter(
    "python_api_requests_total", "Total HTTP requests", ["path", "status"]
)
DB_HEALTH = Gauge(
    "python_api_db_health", "1 if DB reachable, 0 otherwise"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "db"),
    "port": int(os.environ.get("DB_PORT", "5432")),
    "dbname": os.environ.get("DB_NAME", "sre_lab"),
    "user": os.environ.get("DB_USER", "sre_user"),
    "password": os.environ.get("DB_PASSWORD", ""),
    "connect_timeout": 3,
}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            if random.random() < 0.5:
                self.send_response(500)
                REQUEST_COUNT.labels(path="/health", status="500").inc()
            else:
                self.send_response(200)
                REQUEST_COUNT.labels(path="/health", status="200").inc()
            self.end_headers()
            self.wfile.write(b"done\n")
            return
        
        if self.path == "/metrics":
            body = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path == "/db-health":
            try:
                with psycopg.connect(**DB_CONFIG) as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                        result = cur.fetchone()

                if result != (1,):
                    raise RuntimeError("Unexpected database health query result")

                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"database ok\n")
                
                # 【修复点 1】：连库成功，指标设为 1
                DB_HEALTH.set(1)
                # 【修复点 2】：这里的 path 应该是 "/db-health"
                REQUEST_COUNT.labels(path="/db-health", status="200").inc()
                return

            except Exception:
                logging.exception("Database health check failed")
                self.send_response(503)
                self.end_headers()
                self.wfile.write(b"database unavailable\n")
                
                # 【修复点 3】：连库失败，指标设为 0
                DB_HEALTH.set(0)
                REQUEST_COUNT.labels(path="/db-health", status="503").inc()
                return

        self.send_response(404)
        self.end_headers()
        self.wfile.write(b"not found\n")
        REQUEST_COUNT.labels(path="other", status="404").inc()

    def log_message(self, format, *args):
        logging.info("%s - %s", self.address_string(), format % args)


server = ThreadingHTTPServer(("0.0.0.0", 8000), Handler)
print("Server listening on port 8000", flush=True)
server.serve_forever()

