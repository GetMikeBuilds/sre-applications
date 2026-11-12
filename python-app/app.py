import os
import logging
import redis
from redis.sentinel import Sentinel
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST
import psycopg

# 从环境变量获取 Redis 地址和哨兵端口
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_SENTINEL_PORT = int(os.environ.get("REDIS_SENTINEL_PORT", "26379"))

redis_client = None
try:
    # 1. 连接 Sentinel 集群（3个哨兵）
    sentinel = Sentinel([(REDIS_HOST, REDIS_SENTINEL_PORT)], socket_timeout=2)
    # 2. 向哨兵要主节点连接。注意：Bitnami Chart 默认的 master 名字是 "mymaster"
    redis_client = sentinel.master_for('mymaster', socket_timeout=2)
except Exception as e:
    logging.error(f"Failed to init Redis Sentinel: {e}")

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
    "host": os.environ.get("DB_HOST", "python-api-db-rw"),
    "port": int(os.environ.get("DB_PORT", "5432")),
    "dbname": os.environ.get("DB_NAME", "sre_lab"),
    "user": os.environ.get("DB_USER", "sre_user"),
    "password": os.environ.get("DB_PASSWORD", ""),
    "connect_timeout": 3,
}

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            REQUEST_COUNT.labels(path="/health", status="200").inc()
            self.end_headers()
            self.wfile.write(b"ok\n")
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
                # 1. 检查数据库
                with psycopg.connect(**DB_CONFIG) as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                        result = cur.fetchone()
                if result != (1,):
                    raise RuntimeError("Unexpected database health query result")
                
                # 2. 检查 Redis (如果 redis_client 没初始化成功，这里会直接抛异常)
                if not redis_client:
                    raise RuntimeError("Redis client not initialized")
                    
                redis_client.set("health_check", "ok", ex=10)
                val = redis_client.get("health_check")
                if val != b"ok":
                    raise Exception("Redis read/write failed")

                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"database and redis ok\n")
                
                DB_HEALTH.set(1)
                REQUEST_COUNT.labels(path="/db-health", status="200").inc()
                return

            except Exception as e:
                logging.exception("Dependency health check failed")
                self.send_response(503)
                self.end_headers()
                self.wfile.write(b"dependency unavailable\n")
                
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