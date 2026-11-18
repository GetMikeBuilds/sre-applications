import os
import logging
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
import redis
from redis.sentinel import Sentinel
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST
import psycopg

# 从环境变量获取 Redis 地址和哨兵端口
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_SENTINEL_PORT = int(os.environ.get("REDIS_SENTINEL_PORT", "26379"))

# ==========================================
# 【核心修改区】：配置 OpenTelemetry 并注入环境变量
# ==========================================
# 获取当前 Pod 的 Namespace（由 Downward API 注入）
namespace = os.environ.get("POD_NAMESPACE", "unknown")

# 配置 OpenTelemetry 导出到 Tempo，并把环境标识打进去
resource = Resource.create(attributes={
    "service.name": "python-api",
    "k8s.namespace.name": namespace,          # 标准 K8s 属性
    "deployment.environment": namespace       # 业界标准的环境属性
})

provider = TracerProvider(resource=resource)
# Tempo 默认的 OTLP gRPC 接收端口是 4317
otlp_exporter = OTLPSpanExporter(endpoint="http://tempo.monitoring.svc.cluster.local:4317", insecure=True)
provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

# ==========================================
# 【核心修改区】：日志与 TraceID 绑定
# ==========================================
class TraceFilter(logging.Filter):
    def filter(self, record):
        span = trace.get_current_span()
        if span and span.get_span_context().is_valid:
            record.trace_id = format(span.get_span_context().trace_id, '032x')
        else:
            record.trace_id = "00000000000000000000000000000000"
        return True

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [trace_id=%(trace_id)s] %(message)s",
)

for handler in logging.root.handlers:
    handler.addFilter(TraceFilter())

# ==========================================
# Redis 和 业务初始化
# ==========================================
redis_client = None
try:
    sentinel = Sentinel([(REDIS_HOST, REDIS_SENTINEL_PORT)], socket_timeout=2)
    redis_client = sentinel.master_for('mymaster', socket_timeout=2)
except Exception as e:
    logging.error(f"Failed to init Redis Sentinel: {e}")

REQUEST_COUNT = Counter(
    "python_api_requests_total", "Total HTTP requests", ["path", "status"]
)
DB_HEALTH = Gauge(
    "python_api_db_health", "1 if DB reachable, 0 otherwise"
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
        # 【补回来的 /health 接口，专供 livenessProbe 存活探针使用】
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok\n")
            REQUEST_COUNT.labels(path="/health", status="200").inc()
            return

        if self.path == "/metrics":
            body = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(body)
            return

        # 【唯一的 /db-health 接口，集成了 Trace】
        if self.path == "/db-health":
            with tracer.start_as_current_span("db_health_check") as span:
                try:
                    with tracer.start_as_current_span("postgres_query"):
                        with psycopg.connect(**DB_CONFIG) as conn:
                            with conn.cursor() as cur:
                                cur.execute("SELECT 1")
                                result = cur.fetchone()
                    if result != (1,):
                        raise RuntimeError("Unexpected database health query result")
                    
                    with tracer.start_as_current_span("redis_query"):
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

        # 兜底 404
        self.send_response(404)
        self.end_headers()
        self.wfile.write(b"not found\n")
        REQUEST_COUNT.labels(path="other", status="404").inc()

    def log_message(self, format, *args):
        logging.info("%s - %s", self.address_string(), format % args)

server = ThreadingHTTPServer(("0.0.0.0", 8000), Handler)
print("Server listening on port 8000", flush=True)
server.serve_forever()