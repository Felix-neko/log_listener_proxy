"""Log Listener Proxy - микросервис для проксирования логов от PySpark к клиентам."""

from log_listener_proxy.server import app

__all__ = ["app"]
