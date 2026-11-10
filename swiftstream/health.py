"""Health check HTTP endpoint."""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

logger = logging.getLogger(__name__)


class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP handler for health and readiness checks."""

    health_fn: Callable[[], dict[str, Any]] = lambda: {"status": "ok"}

    def do_GET(self) -> None:  # noqa: N802
        """Handle GET requests for /health and /ready."""
        if self.path == "/health":
            self._respond_json(200, {"status": "ok"})
        elif self.path == "/ready":
            health = self.health_fn()
            status_code = 200 if health.get("status") == "ok" else 503
            self._respond_json(status_code, health)
        elif self.path == "/metrics":
            # Redirect to the Prometheus metrics endpoint
            self._respond_json(200, {"hint": "Prometheus metrics are on the metrics port"})
        else:
            self._respond_json(404, {"error": "not found"})

    def _respond_json(self, status: int, body: dict[str, Any]) -> None:
        """Send a JSON response."""
        payload = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: Any) -> None:
        """Suppress default HTTP logging; use our logger instead."""
        logger.debug("Health check: %s", format % args)


class HealthCheckServer:
    """Runs the health check HTTP server in a background thread."""

    def __init__(
        self,
        port: int = 8080,
        health_fn: Callable[[], dict[str, Any]] | None = None,
    ) -> None:
        self._port = port
        self._health_fn = health_fn or (lambda: {"status": "ok"})
        self._server: HTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the health check server in a daemon thread."""
        # Create a handler class with the health function bound
        health_fn = self._health_fn

        class Handler(HealthCheckHandler):
            pass

        Handler.health_fn = staticmethod(health_fn)

        self._server = HTTPServer(("0.0.0.0", self._port), Handler)
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="health-check",
            daemon=True,
        )
        self._thread.start()
        logger.info("Health check server started on port %d", self._port)

    def stop(self) -> None:
        """Shut down the health check server."""
        if self._server:
            self._server.shutdown()
            self._server = None
        if self._thread:
            self._thread.join(timeout=5.0)
            self._thread = None
        logger.info("Health check server stopped")
