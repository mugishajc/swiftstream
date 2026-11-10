"""Tests for the health check server."""

from __future__ import annotations

import json
import time
import urllib.request

from swiftstream.health import HealthCheckServer


class TestHealthCheckServer:
    """Tests for the health check HTTP endpoint."""

    def test_health_endpoint(self) -> None:
        server = HealthCheckServer(port=18080)
        server.start()
        try:
            time.sleep(0.2)  # let the server bind
            resp = urllib.request.urlopen("http://localhost:18080/health", timeout=2)
            data = json.loads(resp.read())
            assert data["status"] == "ok"
        finally:
            server.stop()

    def test_ready_endpoint_custom_fn(self) -> None:
        def health_fn() -> dict[str, str]:
            return {"status": "ok", "detail": "all good"}

        server = HealthCheckServer(port=18081, health_fn=health_fn)
        server.start()
        try:
            time.sleep(0.5)
            resp = urllib.request.urlopen("http://localhost:18081/ready", timeout=2)
            data = json.loads(resp.read())
            assert data["status"] == "ok"
            assert data["detail"] == "all good"
        finally:
            server.stop()
