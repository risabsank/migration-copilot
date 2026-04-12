"""HTTP API and static UI serving for migration control plane (stdlib implementation)."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

from sdk.control_plane.service import ControlPlaneService
from sdk.engine.models import MigrationPattern
from sdk.observability import EventCollector
from sdk.orchestration.execution_policy import ExecutionAction, ExecutionPolicyEngine
from sdk.state.models import RollbackTriggerReason
from sdk.state.store import JsonMigrationRunStore


@dataclass
class ApiResponse:
    status: int
    body: Any
    content_type: str = "application/json"


class ControlPlaneAPI:
    """Small route dispatcher used by HTTP server and tests."""

    def __init__(self, store_path: str | Path = ".migration_runs.json"):
        self.service = ControlPlaneService(
            store=JsonMigrationRunStore(store_path),
            collector=EventCollector(plan_id="control-plane"),
            execution_policy=ExecutionPolicyEngine(),
        )
        self._static_dir = Path(__file__).parent / "web"

    def handle(self, method: str, path: str, payload: dict[str, Any] | None = None) -> ApiResponse:
        payload = payload or {}
        parsed = urlparse(path)
        route = parsed.path

        if method == "GET" and route == "/":
            return ApiResponse(200, (self._static_dir / "index.html").read_text(encoding="utf-8"), "text/html")
        if method == "GET" and route.startswith("/static/"):
            target = self._static_dir / route.removeprefix("/static/")
            if not target.exists() or not target.is_file():
                return self._error(404, f"Static file not found: {route}")
            content_type = "text/plain"
            if target.suffix == ".css":
                content_type = "text/css"
            elif target.suffix == ".js":
                content_type = "application/javascript"
            return ApiResponse(200, target.read_text(encoding="utf-8"), content_type)

        try:
            return self._dispatch_api(method=method, route=route, payload=payload)
        except ValueError as exc:
            return self._error(404, str(exc))
        except PermissionError as exc:
            return self._error(403, str(exc))

    def _dispatch_api(self, *, method: str, route: str, payload: dict[str, Any]) -> ApiResponse:
        if method == "GET" and route == "/api/runs":
            return ApiResponse(200, self.service.list_runs())
        if method == "POST" and route == "/api/runs":
            if not payload.get("table_names"):
                return self._error(400, "table_names must be non-empty")
            body = self.service.create_run_from_plan(
                plan_id=payload["plan_id"],
                schema=payload.get("schema", "public"),
                selected_variant=payload.get("selected_variant", "backfill_cdc_sync"),
                pattern=payload.get("pattern", MigrationPattern.BACKFILL_CDC.value),
                table_names=list(payload.get("table_names", [])),
                run_id=payload.get("run_id"),
            )
            return ApiResponse(200, body)

        run_routes = self._match_run_route(route)
        if run_routes is None:
            return self._error(404, f"Route not found: {route}")
        run_id, suffix = run_routes

        if method == "GET" and suffix == "":
            return ApiResponse(200, self.service.get_run(run_id))
        if method == "GET" and suffix == "/tables":
            return ApiResponse(200, self.service.get_run(run_id)["table_progress"])
        if method == "GET" and suffix == "/validation":
            run = self.service.get_run(run_id)
            return ApiResponse(200, {"status": run["validation_status"], "summary": run["validation_summary"]})
        if method == "GET" and suffix == "/cdc":
            run = self.service.get_run(run_id)
            return ApiResponse(200, {"status": run["cdc_status"], "replication_lag_seconds": run["replication_lag_seconds"], "source_freshness_seconds": run["source_freshness_seconds"], "tables": run["cdc_table_progress"]})
        if method == "GET" and suffix == "/cutover":
            run = self.service.get_run(run_id)
            return ApiResponse(200, {"cutover_ready": run["cutover_ready"], "evaluation": run["cutover_evaluation"], "execution": run["cutover_execution"]})
        if method == "GET" and suffix == "/rollback":
            run = self.service.get_run(run_id)
            return ApiResponse(200, {"rollback_ready": run["rollback_ready"], "rollback_readiness": run["rollback_readiness"], "rollback_plan": run["rollback_plan"]})
        if method == "GET" and suffix == "/timeline":
            return ApiResponse(200, self.service.timeline(run_id=run_id))
        if method == "GET" and suffix == "/approvals":
            return ApiResponse(200, self.service.get_run(run_id)["approval_history"])
        if method == "GET" and suffix == "/recommendations":
            run = self.service.get_run(run_id)
            latest = run["ops_recommendation_history"][-1] if run["ops_recommendation_history"] else None
            return ApiResponse(200, {"latest": latest, "history": run["ops_recommendation_history"], "policy_profile": run["execution_policy_profile"], "phase_policy_overrides": run["phase_execution_policy_overrides"]})
        if method == "GET" and suffix == "/incident-pack":
            return ApiResponse(200, self.service.incident_pack(run_id=run_id))
        if method == "GET" and suffix == "/dashboard":
            return ApiResponse(200, self.service.dashboard(run_id=run_id))

        if method == "POST" and suffix == "/start":
            result = self.service.start_orchestration(run_id=run_id, max_phases=int(payload.get("max_phases", 1)))
            return ApiResponse(200, asdict(result))
        if method == "POST" and suffix == "/pause":
            return ApiResponse(200, self.service.pause_orchestration(run_id=run_id).as_dict())
        if method == "POST" and suffix == "/resume":
            result = self.service.resume_orchestration(run_id=run_id, max_phases=int(payload.get("max_phases", 1)))
            return ApiResponse(200, asdict(result))
        if method == "POST" and suffix.startswith("/tables/") and suffix.endswith("/retry"):
            parts = suffix.split("/")
            table_name = parts[2]
            decision = self.service.retry_failed_table(
                run_id=run_id,
                table_name=table_name,
                actor=payload.get("actor", "operator"),
                human_approved=payload.get("human_approved"),
            )
            return ApiResponse(200, decision.as_dict())
        if method == "POST" and suffix == "/approvals/request":
            action = ExecutionAction(payload["action"])
            return ApiResponse(200, self.service.request_approval(run_id=run_id, action=action, actor=payload.get("actor", "operator")))
        if method == "POST" and suffix == "/approvals/decision":
            action = ExecutionAction(payload["action"])
            decision = self.service.approve_or_deny_action(
                run_id=run_id,
                action=action,
                actor=payload.get("actor", "operator"),
                approved=bool(payload.get("approved")),
            )
            return ApiResponse(200, decision.as_dict())
        if method == "POST" and suffix == "/rollback":
            reason = RollbackTriggerReason(payload.get("reason", RollbackTriggerReason.OPERATOR_REQUESTED.value))
            body = self.service.trigger_rollback(
                run_id=run_id,
                actor=payload.get("actor", "operator"),
                human_approved=payload.get("human_approved"),
                reason=reason,
            )
            return ApiResponse(200, body)

        return self._error(404, f"Route not found: {route}")

    @staticmethod
    def _match_run_route(route: str) -> tuple[str, str] | None:
        prefix = "/api/runs/"
        if not route.startswith(prefix):
            return None
        tail = route.removeprefix(prefix)
        if "/" in tail:
            run_id, suffix = tail.split("/", 1)
            return run_id, f"/{suffix}"
        return tail, ""

    @staticmethod
    def _error(status: int, message: str) -> ApiResponse:
        return ApiResponse(status, {"detail": message})


class ControlPlaneHTTPRequestHandler(BaseHTTPRequestHandler):
    """Simple JSON API handler for local control-plane UI."""

    api_factory: Callable[[], ControlPlaneAPI] | None = None

    def _api(self) -> ControlPlaneAPI:
        if not self.api_factory:
            raise RuntimeError("api_factory must be set on handler class")
        return self.api_factory()

    def do_GET(self) -> None:  # noqa: N802
        response = self._api().handle("GET", self.path)
        self._write_response(response)

    def do_POST(self) -> None:  # noqa: N802
        body_raw = self.rfile.read(int(self.headers.get("Content-Length", "0") or "0"))
        payload = json.loads(body_raw.decode("utf-8")) if body_raw else {}
        response = self._api().handle("POST", self.path, payload)
        self._write_response(response)

    def _write_response(self, response: ApiResponse) -> None:
        self.send_response(response.status)
        self.send_header("Content-Type", response.content_type)
        self.end_headers()
        if response.content_type == "application/json":
            self.wfile.write(json.dumps(response.body).encode("utf-8"))
        else:
            self.wfile.write(str(response.body).encode("utf-8"))


def run_server(*, host: str = "127.0.0.1", port: int = 8000, store_path: str | Path = ".migration_runs.json") -> None:
    api = ControlPlaneAPI(store_path=store_path)

    class Handler(ControlPlaneHTTPRequestHandler):
        @staticmethod
        def api_factory() -> ControlPlaneAPI:
            return api

    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Migration Control Plane running at http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    run_server()
