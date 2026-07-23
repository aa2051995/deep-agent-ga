"""Unit tests for the deep-research Helm chart.

Render the chart with `helm template` and assert that the env vars wired into the
apiserver / worker / ui containers exactly match the names and connection formats
the application code reads:

  - stream-backend/app/main.py        -> STREAM_BACKEND_STORE / _POSTGRES_URI / _RUNNER_BACKEND / _CELERY_BROKER_URL
  - stream-backend/app/event_bus.py   -> STREAM_BACKEND_EVENT_BROKER / RABBITMQ_STREAM_URL
  - stream-backend/worker/celery_app.py -> STREAM_BACKEND_CELERY_BROKER_URL / _CELERY_QUEUE
  - ui/src/stream.ts                  -> window.__API_URL__ (fed from API_URL)
"""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

CHART_DIR = Path(__file__).resolve().parents[1]

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="helm CLI not installed"
)


def _render(*extra_args: str) -> list[dict]:
    """Render the chart and return all manifests as dicts."""
    result = subprocess.run(
        ["helm", "template", "dr", str(CHART_DIR), *extra_args],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"helm template failed:\n{result.stderr}"
    return [doc for doc in yaml.safe_load_all(result.stdout) if doc]


def _by(manifests: list[dict], kind: str, component: str) -> dict:
    for m in manifests:
        if m.get("kind") != kind:
            continue
        labels = m.get("metadata", {}).get("labels", {})
        if labels.get("app.kubernetes.io/component") == component:
            return m
    raise AssertionError(f"no {kind} with component={component}")


def _env_map(deployment: dict) -> dict[str, dict]:
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    return {e["name"]: e for e in container.get("env", [])}


@pytest.fixture(scope="module")
def manifests() -> list[dict]:
    return _render()


def test_all_components_present(manifests):
    kinds = [(m["kind"], m["metadata"]["labels"].get("app.kubernetes.io/component"))
             for m in manifests if "labels" in m.get("metadata", {})]
    assert ("StatefulSet", "postgres") in kinds
    assert ("StatefulSet", "rabbitmq") in kinds
    assert ("Deployment", "apiserver") in kinds
    assert ("Deployment", "worker") in kinds
    assert ("Deployment", "ui") in kinds


def test_autoscaling_for_each_stateless_tier(manifests):
    components = {
        m["metadata"]["labels"].get("app.kubernetes.io/component")
        for m in manifests
        if m.get("kind") == "HorizontalPodAutoscaler"
    }
    assert {"apiserver", "worker", "ui"} <= components


def test_apiserver_backend_wiring(manifests):
    env = _env_map(_by(manifests, "Deployment", "apiserver"))
    assert env["STREAM_BACKEND_STORE"]["value"] == "postgres"
    assert env["STREAM_BACKEND_EVENT_BROKER"]["value"] == "rabbitmq"
    assert env["STREAM_BACKEND_RUNNER_BACKEND"]["value"] == "celery"
    # Connection strings target the in-cluster Service DNS names.
    assert env["STREAM_BACKEND_POSTGRES_URI"]["value"].startswith(
        "postgresql://postgres:postgres@dr-deep-research-postgres:5432/"
    )
    assert env["RABBITMQ_STREAM_URL"]["value"] == (
        "rabbitmq-stream://guest:guest@dr-deep-research-rabbitmq:5552/"
    )
    assert env["STREAM_BACKEND_CELERY_BROKER_URL"]["value"] == (
        "amqp://guest:guest@dr-deep-research-rabbitmq:5672//"
    )


def test_worker_shares_backend_wiring_and_runs_celery(manifests):
    dep = _by(manifests, "Deployment", "worker")
    env = _env_map(dep)
    # Same store/broker as the apiserver so both see the same runs and events.
    assert env["STREAM_BACKEND_POSTGRES_URI"]["value"].startswith("postgresql://")
    assert env["STREAM_BACKEND_CELERY_BROKER_URL"]["value"].startswith("amqp://")
    command = dep["spec"]["template"]["spec"]["containers"][0]["command"]
    assert "celery" in command and "worker.celery_app.celery_app" in command
    assert "--queues=deep-research-runs" in command


def test_api_keys_come_from_secret(manifests):
    env = _env_map(_by(manifests, "Deployment", "apiserver"))
    for key in ("TAVILY_API_KEY", "ANTHROPIC_API_KEY", "OPENAI_API_KEY", "AWS_SECRET_ACCESS_KEY"):
        ref = env[key]["valueFrom"]["secretKeyRef"]
        assert ref["name"] == "dr-deep-research-secrets"
        assert ref["key"] == key


def test_ui_points_at_apiserver_and_serves_same_origin(manifests):
    env = _env_map(_by(manifests, "Deployment", "ui"))
    assert env["API_URL"]["value"] == "/api"
    assert env["APISERVER_UPSTREAM"]["value"] == "http://dr-deep-research-apiserver:8123"


def test_rabbitmq_stream_advertised_host_is_service_dns(manifests):
    cfg = next(
        m for m in manifests
        if m.get("kind") == "ConfigMap"
        and m["metadata"]["labels"].get("app.kubernetes.io/component") == "rabbitmq"
    )
    assert "stream.advertised_host = dr-deep-research-rabbitmq" in cfg["data"]["rabbitmq.conf"]
    assert "rabbitmq_stream" in cfg["data"]["enabled_plugins"]


def test_external_postgres_replaces_statefulset():
    manifests = _render(
        "--set", "postgres.external.enabled=true",
        "--set-string", "postgres.external.connectionUrl=postgresql://u:p@rds.aws:5432/db?sslmode=require",
    )
    pg_statefulsets = [
        m for m in manifests
        if m.get("kind") == "StatefulSet"
        and m["metadata"]["labels"].get("app.kubernetes.io/component") == "postgres"
    ]
    assert not pg_statefulsets, "external postgres must not deploy an in-cluster StatefulSet"
    env = _env_map(_by(manifests, "Deployment", "apiserver"))
    assert env["STREAM_BACKEND_POSTGRES_URI"]["value"] == (
        "postgresql://u:p@rds.aws:5432/db?sslmode=require"
    )
