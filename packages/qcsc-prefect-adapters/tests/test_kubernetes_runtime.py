from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace as NS

import pytest
from qcsc_prefect_adapters.kubernetes import runtime as rt


def _job(*conditions):
    return NS(
        metadata=NS(name="job1"),
        status=NS(conditions=[NS(type=t, status="True", reason=r) for t, r in conditions]),
    )


def _pod(name="job1-abc", exit_code=0, ts=1):
    return NS(
        metadata=NS(name=name, creation_timestamp=datetime.fromtimestamp(ts, tz=timezone.utc)),
        spec=NS(node_name="node-1"),
        status=NS(
            container_statuses=[NS(name="main", state=NS(terminated=NS(exit_code=exit_code)))]
        ),
    )


class _ApiError(Exception):
    def __init__(self, status):
        self.status = status


class FakeBatch:
    def __init__(self, jobs):
        self.jobs = list(jobs)
        self.created = []
        self.deleted = []
        self.delete_error = None

    async def create_namespaced_job(self, namespace, body):
        self.created.append((namespace, body))
        return NS(metadata=NS(name=body["metadata"]["name"]))

    async def read_namespaced_job_status(self, name, namespace):
        return self.jobs.pop(0) if len(self.jobs) > 1 else self.jobs[0]

    async def delete_namespaced_job(self, name, namespace, propagation_policy):
        if self.delete_error:
            raise self.delete_error
        self.deleted.append((namespace, name, propagation_policy))


class FakeCore:
    def __init__(self, pods, log="hello"):
        self.pods = pods
        self.log = log

    async def list_namespaced_pod(self, namespace, label_selector):
        assert label_selector == "job-name=job1"
        return NS(items=self.pods)

    async def read_namespaced_pod_log(self, name, namespace, container):
        assert container == "main"
        return self.log


def _install(monkeypatch, batch, core):
    @asynccontextmanager
    async def fake_open_clients(connection):
        yield batch, core

    monkeypatch.setattr(rt, "open_clients", fake_open_clients)


MANIFEST = {"metadata": {"name": "job1", "namespace": "ns"}}


def test_submit_returns_namespaced_id(monkeypatch):
    batch = FakeBatch([_job()])
    _install(monkeypatch, batch, FakeCore([]))

    result = asyncio.run(rt.KubernetesRuntime().submit(MANIFEST))

    assert result.job_id == "ns/job1"
    assert batch.created == [("ns", MANIFEST)]


def test_submit_wraps_api_errors(monkeypatch):
    batch = FakeBatch([_job()])

    async def boom(namespace, body):
        raise _ApiError(403)

    batch.create_namespaced_job = boom
    _install(monkeypatch, batch, FakeCore([]))

    with pytest.raises(rt.SubmitError):
        asyncio.run(rt.KubernetesRuntime().submit(MANIFEST))


def test_wait_final_status_success_after_polling(monkeypatch):
    batch = FakeBatch([_job(), _job(("Complete", None))])
    _install(monkeypatch, batch, FakeCore([_pod(exit_code=0)]))

    status = asyncio.run(
        rt.KubernetesRuntime().wait_final_status("ns/job1", watch_poll_interval=0.01)
    )

    assert status["State"] == "COMPLETED"
    assert status["ExitCode"] == "0:0"
    assert status["PodName"] == "job1-abc"
    assert status["NodeName"] == "node-1"


def test_wait_final_status_failure_uses_container_exit_code(monkeypatch):
    batch = FakeBatch([_job(("Failed", "BackoffLimitExceeded"))])
    _install(monkeypatch, batch, FakeCore([_pod("old", 9, ts=1), _pod("new", 7, ts=2)]))

    status = asyncio.run(rt.KubernetesRuntime().wait_final_status("ns/job1"))

    assert status["State"] == "FAILED"
    assert status["ExitCode"] == "7:0"
    assert status["Reason"] == "BackoffLimitExceeded"
    assert status["PodName"] == "new"


def test_wait_final_status_timeout_deletes_job(monkeypatch):
    batch = FakeBatch([_job()])
    _install(monkeypatch, batch, FakeCore([]))

    with pytest.raises(rt.WaitTimeout):
        asyncio.run(
            rt.KubernetesRuntime().wait_final_status(
                "ns/job1", watch_poll_interval=0.01, timeout_seconds=0.02
            )
        )

    assert batch.deleted == [("ns", "job1", "Foreground")]


def test_fetch_logs(monkeypatch):
    _install(monkeypatch, FakeBatch([_job()]), FakeCore([_pod()], log="out"))
    assert asyncio.run(rt.KubernetesRuntime().fetch_logs("ns/job1")) == "out"

    _install(monkeypatch, FakeBatch([_job()]), FakeCore([]))
    assert asyncio.run(rt.KubernetesRuntime().fetch_logs("ns/job1")) == ""


def test_cancel_ignores_not_found_but_raises_other_errors(monkeypatch):
    batch = FakeBatch([_job()])
    _install(monkeypatch, batch, FakeCore([]))

    asyncio.run(rt.KubernetesRuntime().cancel("ns/job1"))
    assert batch.deleted == [("ns", "job1", "Foreground")]

    batch.delete_error = _ApiError(404)
    asyncio.run(rt.KubernetesRuntime().cancel("ns/job1"))

    batch.delete_error = _ApiError(500)
    with pytest.raises(rt.CancelError):
        asyncio.run(rt.KubernetesRuntime().cancel("ns/job1"))


def test_split_job_id_validation():
    assert rt.split_job_id("a/b") == ("a", "b")
    for bad in ("ab", "/b", "a/"):
        with pytest.raises(ValueError):
            rt.split_job_id(bad)


def test_connection_resolution(monkeypatch):
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    assert rt.KubernetesConnection().use_in_cluster() is False
    assert rt.KubernetesConnection(in_cluster=True).use_in_cluster() is True

    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    assert rt.KubernetesConnection().use_in_cluster() is True
    assert rt.KubernetesConnection(in_cluster=False).use_in_cluster() is False
    assert rt.KubernetesConnection(context="dev").use_in_cluster() is False
    assert rt.KubernetesConnection(kubeconfig_path="/tmp/kc").use_in_cluster() is False
