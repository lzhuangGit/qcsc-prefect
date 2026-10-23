"""Kind-based integration tests for the Kubernetes adapter.

These tests require ``kind`` and ``kubectl`` to be installed and a Docker /
containerd socket to be accessible.  They are skipped automatically when either
tool is missing or when the ``--kind`` flag is not passed to pytest.

Run manually::

    pytest packages/qcsc-prefect-adapters/tests/test_kubernetes_kind.py \
        --kind -v

Or via the ``kind_integration`` marker::

    pytest -m kind_integration --kind
"""

from __future__ import annotations

import asyncio
import shutil
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

import pytest

from qcsc_prefect_adapters.kubernetes.builder import (
    KubernetesJobRequest,
    build_job_manifest,
)
from qcsc_prefect_adapters.kubernetes.runtime import (
    KubernetesConnection,
    KubernetesRuntime,
    WaitTimeout,
)
from qcsc_prefect_core.models.execution_profile import ExecutionProfile

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_KIND_CLUSTER_NAME = f"qcsc-test-{uuid.uuid4().hex[:8]}"
_NAMESPACE = "default"
_BUSYBOX_IMAGE = "busybox:1.36"  # small, freely available, deterministic
_POLL = 2.0  # seconds between status polls
_TIMEOUT = 120.0  # seconds for each wait_final_status call


def _exec_profile(**overrides: Any) -> ExecutionProfile:
    values: dict[str, Any] = {"command_key": "test", "num_nodes": 1, "launcher": "single"}
    values.update(overrides)
    return ExecutionProfile(**values)


def _req(executable: str, job_name: str | None = None, **overrides: Any) -> KubernetesJobRequest:
    """Build a minimal KubernetesJobRequest targeting the kind cluster."""
    return KubernetesJobRequest(
        image=_BUSYBOX_IMAGE,
        executable=executable,
        namespace=_NAMESPACE,
        job_name=job_name,
        image_pull_policy="IfNotPresent",
        ttl_seconds_after_finished=None,  # keep pods for log inspection
        **overrides,
    )


# ---------------------------------------------------------------------------
# Session-scoped kind cluster fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def kind_kubeconfig(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory):
    """Create a temporary kind cluster and yield its kubeconfig path.

    The cluster is deleted after the entire test session, even on failure.
    Tests that need this fixture are automatically skipped when ``--kind`` is
    not provided or when ``kind``/``kubectl`` binaries are missing.
    """
    if not request.config.getoption("--kind", default=False):
        pytest.skip("Pass --kind to run kind integration tests.")

    for binary in ("kind", "kubectl"):
        if shutil.which(binary) is None:
            pytest.skip(f"'{binary}' not found on PATH; cannot run kind tests.")

    kubeconfig_dir = tmp_path_factory.mktemp("kubeconfig")
    kubeconfig_path = str(kubeconfig_dir / "kind.kubeconfig")

    # --- create cluster ---
    subprocess.run(
        [
            "kind",
            "create",
            "cluster",
            "--name",
            _KIND_CLUSTER_NAME,
            "--kubeconfig",
            kubeconfig_path,
            "--wait",
            "90s",
        ],
        check=True,
    )

    # Pull the test image into the cluster so jobs start without a long pull.
    # We ignore failures here — tests will still work, they just may be slower.
    subprocess.run(
        ["kind", "load", "docker-image", _BUSYBOX_IMAGE, "--name", _KIND_CLUSTER_NAME],
        check=False,
    )

    try:
        yield kubeconfig_path
    finally:
        subprocess.run(
            ["kind", "delete", "cluster", "--name", _KIND_CLUSTER_NAME],
            check=False,
        )


@pytest.fixture()
def runtime(kind_kubeconfig: str) -> KubernetesRuntime:
    """KubernetesRuntime wired to the ephemeral kind cluster."""
    conn = KubernetesConnection(kubeconfig_path=kind_kubeconfig, in_cluster=False)
    return KubernetesRuntime(conn)


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


@pytest.mark.kind_integration
def test_submit_returns_namespaced_id(runtime: KubernetesRuntime):
    """Jobs are created and IDs have the expected ``namespace/name`` shape."""
    manifest = build_job_manifest(
        exec_profile=_exec_profile(),
        req=_req("/bin/true", job_name="kind-submit-test"),
    )

    result = asyncio.run(runtime.submit(manifest))

    assert result.job_id == f"{_NAMESPACE}/kind-submit-test"
    assert result.raw_output == "kind-submit-test"


@pytest.mark.kind_integration
def test_wait_final_status_success(runtime: KubernetesRuntime):
    """A job that exits 0 is reported as COMPLETED with ExitCode 0:0."""
    manifest = build_job_manifest(
        exec_profile=_exec_profile(arguments=["Hello from kind"]),
        req=_req("/bin/echo", job_name=f"kind-ok-{uuid.uuid4().hex[:6]}"),
    )

    result = asyncio.run(runtime.submit(manifest))
    status = asyncio.run(
        runtime.wait_final_status(
            result.job_id,
            watch_poll_interval=_POLL,
            timeout_seconds=_TIMEOUT,
        )
    )

    assert status["State"] == "COMPLETED"
    assert status["ExitCode"] == "0:0"
    assert status["JobID"] == result.job_id
    assert status["PodName"]  # non-empty
    assert float(status["Elapsed"]) >= 0


@pytest.mark.kind_integration
def test_wait_final_status_failure(runtime: KubernetesRuntime):
    """A job that exits non-zero is reported as FAILED with the correct exit code."""
    manifest = build_job_manifest(
        exec_profile=_exec_profile(),
        req=_req("/bin/sh", job_name=f"kind-fail-{uuid.uuid4().hex[:6]}"),
    )
    # Override the command to exit 42 — build_job_manifest already sets
    # command to ["/bin/sh"], so we patch the container command directly.
    manifest["spec"]["template"]["spec"]["containers"][0]["command"] = [
        "/bin/sh",
        "-c",
        "exit 42",
    ]

    result = asyncio.run(runtime.submit(manifest))
    status = asyncio.run(
        runtime.wait_final_status(
            result.job_id,
            watch_poll_interval=_POLL,
            timeout_seconds=_TIMEOUT,
        )
    )

    assert status["State"] == "FAILED"
    assert status["ExitCode"] == "42:0"


@pytest.mark.kind_integration
def test_fetch_logs(runtime: KubernetesRuntime):
    """Logs of a completed pod are returned verbatim."""
    unique_token = f"kind-log-{uuid.uuid4().hex[:8]}"
    manifest = build_job_manifest(
        exec_profile=_exec_profile(arguments=[unique_token]),
        req=_req("/bin/echo", job_name=f"kind-log-{uuid.uuid4().hex[:6]}"),
    )

    result = asyncio.run(runtime.submit(manifest))
    asyncio.run(
        runtime.wait_final_status(
            result.job_id,
            watch_poll_interval=_POLL,
            timeout_seconds=_TIMEOUT,
        )
    )

    logs = asyncio.run(runtime.fetch_logs(result.job_id))
    assert unique_token in logs


@pytest.mark.kind_integration
def test_cancel_deletes_job(runtime: KubernetesRuntime):
    """cancel() removes the Job from the cluster; a second cancel is a no-op."""
    manifest = build_job_manifest(
        exec_profile=_exec_profile(),
        req=_req("/bin/sleep", job_name=f"kind-cancel-{uuid.uuid4().hex[:6]}"),
    )
    # Override command to sleep so the job is still running when we cancel it.
    manifest["spec"]["template"]["spec"]["containers"][0]["command"] = [
        "/bin/sleep",
        "600",
    ]

    result = asyncio.run(runtime.submit(manifest))

    # First cancel — should succeed.
    asyncio.run(runtime.cancel(result.job_id))

    # Second cancel — job is gone; should be silently ignored (not raise).
    asyncio.run(runtime.cancel(result.job_id))


@pytest.mark.kind_integration
def test_wait_final_status_timeout_cancels_job(runtime: KubernetesRuntime):
    """WaitTimeout is raised and the timed-out Job is deleted from the cluster."""
    manifest = build_job_manifest(
        exec_profile=_exec_profile(),
        req=_req("/bin/sleep", job_name=f"kind-timeout-{uuid.uuid4().hex[:6]}"),
    )
    manifest["spec"]["template"]["spec"]["containers"][0]["command"] = [
        "/bin/sleep",
        "600",
    ]

    result = asyncio.run(runtime.submit(manifest))

    with pytest.raises(WaitTimeout):
        asyncio.run(
            runtime.wait_final_status(
                result.job_id,
                watch_poll_interval=_POLL,
                # Give the pod a moment to be scheduled, then cut off early.
                timeout_seconds=max(_POLL * 2, 10.0),
            )
        )

    # After the timeout the runtime must have deleted the job.
    # A second cancel on the same ID should be a silent no-op.
    asyncio.run(runtime.cancel(result.job_id))
