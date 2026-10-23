from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest
from qcsc_prefect_adapters.kubernetes.builder import KubernetesJobRequest
from qcsc_prefect_adapters.kubernetes.runtime import SubmitResult
from qcsc_prefect_core.data_ref import DataIntegrityError, file_ref
from qcsc_prefect_core.models.execution_profile import ExecutionProfile
from qcsc_prefect_executor.kubernetes import run as run_mod


class _LoggerStub:
    def __init__(self) -> None:
        self.info_lines: list[str] = []
        self.warning_lines: list[str] = []

    def info(self, message: str) -> None:
        self.info_lines.append(message)

    def warning(self, message: str) -> None:
        self.warning_lines.append(message)


class _RuntimeStub:
    def __init__(self, connection, *, final_status, logs="", logs_error=None) -> None:
        self.connection = connection
        self.final_status = final_status
        self.logs = logs
        self.logs_error = logs_error
        self.manifest: dict[str, Any] | None = None

    async def submit(self, manifest):
        self.manifest = manifest
        return SubmitResult(job_id="ns/job1", raw_output="job1")

    async def wait_final_status(self, job_id, *, watch_poll_interval, timeout_seconds):
        return self.final_status

    async def fetch_logs(self, job_id):
        if self.logs_error:
            raise self.logs_error
        return self.logs


def _run(
    tmp_path: Path, monkeypatch, stub_kwargs, **run_kwargs
) -> tuple[Any, _LoggerStub, list, _RuntimeStub]:
    logger = _LoggerStub()
    artifacts: list[dict[str, Any]] = []
    holder: dict[str, _RuntimeStub] = {}

    def factory(connection):
        holder["rt"] = _RuntimeStub(connection, **stub_kwargs)
        return holder["rt"]

    async def fake_artifact(*, table, key):
        artifacts.append({"table": table, "key": key})

    monkeypatch.setattr(run_mod, "KubernetesRuntime", factory)
    monkeypatch.setattr(run_mod, "get_run_logger", lambda: logger)
    monkeypatch.setattr(run_mod, "create_table_artifact", fake_artifact)

    result = asyncio.run(
        run_mod.run_kubernetes_job(
            work_dir=tmp_path,
            exec_profile=ExecutionProfile(command_key="hello", num_nodes=1),
            req=KubernetesJobRequest(image="busybox", executable="/bin/echo", job_name="job1"),
            metrics_artifact_key="k8s-test",
            **run_kwargs,
        )
    )
    return result, logger, artifacts, holder["rt"]


def test_run_kubernetes_job_success(tmp_path: Path, monkeypatch):
    status = {"State": "COMPLETED", "ExitCode": "0:0", "Reason": "", "PodName": "p"}

    result, logger, artifacts, stub = _run(
        tmp_path, monkeypatch, {"final_status": status, "logs": "hello"}
    )

    assert result.job_id == "ns/job1"
    assert result.exit_status == 0
    assert result.state == "COMPLETED"
    assert result.logs == "hello"
    assert logger.info_lines == ["hello"]
    assert artifacts[0]["key"] == "k8s-test"
    assert "exit_code" in artifacts[0]["table"][0]
    assert json.loads((tmp_path / "job-manifest.json").read_text()) == stub.manifest


def test_run_kubernetes_job_failure_and_log_fetch_error(tmp_path: Path, monkeypatch):
    status = {"State": "FAILED", "ExitCode": "3:0"}

    result, logger, _artifacts, _stub = _run(
        tmp_path,
        monkeypatch,
        {"final_status": status, "logs_error": RuntimeError("pod gone")},
    )

    assert result.exit_status == 3
    assert result.state == "FAILED"
    assert result.logs == ""
    assert "pod gone" in logger.warning_lines[0]
    assert result.outputs == ()


def test_run_kubernetes_job_returns_output_refs_on_success(tmp_path: Path, monkeypatch):
    (tmp_path / "output.json").write_text("{}")
    status = {"State": "COMPLETED", "ExitCode": "0:0"}

    result, _logger, _artifacts, _stub = _run(
        tmp_path,
        monkeypatch,
        {"final_status": status},
        expected_outputs=["output.json"],
    )

    assert [ref.uri for ref in result.outputs] == [(tmp_path / "output.json").resolve().as_uri()]
    assert result.outputs[0].size == 2
    assert result.outputs[0].sha256 is not None


def test_run_kubernetes_job_missing_expected_output_raises(tmp_path: Path, monkeypatch):
    status = {"State": "COMPLETED", "ExitCode": "0:0"}

    with pytest.raises(FileNotFoundError):
        _run(
            tmp_path,
            monkeypatch,
            {"final_status": status},
            expected_outputs=["output.json"],
        )


def test_run_kubernetes_job_skips_outputs_on_failure(tmp_path: Path, monkeypatch):
    status = {"State": "FAILED", "ExitCode": "1:0"}

    result, _logger, _artifacts, _stub = _run(
        tmp_path,
        monkeypatch,
        {"final_status": status},
        expected_outputs=["output.json"],
    )

    assert result.outputs == ()


def test_run_kubernetes_job_verifies_inputs_before_submitting(tmp_path: Path, monkeypatch):
    src = tmp_path / "input.bin"
    src.write_bytes(b"abc")
    ref = file_ref(src)
    src.write_bytes(b"xyz")  # corrupt after the reference was taken
    status = {"State": "COMPLETED", "ExitCode": "0:0"}
    stubs: list[_RuntimeStub] = []

    original_init = _RuntimeStub.__init__

    def tracking_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        stubs.append(self)

    monkeypatch.setattr(_RuntimeStub, "__init__", tracking_init)

    with pytest.raises(DataIntegrityError):
        _run(tmp_path, monkeypatch, {"final_status": status}, inputs=[ref])

    assert all(stub.manifest is None for stub in stubs)


def test_run_kubernetes_job_accepts_valid_inputs(tmp_path: Path, monkeypatch):
    src = tmp_path / "input.bin"
    src.write_bytes(b"abc")
    status = {"State": "COMPLETED", "ExitCode": "0:0"}

    result, _logger, _artifacts, stub = _run(
        tmp_path, monkeypatch, {"final_status": status}, inputs=[file_ref(src)]
    )

    assert result.state == "COMPLETED"
    assert stub.manifest is not None
