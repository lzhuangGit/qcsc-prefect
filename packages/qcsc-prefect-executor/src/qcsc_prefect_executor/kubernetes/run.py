from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from prefect.artifacts import create_table_artifact
from prefect.logging import get_run_logger
from qcsc_prefect_adapters.kubernetes.builder import KubernetesJobRequest, build_job_manifest
from qcsc_prefect_adapters.kubernetes.runtime import KubernetesConnection, KubernetesRuntime
from qcsc_prefect_core.data_ref import DataRef, file_ref, get_resolver
from qcsc_prefect_core.models.execution_profile import ExecutionProfile

MAX_LOG_SIZE = 10_000


def truncate_log(text: str) -> str:
    """Truncate large log text to the configured maximum length."""

    if len(text) > MAX_LOG_SIZE:
        return text[:MAX_LOG_SIZE] + f"... (truncated {len(text) - MAX_LOG_SIZE} chars)"
    return text


@dataclass(frozen=True)
class KubernetesRunResult:
    """Normalized result returned by `run_kubernetes_job`.

    Attributes:
        job_id: ``"<namespace>/<name>"`` of the Kubernetes Job.
        exit_status: Container exit code (1 if unknown on failure).
        state: ``COMPLETED`` or ``FAILED``.
        job_status: Terminal status dictionary from the runtime.
        logs: Container logs captured after completion.
        outputs: References to the expected output files, in the order given to
            ``expected_outputs``. Empty unless the Job completed successfully.
    """

    job_id: str
    exit_status: int
    state: str
    job_status: dict[str, Any]
    logs: str
    outputs: tuple[DataRef, ...] = ()


def _verify_inputs(inputs: Sequence[DataRef]) -> None:
    for ref in inputs:
        get_resolver(ref).verify(ref)


def _collect_outputs(work_dir: Path, expected: Sequence[str | Path], checksum: bool):
    refs: list[DataRef] = []
    for item in expected:
        path = Path(item)
        path = path if path.is_absolute() else work_dir / path
        refs.append(file_ref(path.resolve(), checksum=checksum))
    return tuple(refs)


async def run_kubernetes_job(
    *,
    work_dir: Path,
    exec_profile: ExecutionProfile,
    req: KubernetesJobRequest,
    inputs: Sequence[DataRef] = (),
    expected_outputs: Sequence[str | Path] = (),
    checksum_outputs: bool = True,
    connection: KubernetesConnection | None = None,
    watch_poll_interval: float = 10.0,
    timeout_seconds: float | None = None,
    metrics_artifact_key: str = "kubernetes-job-metrics",
) -> KubernetesRunResult:
    """Run a command as a Kubernetes Job end-to-end on any cluster.

    Builds a ``batch/v1`` Job, creates it, waits for completion, logs the
    container output, and publishes a Prefect table artifact.

    Args:
        work_dir: Local directory where the submitted Job manifest is saved as
            ``job-manifest.json`` for debugging.
        exec_profile: Scheduler-independent execution profile.
        req: Kubernetes-specific request fields.
        inputs: Data references the Job reads. Each is verified (existence,
            size, checksum) before anything is submitted, so a missing or
            corrupt input fails fast instead of after the pod is scheduled.
        expected_outputs: Files the Job should produce, absolute or relative to
            ``work_dir``. After a successful run they are returned as
            `DataRef` values in `KubernetesRunResult.outputs`.
        checksum_outputs: Record SHA-256 digests for the output references.
        connection: Cluster connection. Defaults to in-cluster config when
            running in a pod, otherwise the default kubeconfig.
        watch_poll_interval: Poll interval in seconds.
        timeout_seconds: Optional maximum wait; the Job is deleted on timeout.
        metrics_artifact_key: Prefect artifact key for the metrics table.
    """
    logger = get_run_logger()
    work_dir.mkdir(parents=True, exist_ok=True)

    await asyncio.to_thread(_verify_inputs, inputs)

    manifest = build_job_manifest(exec_profile=exec_profile, req=req)
    (work_dir / "job-manifest.json").write_text(json.dumps(manifest, indent=2))
    runtime = KubernetesRuntime(connection)

    submit = await runtime.submit(manifest)
    final_status = await runtime.wait_final_status(
        submit.job_id,
        watch_poll_interval=watch_poll_interval,
        timeout_seconds=timeout_seconds,
    )

    logs = ""
    try:
        logs = await runtime.fetch_logs(submit.job_id)
    except Exception as e:  # logs are best-effort; the Job result is authoritative
        logger.warning(f"Could not fetch logs for {submit.job_id}: {e}")
    if logs:
        logger.info(truncate_log(logs))

    exit_code_text = str(final_status.get("ExitCode", "-1:0")).split(":", 1)[0]
    exit_status = int(exit_code_text) if exit_code_text.lstrip("-").isdigit() else -1

    artifact = {
        "job_id": submit.job_id,
        "state": final_status.get("State"),
        "exit_code": exit_status,
        "reason": final_status.get("Reason"),
        "elapsed_seconds": final_status.get("Elapsed"),
        "pod_name": final_status.get("PodName"),
        "node_name": final_status.get("NodeName"),
    }
    await create_table_artifact(
        table=[list(artifact.keys()), list(artifact.values())],
        key=metrics_artifact_key,
    )

    outputs: tuple[DataRef, ...] = ()
    if exit_status == 0 and final_status.get("State") == "COMPLETED":
        outputs = await asyncio.to_thread(
            _collect_outputs, work_dir, expected_outputs, checksum_outputs
        )

    return KubernetesRunResult(
        job_id=submit.job_id,
        exit_status=exit_status,
        state=str(final_status.get("State", "")),
        job_status=final_status,
        logs=logs,
        outputs=outputs,
    )
