from __future__ import annotations

import asyncio
import logging
import os
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from qcsc_prefect_adapters.kubernetes.builder import CONTAINER_NAME

_IN_CLUSTER_ENV = "KUBERNETES_SERVICE_HOST"


class SubmitError(RuntimeError):
    """Raised when Job creation fails."""


class WaitTimeout(RuntimeError):
    """Raised when waiting for a final Job status times out."""


class CancelError(RuntimeError):
    """Raised when Job deletion fails."""


@dataclass(frozen=True)
class KubernetesConnection:
    """How to reach a cluster. Every field is optional.

    Resolution order:

    1. ``kubeconfig_path`` or ``context`` set: load that kubeconfig/context.
    2. ``in_cluster=True``, or running inside a pod (``KUBERNETES_SERVICE_HOST``
       is set) and ``in_cluster`` is not ``False``: use the pod service account.
    3. Otherwise: the default kubeconfig (``$KUBECONFIG`` or ``~/.kube/config``)
       and its current context.

    Attributes:
        kubeconfig_path: Explicit kubeconfig file.
        context: Kubeconfig context name.
        in_cluster: Force (``True``) or forbid (``False``) in-cluster config.
    """

    kubeconfig_path: str | None = None
    context: str | None = None
    in_cluster: bool | None = None

    def use_in_cluster(self) -> bool:
        if self.kubeconfig_path or self.context:
            return False
        if self.in_cluster is not None:
            return self.in_cluster
        return _IN_CLUSTER_ENV in os.environ


@dataclass(frozen=True)
class SubmitResult:
    """Result of creating a Kubernetes Job.

    Attributes:
        job_id: ``"<namespace>/<name>"`` identifier of the created Job.
        raw_output: Name of the created Job.
    """

    job_id: str
    raw_output: str


def split_job_id(job_id: str) -> tuple[str, str]:
    """Split ``"<namespace>/<name>"`` into ``(namespace, name)``."""

    namespace, sep, name = job_id.partition("/")
    if not sep or not namespace or not name:
        raise ValueError(f"Invalid Kubernetes job id {job_id!r}; expected '<namespace>/<name>'.")
    return namespace, name


@asynccontextmanager
async def open_clients(connection: KubernetesConnection) -> AsyncIterator[tuple[Any, Any]]:
    """Yield ``(BatchV1Api, CoreV1Api)`` configured for ``connection``."""

    try:
        from kubernetes_asyncio import client, config
    except ImportError as exc:  # pragma: no cover - depends on optional extra
        raise ImportError(
            "Kubernetes support requires 'kubernetes_asyncio'. "
            "Install it with: pip install 'qcsc-prefect-adapters[kubernetes]'"
        ) from exc

    if connection.use_in_cluster():
        config.load_incluster_config()
    else:
        await config.load_kube_config(
            config_file=connection.kubeconfig_path, context=connection.context
        )

    api_client = client.ApiClient()
    try:
        yield client.BatchV1Api(api_client), client.CoreV1Api(api_client)
    finally:
        await api_client.close()


def _job_condition(job: Any, kind: str) -> Any | None:
    for cond in job.status.conditions or []:
        if cond.type == kind and str(cond.status) == "True":
            return cond
    return None


def _terminated_exit_code(pod: Any) -> int | None:
    for status in pod.status.container_statuses or []:
        if status.name == CONTAINER_NAME and status.state and status.state.terminated:
            return int(status.state.terminated.exit_code)
    return None


def _pick_pod(pods: list[Any]) -> Any | None:
    if not pods:
        return None
    return max(pods, key=lambda p: p.metadata.creation_timestamp.timestamp())


class KubernetesRuntime:
    """Async runtime wrapper around the Kubernetes Job API.

    Uses only the stable ``batch/v1`` and ``core/v1`` APIs, so it works with any
    conformant cluster. Workflow code usually calls
    `qcsc_prefect_executor.kubernetes.run.run_kubernetes_job`.
    """

    def __init__(self, connection: KubernetesConnection | None = None):
        self.connection = connection or KubernetesConnection()
        self.logger = logging.getLogger(self.__class__.__name__)

    async def submit(self, manifest: dict[str, Any]) -> SubmitResult:
        """Create the Job described by ``manifest``.

        Raises:
            SubmitError: If the API rejects the Job.
        """

        namespace = manifest["metadata"]["namespace"]
        try:
            async with open_clients(self.connection) as (batch, _core):
                job = await batch.create_namespaced_job(namespace=namespace, body=manifest)
        except Exception as e:
            raise SubmitError(
                f"Failed to create Job {manifest['metadata'].get('name')!r} in {namespace!r}"
            ) from e
        name = job.metadata.name
        return SubmitResult(job_id=f"{namespace}/{name}", raw_output=name)

    async def wait_final_status(
        self,
        job_id: str,
        *,
        watch_poll_interval: float = 10.0,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Poll until the Job completes or fails.

        Returns:
            A dictionary with ``JobID``, ``State`` (``COMPLETED`` or ``FAILED``),
            ``ExitCode`` (``"<code>:0"``), ``Reason``, ``PodName``, ``NodeName``
            and ``Elapsed`` (seconds).

        Raises:
            WaitTimeout: If ``timeout_seconds`` elapses first. The Job is
                deleted before raising.
        """

        namespace, name = split_job_id(job_id)
        started = time.monotonic()
        try:
            async with open_clients(self.connection) as (batch, core):
                while True:
                    job = await batch.read_namespaced_job_status(name=name, namespace=namespace)
                    complete = _job_condition(job, "Complete")
                    failed = _job_condition(job, "Failed")
                    if complete or failed:
                        pods = (
                            await core.list_namespaced_pod(
                                namespace=namespace, label_selector=f"job-name={name}"
                            )
                        ).items
                        return self._final_status(
                            job_id=job_id,
                            succeeded=bool(complete),
                            condition=complete or failed,
                            pod=_pick_pod(pods),
                            elapsed=time.monotonic() - started,
                        )

                    if timeout_seconds is not None and time.monotonic() - started > timeout_seconds:
                        raise WaitTimeout(
                            f"Job {job_id} did not finish within {timeout_seconds} seconds."
                        )
                    await asyncio.sleep(watch_poll_interval)
        except WaitTimeout:
            await self.cancel(job_id)
            raise
        except asyncio.CancelledError:
            await asyncio.shield(self.cancel(job_id))
            raise

    @staticmethod
    def _final_status(
        *, job_id: str, succeeded: bool, condition: Any, pod: Any | None, elapsed: float
    ) -> dict[str, Any]:
        exit_code = _terminated_exit_code(pod) if pod is not None else None
        if exit_code is None:
            exit_code = 0 if succeeded else 1
        return {
            "JobID": job_id,
            "State": "COMPLETED" if succeeded else "FAILED",
            "ExitCode": f"{exit_code}:0",
            "Reason": getattr(condition, "reason", None) or "",
            "PodName": pod.metadata.name if pod is not None else "",
            "NodeName": (pod.spec.node_name or "") if pod is not None else "",
            "Elapsed": f"{elapsed:.1f}",
        }

    async def fetch_logs(self, job_id: str) -> str:
        """Return container logs of the Job's most recent pod ("" if none)."""

        namespace, name = split_job_id(job_id)
        async with open_clients(self.connection) as (_batch, core):
            pods = (
                await core.list_namespaced_pod(
                    namespace=namespace, label_selector=f"job-name={name}"
                )
            ).items
            pod = _pick_pod(pods)
            if pod is None:
                return ""
            return await core.read_namespaced_pod_log(
                name=pod.metadata.name, namespace=namespace, container=CONTAINER_NAME
            )

    async def cancel(self, job_id: str) -> None:
        """Delete the Job and its pods.

        Raises:
            CancelError: If deletion fails for a reason other than "not found".
        """

        namespace, name = split_job_id(job_id)
        try:
            async with open_clients(self.connection) as (batch, _core):
                await batch.delete_namespaced_job(
                    name=name, namespace=namespace, propagation_policy="Foreground"
                )
        except Exception as e:
            if getattr(e, "status", None) == 404:
                return
            raise CancelError(f"Failed to delete Job {job_id}") from e
