from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Any

from qcsc_prefect_core.models.execution_profile import ExecutionProfile

CONTAINER_NAME = "main"
JOB_NAME_LABEL = "job-name"
MANAGED_BY_LABEL = "app.kubernetes.io/managed-by"
MANAGED_BY_VALUE = "qcsc-prefect"
WORK_VOLUME_NAME = "qcsc-work"
FLOW_RUN_LABEL = "qcsc-prefect/flow-run"
# Set in a flow pod by the flow launcher; compute Jobs it creates inherit them.
FLOW_RUN_ID_ENV = "QCSC_FLOW_RUN_ID"
OWNER_POD_NAME_ENV = "QCSC_OWNER_POD_NAME"
OWNER_POD_UID_ENV = "QCSC_OWNER_POD_UID"

_DNS_LABEL_MAX = 63
_JOB_NAME_MAX = 52  # Kubernetes appends a suffix to pod names; stay well under 63.


@dataclass(frozen=True)
class KubernetesJobRequest:
    """Cluster-agnostic request fields required to build a Kubernetes Job.

    Only standard ``batch/v1`` Job features are used, so the manifest is valid
    on any conformant cluster (EKS, GKE, AKS, OpenShift, k3s, kind, ...).

    Attributes:
        image: Container image that contains the executable.
        executable: Command run inside the container (the container entrypoint
            is overridden).
        namespace: Namespace the Job is created in.
        job_name: Optional explicit Job name. A unique name is generated when
            omitted.
        service_account_name: Optional service account for the pod.
        image_pull_policy: Optional pull policy (``Always``, ``IfNotPresent``,
            ``Never``).
        image_pull_secrets: Names of image pull secrets.
        cpu: Optional CPU request and limit, for example ``"2"`` or ``"500m"``.
        memory: Optional memory request and limit, for example ``"4Gi"``.
        extra_resources: Extra resource limits, for example
            ``{"nvidia.com/gpu": "1"}``.
        node_selector: Pod node selector.
        tolerations: Pod tolerations as Kubernetes API dictionaries.
        labels: Extra labels applied to the Job and pod.
        annotations: Extra annotations applied to the Job and pod.
        backoff_limit: Job retry count. Defaults to 0 so failures surface once.
        ttl_seconds_after_finished: Automatic Job cleanup delay. ``None``
            disables automatic cleanup.
        pod_spec_overrides: Extra keys merged into the pod spec for
            cluster-specific needs such as ``runtimeClassName`` or
            ``securityContext``.
        volumes: Raw pod ``volumes`` entries (any storage the cluster supports).
        volume_mounts: Raw container ``volumeMounts`` entries.
        work_volume_claim: Name of an existing ReadWriteMany PVC that holds the
            shared work directory. Mounted at ``work_mount_path``.
        work_mount_path: Absolute path where ``work_volume_claim`` is mounted.
            Use the same path the Prefect worker sees, so file paths are valid
            on both sides.
        working_dir: Optional explicit working directory for the container.
            Defaults to ``work_mount_path`` when ``work_volume_claim`` is set.
            When ``work_volume_claim`` is set it must be inside
            ``work_mount_path``, otherwise the directory does not exist on the
            shared volume in the pod.
        env_from_secrets: Names of Secrets whose keys are all exposed as
            environment variables (``envFrom``/``secretRef``).
        extra_env: Raw container ``env`` entries, for values that cannot be
            plain strings such as downward-API ``valueFrom`` references.
        owner_references: Raw ``ownerReferences`` for the Job. Kubernetes
            garbage-collects the Job when its owner is deleted. The owner must
            be in the same namespace as the Job.
    """

    image: str
    executable: str
    namespace: str = "default"
    job_name: str | None = None
    service_account_name: str | None = None
    image_pull_policy: str | None = None
    image_pull_secrets: tuple[str, ...] = ()
    cpu: str | None = None
    memory: str | None = None
    extra_resources: dict[str, str] = field(default_factory=dict)
    node_selector: dict[str, str] = field(default_factory=dict)
    tolerations: tuple[dict[str, Any], ...] = ()
    labels: dict[str, str] = field(default_factory=dict)
    annotations: dict[str, str] = field(default_factory=dict)
    backoff_limit: int = 0
    ttl_seconds_after_finished: int | None = 3600
    pod_spec_overrides: dict[str, Any] = field(default_factory=dict)
    volumes: tuple[dict[str, Any], ...] = ()
    volume_mounts: tuple[dict[str, Any], ...] = ()
    work_volume_claim: str | None = None
    work_mount_path: str | None = None
    working_dir: str | None = None
    env_from_secrets: tuple[str, ...] = ()
    extra_env: tuple[dict[str, Any], ...] = ()
    owner_references: tuple[dict[str, Any], ...] = ()


def pod_owner_reference(*, name: str, uid: str) -> dict[str, Any]:
    """Build an ``ownerReferences`` entry that points at a Pod.

    ``blockOwnerDeletion`` is deliberately omitted: setting it requires extra
    RBAC on clusters that enforce ``OwnerReferencesPermissionEnforcement``, and
    cascading deletion works without it.
    """

    return {"apiVersion": "v1", "kind": "Pod", "name": name, "uid": uid}


def sanitize_job_name(name: str) -> str:
    """Convert ``name`` to a valid, bounded DNS-1123 label."""

    cleaned = re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-")
    cleaned = cleaned[:_JOB_NAME_MAX].strip("-")
    if not cleaned:
        raise ValueError(f"Cannot derive a valid Kubernetes name from {name!r}.")
    return cleaned


def generate_job_name(prefix: str = "qcsc") -> str:
    """Return a unique Job name such as ``qcsc-3f9a1c2b``."""

    return f"{sanitize_job_name(prefix)[:40]}-{uuid.uuid4().hex[:8]}"


def parse_walltime_seconds(walltime: str) -> int:
    """Parse ``HH:MM:SS`` (or ``MM:SS``) into seconds."""

    parts = walltime.strip().split(":")
    if not 2 <= len(parts) <= 3 or not all(p.isdigit() for p in parts):
        raise ValueError(f"Invalid walltime {walltime!r}; expected HH:MM:SS.")
    seconds = 0
    for part in parts:
        seconds = seconds * 60 + int(part)
    return seconds


def build_kubernetes_command(*, exec_profile: ExecutionProfile, executable: str) -> list[str]:
    """Build the container ``command`` list without invoking a shell.

    Raises:
        ValueError: If the profile needs shell-level setup or multiple nodes,
            which a single-pod Job cannot honor.
    """

    unsupported: list[str] = []
    if exec_profile.modules:
        unsupported.append("modules")
    if exec_profile.pre_commands:
        unsupported.append("pre_commands")
    if unsupported:
        raise ValueError(
            "Kubernetes execution does not support "
            + " or ".join(unsupported)
            + ". Bake the setup into the container image instead."
        )
    if exec_profile.num_nodes != 1:
        raise ValueError(
            "Kubernetes execution runs a single pod; num_nodes must be 1 "
            f"(got {exec_profile.num_nodes})."
        )
    if not executable.strip():
        raise ValueError("Kubernetes executable must not be empty.")

    command: list[str] = []
    if exec_profile.launcher != "single":
        command.append(exec_profile.launcher)
        command.extend(exec_profile.mpi_options)
    command.append(executable)
    command.extend(exec_profile.arguments)
    return command


def build_job_manifest(
    *, exec_profile: ExecutionProfile, req: KubernetesJobRequest
) -> dict[str, Any]:
    """Build a ``batch/v1`` Job manifest as a plain dictionary."""

    if not req.image.strip():
        raise ValueError("Kubernetes image must not be empty.")

    name = sanitize_job_name(req.job_name) if req.job_name else generate_job_name()
    command = build_kubernetes_command(exec_profile=exec_profile, executable=req.executable)

    env = dict(exec_profile.environments)
    if exec_profile.ompthreads is not None:
        env.setdefault("OMP_NUM_THREADS", str(exec_profile.ompthreads))

    container: dict[str, Any] = {
        "name": CONTAINER_NAME,
        "image": req.image,
        "command": command,
    }
    env_entries: list[dict[str, Any]] = [{"name": str(k), "value": str(v)} for k, v in env.items()]
    env_entries.extend(dict(e) for e in req.extra_env)
    if env_entries:
        container["env"] = env_entries
    if req.env_from_secrets:
        container["envFrom"] = [{"secretRef": {"name": s}} for s in req.env_from_secrets]
    if req.image_pull_policy:
        container["imagePullPolicy"] = req.image_pull_policy

    resources: dict[str, str] = {}
    if req.cpu:
        resources["cpu"] = req.cpu
    if req.memory:
        resources["memory"] = req.memory
    resources.update(req.extra_resources)
    if resources:
        container["resources"] = {"requests": dict(resources), "limits": dict(resources)}

    volumes = [dict(v) for v in req.volumes]
    volume_mounts = [dict(m) for m in req.volume_mounts]
    if req.work_volume_claim:
        if not req.work_mount_path or not req.work_mount_path.startswith("/"):
            raise ValueError("work_volume_claim requires an absolute work_mount_path.")
        if req.working_dir and not PurePosixPath(req.working_dir).is_relative_to(
            PurePosixPath(req.work_mount_path)
        ):
            raise ValueError(
                f"working_dir {req.working_dir!r} is not inside work_mount_path "
                f"{req.work_mount_path!r}. The Job pod mounts the shared volume only at "
                f"{req.work_mount_path!r}, so that directory would not exist there. "
                "Use a work directory under the mount path."
            )
        volumes.append(
            {
                "name": WORK_VOLUME_NAME,
                "persistentVolumeClaim": {"claimName": req.work_volume_claim},
            }
        )
        volume_mounts.append({"name": WORK_VOLUME_NAME, "mountPath": req.work_mount_path})
        container["workingDir"] = req.working_dir or req.work_mount_path
    elif req.working_dir:
        container["workingDir"] = req.working_dir
    if volume_mounts:
        container["volumeMounts"] = volume_mounts

    pod_spec: dict[str, Any] = {"restartPolicy": "Never", "containers": [container]}
    if volumes:
        pod_spec["volumes"] = volumes
    if req.service_account_name:
        pod_spec["serviceAccountName"] = req.service_account_name
    if req.image_pull_secrets:
        pod_spec["imagePullSecrets"] = [{"name": s} for s in req.image_pull_secrets]
    if req.node_selector:
        pod_spec["nodeSelector"] = dict(req.node_selector)
    if req.tolerations:
        pod_spec["tolerations"] = [dict(t) for t in req.tolerations]
    pod_spec.update(req.pod_spec_overrides)

    labels = {MANAGED_BY_LABEL: MANAGED_BY_VALUE, **req.labels}
    metadata: dict[str, Any] = {"labels": labels}
    if req.annotations:
        metadata["annotations"] = dict(req.annotations)

    job_spec: dict[str, Any] = {
        "backoffLimit": req.backoff_limit,
        "template": {"metadata": dict(metadata), "spec": pod_spec},
    }
    if exec_profile.walltime is not None:
        job_spec["activeDeadlineSeconds"] = parse_walltime_seconds(exec_profile.walltime)
    if req.ttl_seconds_after_finished is not None:
        job_spec["ttlSecondsAfterFinished"] = req.ttl_seconds_after_finished

    job_metadata: dict[str, Any] = {"name": name, "namespace": req.namespace, **metadata}
    if req.owner_references:
        job_metadata["ownerReferences"] = [dict(o) for o in req.owner_references]

    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": job_metadata,
        "spec": job_spec,
    }
