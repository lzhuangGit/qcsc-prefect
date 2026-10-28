"""Rules that tie a flow pod to the ``HPCProfileBlock`` it was launched with.

A flow that fans out to compute Jobs must run in the same cluster, namespace and
shared volume as those Jobs. The flow launcher therefore reads the pod settings
from an ``HPCProfileBlock`` instead of taking them as flags, and records the
block name and a fingerprint of those settings in the flow pod. Inside the pod,
`verify_flow_pod_profile` rejects any job that uses another block, and rejects
a block that was edited after the manifest was rendered.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from typing import Any

HPC_PROFILE_BLOCK_ENV = "QCSC_HPC_PROFILE_BLOCK"
HPC_PROFILE_FINGERPRINT_ENV = "QCSC_HPC_PROFILE_FINGERPRINT"
HPC_PROFILE_BLOCK_ANNOTATION = "qcsc-prefect/hpc-profile-block"
HPC_PROFILE_FINGERPRINT_ANNOTATION = "qcsc-prefect/hpc-profile-fingerprint"
PREFECT_API_URL_ANNOTATION = "qcsc-prefect/prefect-api-url"


def flow_pod_settings(hpc_block: Any) -> dict[str, Any]:
    """Return the block fields that are frozen into the flow pod's manifest.

    Fields that only shape the compute Jobs (CPU, memory, labels, and so on) are
    left out: they are read when each compute Job is created, so editing them
    between render and run is harmless.
    """

    return {
        "image": getattr(hpc_block, "kubernetes_image", None),
        "namespace": getattr(hpc_block, "kubernetes_namespace", None),
        "service_account_name": getattr(hpc_block, "kubernetes_service_account_name", None),
        "image_pull_policy": getattr(hpc_block, "kubernetes_image_pull_policy", None),
        "image_pull_secrets": list(getattr(hpc_block, "kubernetes_image_pull_secrets", ()) or ()),
        "node_selector": dict(getattr(hpc_block, "kubernetes_node_selector", {}) or {}),
        "tolerations": [dict(t) for t in getattr(hpc_block, "kubernetes_tolerations", ()) or ()],
        "work_volume_claim": getattr(hpc_block, "kubernetes_work_volume_claim", None),
        "work_mount_path": getattr(hpc_block, "kubernetes_work_mount_path", None),
    }


def settings_fingerprint(settings: Mapping[str, Any]) -> str:
    """Return a short, stable hash of ``settings``."""

    canonical = json.dumps(settings, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def validate_flow_block(hpc_block: Any, block_name: str) -> None:
    """Reject a block that cannot host a flow pod next to its compute Jobs.

    Raises:
        ValueError: If the block is not a Kubernetes target, or lacks the image or
            shared volume that the batch scheme requires.
    """

    if getattr(hpc_block, "hpc_target", None) != "kubernetes":
        raise ValueError(
            f"HPCProfileBlock '{block_name}' has hpc_target="
            f"{getattr(hpc_block, 'hpc_target', None)!r}; the flow launcher needs 'kubernetes'."
        )
    settings = flow_pod_settings(hpc_block)
    missing = [
        field
        for field, key in (
            ("kubernetes_image", "image"),
            ("kubernetes_work_volume_claim", "work_volume_claim"),
            ("kubernetes_work_mount_path", "work_mount_path"),
        )
        if not settings[key]
    ]
    if missing:
        raise ValueError(
            f"HPCProfileBlock '{block_name}' must set {', '.join(missing)}: the flow pod and its "
            "compute Jobs must run on the same cluster with the same shared volume."
        )


def verify_flow_pod_profile(hpc_block: Any, block_name: str) -> None:
    """Inside a flow pod, require the launcher's block and its rendered settings.

    Does nothing when ``QCSC_HPC_PROFILE_BLOCK`` is not set, that is, outside a
    pod created by the flow launcher.

    Raises:
        ValueError: If ``block_name`` is not the block the flow pod was launched
            with, or that block changed after the manifest was rendered.
    """

    expected_block = os.environ.get(HPC_PROFILE_BLOCK_ENV)
    if not expected_block:
        return
    if block_name != expected_block:
        raise ValueError(
            f"This flow was launched with hpc_profile_block '{expected_block}' and every job must "
            f"use it, but a job asked for '{block_name}'. The flow pod and its compute Jobs must "
            "share one cluster and volume."
        )
    expected_fingerprint = os.environ.get(HPC_PROFILE_FINGERPRINT_ENV)
    if not expected_fingerprint:
        return
    current = flow_pod_settings(hpc_block)
    if settings_fingerprint(current) != expected_fingerprint:
        raise ValueError(
            f"HPCProfileBlock '{block_name}' changed after this flow Job was rendered "
            f"(fingerprint {expected_fingerprint} then, {settings_fingerprint(current)} now). "
            "Re-run the flow launcher to render a new manifest. Current flow pod settings: "
            f"{json.dumps(current, sort_keys=True, default=str)}"
        )
