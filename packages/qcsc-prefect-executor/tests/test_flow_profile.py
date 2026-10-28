from __future__ import annotations

from types import SimpleNamespace

import pytest
from qcsc_prefect_executor.kubernetes.flow_profile import (
    HPC_PROFILE_BLOCK_ENV,
    HPC_PROFILE_FINGERPRINT_ENV,
    flow_pod_settings,
    settings_fingerprint,
    verify_flow_pod_profile,
)


def _block(**overrides) -> SimpleNamespace:
    values = {
        "hpc_target": "kubernetes",
        "kubernetes_image": "img:1",
        "kubernetes_namespace": "qcsc",
        "kubernetes_service_account_name": "sa",
        "kubernetes_image_pull_policy": "IfNotPresent",
        "kubernetes_image_pull_secrets": [],
        "kubernetes_node_selector": {"arch": "arm64"},
        "kubernetes_tolerations": [],
        "kubernetes_work_volume_claim": "ldata-pvc",
        "kubernetes_work_mount_path": "/ldata",
        "kubernetes_cpu": "1",
        "kubernetes_memory": "1Gi",
        "kubernetes_labels": {},
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _in_flow_pod(monkeypatch, block: SimpleNamespace, name: str = "hpc-k8s") -> None:
    monkeypatch.setenv(HPC_PROFILE_BLOCK_ENV, name)
    monkeypatch.setenv(HPC_PROFILE_FINGERPRINT_ENV, settings_fingerprint(flow_pod_settings(block)))


def test_fingerprint_is_stable_and_order_independent():
    first = settings_fingerprint({"a": 1, "b": {"x": 1, "y": 2}})
    second = settings_fingerprint({"b": {"y": 2, "x": 1}, "a": 1})

    assert first == second
    assert len(first) == 16


@pytest.mark.parametrize(
    "change",
    [
        {"kubernetes_image": "img:2"},
        {"kubernetes_namespace": "other"},
        {"kubernetes_service_account_name": "other"},
        {"kubernetes_node_selector": {"arch": "amd64"}},
        {"kubernetes_tolerations": [{"key": "k"}]},
        {"kubernetes_work_volume_claim": "other-pvc"},
        {"kubernetes_work_mount_path": "/other"},
    ],
)
def test_fingerprint_changes_when_a_flow_pod_field_changes(change):
    assert settings_fingerprint(flow_pod_settings(_block())) != settings_fingerprint(
        flow_pod_settings(_block(**change))
    )


@pytest.mark.parametrize(
    "change",
    [{"kubernetes_cpu": "8"}, {"kubernetes_memory": "32Gi"}, {"kubernetes_labels": {"a": "b"}}],
)
def test_fingerprint_ignores_compute_job_only_fields(change):
    assert settings_fingerprint(flow_pod_settings(_block())) == settings_fingerprint(
        flow_pod_settings(_block(**change))
    )


def test_verify_is_a_no_op_outside_a_flow_pod(monkeypatch):
    monkeypatch.delenv(HPC_PROFILE_BLOCK_ENV, raising=False)

    verify_flow_pod_profile(_block(), "any-block")


def test_verify_accepts_the_launch_block_when_unchanged(monkeypatch):
    block = _block()
    _in_flow_pod(monkeypatch, block)

    verify_flow_pod_profile(block, "hpc-k8s")


def test_verify_rejects_a_different_block_name(monkeypatch):
    block = _block()
    _in_flow_pod(monkeypatch, block)

    with pytest.raises(ValueError, match="launched with hpc_profile_block 'hpc-k8s'"):
        verify_flow_pod_profile(block, "other-block")


def test_verify_rejects_a_block_edited_after_render(monkeypatch):
    _in_flow_pod(monkeypatch, _block())
    edited = _block(kubernetes_work_volume_claim="other-pvc")

    with pytest.raises(ValueError, match="changed after this flow Job was rendered") as excinfo:
        verify_flow_pod_profile(edited, "hpc-k8s")

    assert "other-pvc" in str(excinfo.value)
    assert "Re-run the flow launcher" in str(excinfo.value)


def test_verify_allows_editing_compute_job_only_fields(monkeypatch):
    _in_flow_pod(monkeypatch, _block())

    verify_flow_pod_profile(_block(kubernetes_cpu="64"), "hpc-k8s")
