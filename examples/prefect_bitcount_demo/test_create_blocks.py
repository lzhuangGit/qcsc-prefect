from __future__ import annotations

from examples.prefect_bitcount_demo import create_blocks as mod


def test_resolve_legacy_tutorial_asset_creation_defaults_to_false():
    assert (
        mod._resolve_legacy_tutorial_asset_creation(
            requested=None,
            tutorial_variable_name=None,
            bitcounter_block_name=None,
        )
        is False
    )


def test_resolve_legacy_tutorial_asset_creation_infers_from_explicit_names():
    assert (
        mod._resolve_legacy_tutorial_asset_creation(
            requested=None,
            tutorial_variable_name="miyabi-tutorial",
            bitcounter_block_name=None,
        )
        is True
    )


def test_resolve_legacy_tutorial_asset_creation_honors_explicit_false():
    assert (
        mod._resolve_legacy_tutorial_asset_creation(
            requested=False,
            tutorial_variable_name="miyabi-tutorial",
            bitcounter_block_name="miyabi-tutorial",
        )
        is False
    )


def test_normalize_bool_accepts_common_string_values():
    assert mod._normalize_bool("true") is True
    assert mod._normalize_bool("0") is False
    assert mod._normalize_bool("") is None


def test_main_creates_kubernetes_blocks(monkeypatch, tmp_path):
    saved_blocks: dict[str, object] = {}
    saved_variables: dict[str, object] = {}

    def fake_block_save(self, name, overwrite=True):
        saved_blocks[name] = self

    def fake_set_variable(variable_name, shots, work_dir):
        saved_variables[variable_name] = {"shots": shots, "work_dir": work_dir}

    monkeypatch.setattr(mod, "_set_variable", fake_set_variable)
    monkeypatch.setattr(mod, "_register_block_types", lambda *args: None)

    from qcsc_prefect_blocks.common.blocks import (
        CommandBlock,
        ExecutionProfileBlock,
        HPCProfileBlock,
    )

    monkeypatch.setattr(CommandBlock, "save", fake_block_save)
    monkeypatch.setattr(ExecutionProfileBlock, "save", fake_block_save)
    monkeypatch.setattr(HPCProfileBlock, "save", fake_block_save)

    test_args = [
        "create_blocks.py",
        "--hpc-target",
        "kubernetes",
        "--kubernetes-image",
        "qcsc-hpc-base:v1",
        "--kubernetes-namespace",
        "qcsc-ns",
        "--kubernetes-work-volume-claim",
        "qcsc-shared-pvc",
        "--kubernetes-work-mount-path",
        "/data/qcsc-prefect",
        "--work-dir",
        str(tmp_path),
    ]
    monkeypatch.setattr("sys.argv", test_args)

    mod.main()

    assert "cmd-bitcount-hist" in saved_blocks
    assert "exec-bitcount-k8s" in saved_blocks
    assert "hpc-k8s-bitcount" in saved_blocks
    assert "k8s-bitcount-options" in saved_variables

    hpc_block = saved_blocks["hpc-k8s-bitcount"]
    assert getattr(hpc_block, "hpc_target") == "kubernetes"
    assert getattr(hpc_block, "kubernetes_image") == "qcsc-hpc-base:v1"
    assert getattr(hpc_block, "kubernetes_namespace") == "qcsc-ns"
    assert getattr(hpc_block, "kubernetes_work_volume_claim") == "qcsc-shared-pvc"
    assert getattr(hpc_block, "kubernetes_work_mount_path") == "/data/qcsc-prefect"

    exec_block = saved_blocks["exec-bitcount-k8s"]
    assert getattr(exec_block, "launcher") == "single"
    assert getattr(exec_block, "num_nodes") == 1
    assert getattr(exec_block, "mpiprocs") == 1
