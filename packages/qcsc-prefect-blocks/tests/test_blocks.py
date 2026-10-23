from __future__ import annotations

import pytest
from pydantic import ValidationError
from qcsc_prefect_blocks.common.blocks import (
    CommandBlock,
    ExecutionProfileBlock,
    HPCProfileBlock,
)
from qcsc_prefect_blocks.kubernetes import (
    CommandBlock as K8sCommandBlock,
)
from qcsc_prefect_blocks.kubernetes import (
    ExecutionProfileBlock as K8sExecutionProfileBlock,
)
from qcsc_prefect_blocks.kubernetes import (
    HPCProfileBlock as K8sHPCProfileBlock,
)


def test_hpc_profile_block_kubernetes_fields():
    block = HPCProfileBlock(
        hpc_target="kubernetes",
        kubernetes_image="qcsc-bitcount:latest",
        kubernetes_namespace="qcsc-test",
        kubernetes_cpu="2",
        kubernetes_memory="4Gi",
        kubernetes_work_volume_claim="shared-pvc",
        kubernetes_work_mount_path="/shared/work",
        executable_map={"bitcount_hist": "/usr/local/bin/get_counts_hist"},
    )
    assert block.hpc_target == "kubernetes"
    assert block.kubernetes_image == "qcsc-bitcount:latest"
    assert block.kubernetes_namespace == "qcsc-test"
    assert block.kubernetes_cpu == "2"
    assert block.kubernetes_memory == "4Gi"
    assert block.kubernetes_work_volume_claim == "shared-pvc"
    assert block.kubernetes_work_mount_path == "/shared/work"
    assert block.executable_map["bitcount_hist"] == "/usr/local/bin/get_counts_hist"


def test_hpc_profile_block_invalid_target_raises():
    with pytest.raises(ValidationError):
        HPCProfileBlock(hpc_target="unknown_target")  # type: ignore


def test_kubernetes_module_reexports():
    assert K8sCommandBlock is CommandBlock
    assert K8sExecutionProfileBlock is ExecutionProfileBlock
    assert K8sHPCProfileBlock is HPCProfileBlock
