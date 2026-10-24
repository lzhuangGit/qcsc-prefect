from __future__ import annotations

from typing import Any, Literal

from prefect.blocks.core import Block
from pydantic import Field


class CommandBlock(Block):
    """Command definition (what to run). Common for all HPC systems."""

    _block_type_name = "HPC Command"
    _block_type_slug = "hpc_command"

    command_name: str = Field(title="Command Name")
    executable_key: str = Field(title="Executable Key")
    description: str | None = Field(default=None, title="Description")
    default_args: list[str] = Field(default_factory=list, title="Default Args")


class ExecutionProfileBlock(Block):
    """Execution baseline (how to run). Common for all HPC systems."""

    _block_type_name = "Execution Profile"
    _block_type_slug = "execution_profile"

    profile_name: str = Field(title="Profile Name")
    command_name: str = Field(title="Command Name")
    resource_class: Literal["cpu", "gpu"] = Field(default="cpu", title="Resource Class")

    num_nodes: int = Field(default=1, gt=0, title="Nodes")
    mpiprocs: int = Field(default=1, gt=0, title="MPI Procs per Node")
    ompthreads: int | None = Field(default=None, gt=0, title="OMP Threads")
    walltime: str = Field(default="00:05:00", title="Walltime (HH:MM:SS)")
    launcher: Literal["single", "srun", "mpirun", "mpiexec", "mpiexec.hydra"] = Field(
        default="single",
        title="Launcher",
    )
    mpi_options: list[str] = Field(default_factory=list, title="MPI Options")
    modules: list[str] = Field(default_factory=list, title="Modules")
    pre_commands: list[str] = Field(default_factory=list, title="Pre Commands")
    environments: dict[str, str] = Field(default_factory=dict, title="Environment Variables")


class HPCProfileBlock(Block):
    """Execution target profile for local and supported HPC runtimes."""

    _block_type_name = "HPC Profile"
    _block_type_slug = "hpc_profile"

    hpc_target: Literal["local", "miyabi", "fugaku", "slurm", "kubernetes"] = Field(
        default="miyabi", title="Execution Target"
    )

    # Kubernetes-specific options (ignored for other systems)
    kubernetes_image: str | None = Field(default=None, title="Kubernetes: Container Image")
    kubernetes_namespace: str = Field(default="default", title="Kubernetes: Namespace")
    kubernetes_service_account_name: str | None = Field(
        default=None, title="Kubernetes: Service Account Name"
    )
    kubernetes_image_pull_policy: str | None = Field(
        default="IfNotPresent", title="Kubernetes: Image Pull Policy"
    )
    kubernetes_image_pull_secrets: list[str] = Field(
        default_factory=list, title="Kubernetes: Image Pull Secrets"
    )
    kubernetes_cpu: str | None = Field(default=None, title="Kubernetes: CPU Request and Limit")
    kubernetes_memory: str | None = Field(
        default=None, title="Kubernetes: Memory Request and Limit"
    )
    kubernetes_extra_resources: dict[str, str] = Field(
        default_factory=dict, title="Kubernetes: Extra Resources"
    )
    kubernetes_node_selector: dict[str, str] = Field(
        default_factory=dict, title="Kubernetes: Node Selector"
    )
    kubernetes_tolerations: list[dict[str, Any]] = Field(
        default_factory=list, title="Kubernetes: Tolerations"
    )
    kubernetes_labels: dict[str, str] = Field(
        default_factory=dict, title="Kubernetes: Labels"
    )
    kubernetes_annotations: dict[str, str] = Field(
        default_factory=dict, title="Kubernetes: Annotations"
    )
    kubernetes_backoff_limit: int = Field(default=0, title="Kubernetes: Backoff Limit")
    kubernetes_ttl_seconds_after_finished: int | None = Field(
        default=3600, title="Kubernetes: TTL Seconds After Finished"
    )
    kubernetes_pod_spec_overrides: dict[str, Any] = Field(
        default_factory=dict, title="Kubernetes: Pod Spec Overrides"
    )
    kubernetes_volumes: list[dict[str, Any]] = Field(
        default_factory=list, title="Kubernetes: Volumes"
    )
    kubernetes_volume_mounts: list[dict[str, Any]] = Field(
        default_factory=list, title="Kubernetes: Volume Mounts"
    )
    kubernetes_work_volume_claim: str | None = Field(
        default=None, title="Kubernetes: Work Volume Claim (PVC)"
    )
    kubernetes_work_mount_path: str | None = Field(
        default=None, title="Kubernetes: Work Mount Path"
    )

    # Queue/partition/resource-group names (terminology differs by system)
    queue_cpu: str = Field(default="regular-c", title="CPU Queue/Partition/Resource Group")
    queue_gpu: str = Field(default="regular-g", title="GPU Queue/Partition/Resource Group")

    # Project/group/account
    project_cpu: str = Field(default="", title="CPU Project/Group/Account")
    project_gpu: str = Field(default="", title="GPU Project/Group/Account")

    # Executable mapping
    executable_map: dict[str, str] = Field(default_factory=dict, title="Executable Map")

    # Slurm-specific options (ignored for other systems)
    slurm_qpu: str | None = Field(default=None, title="Slurm: QPU")
    slurm_memory: str | None = Field(default=None, title="Slurm: Memory")
    slurm_ntasks: int | None = Field(default=None, gt=0, title="Slurm: Tasks")
    slurm_extra_directives: list[str] = Field(
        default_factory=list, title="Slurm: Additional SBATCH Directives"
    )

    # Fugaku-specific options (ignored for other systems)
    gfscache: str | None = Field(default=None, title="Fugaku: GFS Cache Path (PJM_LLIO_GFSCACHE)")
    spack_modules: list[str] = Field(default_factory=list, title="Fugaku: Spack Modules")
    mpi_options_for_pjm: list[str] = Field(
        default_factory=list, title="Fugaku: MPI Options for PJM"
    )
    pjm_resources: list[str] = Field(
        default_factory=list,
        title="Fugaku: Additional PJM -L Directives",
    )
