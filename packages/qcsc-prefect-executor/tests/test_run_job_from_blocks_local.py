from __future__ import annotations

import asyncio
from pathlib import Path

from qcsc_prefect_executor import from_blocks as mod


class _CommandBlockStub:
    def __init__(self, command_name: str, executable_key: str, default_args: list[str]) -> None:
        self.command_name = command_name
        self.executable_key = executable_key
        self.default_args = default_args


class _ExecutionProfileBlockStub:
    def __init__(
        self,
        *,
        profile_name: str,
        command_name: str,
        resource_class: str = "cpu",
        num_nodes: int = 2,
        mpiprocs: int = 5,
        ompthreads: int | None = None,
        walltime: str = "00:10:00",
        launcher: str = "mpiexec.hydra",
        mpi_options: list[str] | None = None,
        modules: list[str] | None = None,
        pre_commands: list[str] | None = None,
        environments: dict[str, str] | None = None,
    ) -> None:
        self.profile_name = profile_name
        self.command_name = command_name
        self.resource_class = resource_class
        self.num_nodes = num_nodes
        self.mpiprocs = mpiprocs
        self.ompthreads = ompthreads
        self.walltime = walltime
        self.launcher = launcher
        self.mpi_options = mpi_options or []
        self.modules = modules or []
        self.pre_commands = pre_commands or []
        self.environments = environments or {}


class _HPCProfileBlockStub:
    def __init__(
        self,
        *,
        hpc_target: str,
        executable_map: dict[str, str],
        queue_cpu: str = "regular-c",
        queue_gpu: str = "regular-g",
        project_cpu: str = "gz00",
        project_gpu: str = "gz00",
        gfscache: str | None = None,
        spack_modules: list[str] | None = None,
        mpi_options_for_pjm: list[str] | None = None,
        pjm_resources: list[str] | None = None,
        slurm_qpu: str | None = None,
        slurm_memory: str | None = None,
        slurm_ntasks: int | None = None,
        slurm_extra_directives: list[str] | None = None,
    ) -> None:
        self.hpc_target = hpc_target
        self.executable_map = executable_map
        self.queue_cpu = queue_cpu
        self.queue_gpu = queue_gpu
        self.project_cpu = project_cpu
        self.project_gpu = project_gpu
        self.gfscache = gfscache
        self.spack_modules = spack_modules or []
        self.mpi_options_for_pjm = mpi_options_for_pjm or []
        self.pjm_resources = pjm_resources or []
        self.slurm_qpu = slurm_qpu
        self.slurm_memory = slurm_memory
        self.slurm_ntasks = slurm_ntasks
        self.slurm_extra_directives = slurm_extra_directives or []
        self.kubernetes_image = None
        self.kubernetes_namespace = "default"
        self.kubernetes_service_account_name = None
        self.kubernetes_image_pull_policy = None
        self.kubernetes_image_pull_secrets = []
        self.kubernetes_cpu = None
        self.kubernetes_memory = None
        self.kubernetes_extra_resources = {}
        self.kubernetes_node_selector = {}
        self.kubernetes_tolerations = []
        self.kubernetes_labels = {}
        self.kubernetes_annotations = {}
        self.kubernetes_backoff_limit = 0
        self.kubernetes_ttl_seconds_after_finished = 3600
        self.kubernetes_pod_spec_overrides = {}
        self.kubernetes_volumes = []
        self.kubernetes_volume_mounts = []
        self.kubernetes_work_volume_claim = None
        self.kubernetes_work_mount_path = None


def _patch_block_loading(monkeypatch, command, profile, hpc):
    async def fake_command_load(_name: str):
        return command

    async def fake_profile_load(_name: str):
        return profile

    async def fake_hpc_load(_name: str):
        return hpc

    class _CmdAPI:
        load = staticmethod(fake_command_load)

    class _ProfileAPI:
        load = staticmethod(fake_profile_load)

    class _HpcAPI:
        load = staticmethod(fake_hpc_load)

    monkeypatch.setattr(mod, "CommandBlock", _CmdAPI)
    monkeypatch.setattr(mod, "ExecutionProfileBlock", _ProfileAPI)
    monkeypatch.setattr(mod, "HPCProfileBlock", _HpcAPI)


def test_run_job_from_blocks_dispatches_to_miyabi(monkeypatch, tmp_path: Path):
    command = _CommandBlockStub("bitcount-hist", "bitcount_hist", [])
    profile = _ExecutionProfileBlockStub(
        profile_name="bitcount-mpi",
        command_name="bitcount-hist",
        pre_commands=["unset OMPI_MCA_mca_base_env_list"],
    )
    hpc = _HPCProfileBlockStub(
        hpc_target="miyabi",
        executable_map={"bitcount_hist": "/work/gz00/z99999/get_counts_hist"},
    )
    _patch_block_loading(monkeypatch, command, profile, hpc)

    captured: dict[str, object] = {}

    class _MiyabiResult:
        def __init__(self) -> None:
            self.job_id = "12345.miyabi"
            self.exit_status = 0

    async def fake_run_miyabi_job(**kwargs):
        captured.update(kwargs)
        return _MiyabiResult()

    async def fake_run_fugaku_job(**kwargs):
        raise AssertionError("run_fugaku_job should not be called in this test")

    monkeypatch.setattr(mod, "run_miyabi_job", fake_run_miyabi_job)
    monkeypatch.setattr(mod, "run_fugaku_job", fake_run_fugaku_job)

    result = asyncio.run(
        mod.run_job_from_blocks(
            command_block_name="cmd",
            execution_profile_block_name="exec",
            hpc_profile_block_name="hpc",
            work_dir=tmp_path,
            script_filename="job",
            metrics_artifact_key="miyabi-metrics",
        )
    )

    assert result.job_id == "12345.miyabi"
    assert captured["script_filename"] == "job.pbs"
    assert captured["req"].queue_name == "regular-c"
    assert captured["req"].project == "gz00"
    assert captured["req"].executable == "/work/gz00/z99999/get_counts_hist"
    assert captured["metrics_artifact_key"] == "miyabi-metrics"
    assert captured["exec_profile"].pre_commands == ["unset OMPI_MCA_mca_base_env_list"]


def test_run_job_from_blocks_dispatches_to_fugaku(monkeypatch, tmp_path: Path):
    command = _CommandBlockStub("bitcount-hist", "bitcount_hist", [])
    profile = _ExecutionProfileBlockStub(profile_name="bitcount-mpi", command_name="bitcount-hist")
    hpc = _HPCProfileBlockStub(
        hpc_target="fugaku",
        executable_map={"bitcount_hist": "/vol0001/home/z99999/get_counts_hist"},
        queue_cpu="small",
        project_cpu="hp200999",
        gfscache="/vol0002",
        spack_modules=["fjmpi"],
        mpi_options_for_pjm=["max-proc-per-node=48"],
        pjm_resources=["freq=2000,eco_state=2"],
    )
    _patch_block_loading(monkeypatch, command, profile, hpc)

    captured: dict[str, object] = {}

    class _FugakuResult:
        def __init__(self) -> None:
            self.job_id = "900001"
            self.exit_status = 0
            self.state = "EXT"

    async def fake_run_miyabi_job(**kwargs):
        raise AssertionError("run_miyabi_job should not be called in this test")

    async def fake_run_fugaku_job(**kwargs):
        captured.update(kwargs)
        return _FugakuResult()

    monkeypatch.setattr(mod, "run_miyabi_job", fake_run_miyabi_job)
    monkeypatch.setattr(mod, "run_fugaku_job", fake_run_fugaku_job)

    result = asyncio.run(
        mod.run_job_from_blocks(
            command_block_name="cmd",
            execution_profile_block_name="exec",
            hpc_profile_block_name="hpc",
            work_dir=tmp_path,
            script_filename="job.pbs",
            metrics_artifact_key="fugaku-metrics",
        )
    )

    assert result.job_id == "900001"
    assert captured["script_filename"] == "job.pjm"
    assert captured["req"].queue_name == "small"
    assert captured["req"].project == "hp200999"
    assert captured["req"].executable == "/vol0001/home/z99999/get_counts_hist"
    assert captured["req"].gfscache == "/vol0002"
    assert captured["req"].spack_modules == ["fjmpi"]
    assert captured["req"].mpi_options_for_pjm == ["max-proc-per-node=48"]
    assert captured["req"].pjm_resources == ["freq=2000,eco_state=2"]
    assert captured["metrics_artifact_key"] == "fugaku-metrics"


def test_run_job_from_blocks_executes_local_without_a_script(monkeypatch, tmp_path: Path):
    command = _CommandBlockStub("bitcount-hist", "bitcount_hist", ["--base"])
    profile = _ExecutionProfileBlockStub(
        profile_name="bitcount-local",
        command_name="bitcount-hist",
        launcher="mpiexec",
        mpi_options=["-n", "2"],
        environments={"LOCAL_TEST": "enabled"},
    )
    hpc = _HPCProfileBlockStub(
        hpc_target="local",
        executable_map={"bitcount_hist": "/opt/local/get_counts_hist"},
    )
    _patch_block_loading(monkeypatch, command, profile, hpc)

    captured: dict[str, object] = {}

    class _LocalResult:
        job_id = "local-123"
        exit_status = 0

    async def fake_run_local_job(**kwargs):
        captured.update(kwargs)
        return _LocalResult()

    async def fail_scheduler_run(**kwargs):
        raise AssertionError("A scheduler executor must not be called for local execution")

    monkeypatch.setattr(mod, "run_local_job", fake_run_local_job)
    monkeypatch.setattr(mod, "run_miyabi_job", fail_scheduler_run)
    monkeypatch.setattr(mod, "run_fugaku_job", fail_scheduler_run)
    monkeypatch.setattr(mod, "run_slurm_job", fail_scheduler_run)

    result = asyncio.run(
        mod.run_job_from_blocks(
            command_block_name="cmd",
            execution_profile_block_name="exec",
            hpc_profile_block_name="hpc",
            work_dir=tmp_path,
            user_args=["--override"],
            timeout_seconds=12.5,
            metrics_artifact_key="local-metrics",
        )
    )

    assert result.job_id == "local-123"
    assert captured["work_dir"] == tmp_path.resolve()
    assert captured["req"].executable == "/opt/local/get_counts_hist"
    assert captured["exec_profile"].arguments == ["--base", "--override"]
    assert captured["exec_profile"].environments == {"LOCAL_TEST": "enabled"}
    assert captured["timeout_seconds"] == 12.5
    assert captured["metrics_artifact_key"] == "local-metrics"
    assert not list(tmp_path.iterdir())


def test_run_job_from_blocks_applies_execution_profile_overrides(monkeypatch, tmp_path: Path):
    command = _CommandBlockStub("bitcount-hist", "bitcount_hist", ["--base"])
    profile = _ExecutionProfileBlockStub(
        profile_name="bitcount-mpi",
        command_name="bitcount-hist",
        num_nodes=2,
        mpiprocs=5,
        walltime="00:10:00",
        launcher="mpiexec.hydra",
        mpi_options=["-np", "10"],
        modules=["intel"],
        pre_commands=["echo before"],
        environments={"FOO": "bar"},
    )
    hpc = _HPCProfileBlockStub(
        hpc_target="miyabi",
        executable_map={"bitcount_hist": "/work/gz00/z99999/get_counts_hist"},
    )
    _patch_block_loading(monkeypatch, command, profile, hpc)

    captured: dict[str, object] = {}

    class _MiyabiResult:
        def __init__(self) -> None:
            self.job_id = "12345.miyabi"
            self.exit_status = 0

    async def fake_run_miyabi_job(**kwargs):
        captured.update(kwargs)
        return _MiyabiResult()

    async def fake_run_fugaku_job(**kwargs):
        raise AssertionError("run_fugaku_job should not be called in this test")

    monkeypatch.setattr(mod, "run_miyabi_job", fake_run_miyabi_job)
    monkeypatch.setattr(mod, "run_fugaku_job", fake_run_fugaku_job)

    asyncio.run(
        mod.run_job_from_blocks(
            command_block_name="cmd",
            execution_profile_block_name="exec",
            hpc_profile_block_name="hpc",
            work_dir=tmp_path,
            script_filename="job.pbs",
            user_args=["--override"],
            execution_profile_overrides={
                "num_nodes": 4,
                "mpiprocs": 1,
                "walltime": "00:20:00",
                "launcher": "single",
                "mpi_options": [],
                "modules": ["python"],
                "pre_commands": ["echo override"],
                "environments": {"HELLO": "world"},
            },
        )
    )

    exec_profile = captured["exec_profile"]
    assert exec_profile.num_nodes == 4
    assert exec_profile.mpiprocs == 1
    assert exec_profile.walltime == "00:20:00"
    assert exec_profile.launcher == "single"
    assert exec_profile.mpi_options == []
    assert exec_profile.modules == ["python"]
    assert exec_profile.pre_commands == ["echo override"]
    assert exec_profile.environments == {"HELLO": "world"}
    assert exec_profile.arguments == ["--base", "--override"]


def test_run_job_from_blocks_dispatches_to_slurm(monkeypatch, tmp_path: Path):
    command = _CommandBlockStub("bitcount-hist", "bitcount_hist", ["--base"])
    profile = _ExecutionProfileBlockStub(
        profile_name="bitcount-slurm",
        command_name="bitcount-hist",
        launcher="srun",
        mpi_options=["--cpu-bind=cores"],
        pre_commands=["echo before-run"],
    )
    hpc = _HPCProfileBlockStub(
        hpc_target="slurm",
        executable_map={"bitcount_hist": "/work/proj01/bin/get_counts_hist"},
        queue_cpu="compute",
        project_cpu="proj01",
        slurm_qpu="a100",
        slurm_memory="5001G",
        slurm_ntasks=8,
        slurm_extra_directives=[
            "--workload-kind=quantum",
            "--circ-count=1",
            "--max-qubits=20",
        ],
    )
    _patch_block_loading(monkeypatch, command, profile, hpc)

    captured: dict[str, object] = {}

    class _SlurmResult:
        def __init__(self) -> None:
            self.job_id = "12345"
            self.exit_status = 0
            self.state = "COMPLETED"

    async def fake_run_miyabi_job(**kwargs):
        raise AssertionError("run_miyabi_job should not be called in this test")

    async def fake_run_fugaku_job(**kwargs):
        raise AssertionError("run_fugaku_job should not be called in this test")

    async def fake_run_slurm_job(**kwargs):
        captured.update(kwargs)
        return _SlurmResult()

    monkeypatch.setattr(mod, "run_miyabi_job", fake_run_miyabi_job)
    monkeypatch.setattr(mod, "run_fugaku_job", fake_run_fugaku_job)
    monkeypatch.setattr(mod, "run_slurm_job", fake_run_slurm_job)

    result = asyncio.run(
        mod.run_job_from_blocks(
            command_block_name="cmd",
            execution_profile_block_name="exec",
            hpc_profile_block_name="hpc",
            work_dir=tmp_path,
            script_filename="job.slurm",
            user_args=["--override"],
            metrics_artifact_key="slurm-metrics",
        )
    )

    assert result.job_id == "12345"
    assert captured["req"].partition == "compute"
    assert captured["req"].account == "proj01"
    assert captured["req"].executable == "/work/proj01/bin/get_counts_hist"
    assert captured["req"].qpu == "a100"
    assert captured["req"].memory == "5001G"
    assert captured["req"].ntasks == 8
    assert captured["req"].extra_directives == (
        "--workload-kind=quantum",
        "--circ-count=1",
        "--max-qubits=20",
    )
    assert captured["metrics_artifact_key"] == "slurm-metrics"
    assert captured["exec_profile"].launcher == "srun"
    assert captured["exec_profile"].mpi_options == ["--cpu-bind=cores"]
    assert captured["exec_profile"].pre_commands == ["echo before-run"]
    assert captured["exec_profile"].arguments == ["--base", "--override"]


def test_run_job_from_blocks_rejects_unknown_execution_profile_override(
    monkeypatch, tmp_path: Path
):
    command = _CommandBlockStub("bitcount-hist", "bitcount_hist", [])
    profile = _ExecutionProfileBlockStub(profile_name="bitcount-mpi", command_name="bitcount-hist")
    hpc = _HPCProfileBlockStub(
        hpc_target="miyabi",
        executable_map={"bitcount_hist": "/work/gz00/z99999/get_counts_hist"},
    )
    _patch_block_loading(monkeypatch, command, profile, hpc)

    async def fake_run_miyabi_job(**kwargs):
        raise AssertionError("run_miyabi_job should not be called in this test")

    async def fake_run_fugaku_job(**kwargs):
        raise AssertionError("run_fugaku_job should not be called in this test")

    monkeypatch.setattr(mod, "run_miyabi_job", fake_run_miyabi_job)
    monkeypatch.setattr(mod, "run_fugaku_job", fake_run_fugaku_job)

    try:
        asyncio.run(
            mod.run_job_from_blocks(
                command_block_name="cmd",
                execution_profile_block_name="exec",
                hpc_profile_block_name="hpc",
                work_dir=tmp_path,
                script_filename="job.pbs",
                execution_profile_overrides={"resource_class": "gpu"},
            )
        )
    except ValueError as exc:
        assert "Unsupported execution_profile_overrides keys" in str(exc)
    else:
        raise AssertionError("Expected ValueError for unknown execution_profile_overrides key")


def test_resolve_submission_target_uses_gpu_route(monkeypatch):
    profile = _ExecutionProfileBlockStub(
        profile_name="bitcount-gpu",
        command_name="bitcount-hist",
        resource_class="gpu",
    )
    hpc = _HPCProfileBlockStub(
        hpc_target="fugaku",
        executable_map={"bitcount_hist": "/vol0001/home/z99999/get_counts_hist"},
        queue_cpu="small",
        queue_gpu="gpu-small",
        project_cpu="hp200999",
        project_gpu="hp300999",
    )

    async def fake_profile_load(_name: str):
        return profile

    async def fake_hpc_load(_name: str):
        return hpc

    class _ProfileAPI:
        load = staticmethod(fake_profile_load)

    class _HpcAPI:
        load = staticmethod(fake_hpc_load)

    monkeypatch.setattr(mod, "ExecutionProfileBlock", _ProfileAPI)
    monkeypatch.setattr(mod, "HPCProfileBlock", _HpcAPI)

    target = asyncio.run(
        mod.resolve_submission_target(
            execution_profile_block_name="exec",
            hpc_profile_block_name="hpc",
        )
    )

    assert target.hpc_target == "fugaku"
    assert target.queue_name == "gpu-small"
    assert target.project == "hp300999"


def test_build_scheduler_script_filename_rewrites_scheduler_suffix():
    assert mod.build_scheduler_script_filename("job", "miyabi") == "job.pbs"
    assert mod.build_scheduler_script_filename("job.pbs", "fugaku") == "job.pjm"
    assert mod.build_scheduler_script_filename("job.pjm", "slurm") == "job.slurm"


def test_resolve_submission_target_kubernetes(monkeypatch):
    profile = _ExecutionProfileBlockStub(
        profile_name="bitcount-k8s",
        command_name="bitcount-hist",
        resource_class="cpu",
    )
    hpc = _HPCProfileBlockStub(
        hpc_target="kubernetes",
        executable_map={"bitcount_hist": "/usr/local/bin/get_counts_hist"},
    )
    hpc.kubernetes_namespace = "qcsc-prod"

    async def fake_profile_load(_name: str):
        return profile

    async def fake_hpc_load(_name: str):
        return hpc

    class _ProfileAPI:
        load = staticmethod(fake_profile_load)

    class _HpcAPI:
        load = staticmethod(fake_hpc_load)

    monkeypatch.setattr(mod, "ExecutionProfileBlock", _ProfileAPI)
    monkeypatch.setattr(mod, "HPCProfileBlock", _HpcAPI)

    target = asyncio.run(
        mod.resolve_submission_target(
            execution_profile_block_name="exec",
            hpc_profile_block_name="hpc",
        )
    )

    assert target.hpc_target == "kubernetes"
    assert target.queue_name == "qcsc-prod"
    assert target.project == ""


def test_run_job_from_blocks_kubernetes_calls_run_kubernetes_job(tmp_path: Path, monkeypatch):
    command = _CommandBlockStub("bitcount-hist", "bitcount_hist", ["--base"])
    profile = _ExecutionProfileBlockStub(
        profile_name="bitcount-k8s",
        command_name="bitcount-hist",
        launcher="single",
        num_nodes=1,
        mpiprocs=1,
    )
    hpc = _HPCProfileBlockStub(
        hpc_target="kubernetes",
        executable_map={"bitcount_hist": "/usr/local/bin/get_counts_hist"},
    )
    hpc.kubernetes_image = "qcsc-bitcount:latest"
    hpc.kubernetes_namespace = "qcsc"
    hpc.kubernetes_work_volume_claim = "qcsc-pvc"
    hpc.kubernetes_work_mount_path = "/work"

    _patch_block_loading(monkeypatch, command, profile, hpc)

    captured: dict[str, object] = {}

    async def fake_run_kubernetes_job(*, work_dir, exec_profile, req, **kwargs):
        captured["work_dir"] = work_dir
        captured["exec_profile"] = exec_profile
        captured["req"] = req
        captured["kwargs"] = kwargs
        return "k8s-result"

    from qcsc_prefect_executor.kubernetes import run as k8s_run_mod

    monkeypatch.setattr(k8s_run_mod, "run_kubernetes_job", fake_run_kubernetes_job)

    result = asyncio.run(
        mod.run_job_from_blocks(
            command_block_name="cmd",
            execution_profile_block_name="exec",
            hpc_profile_block_name="hpc",
            work_dir=tmp_path,
            metrics_artifact_key="k8s-metrics",
        )
    )

    assert result == "k8s-result"
    assert captured["work_dir"] == tmp_path.resolve()
    assert captured["exec_profile"].arguments == ["--base"]
    assert captured["req"].image == "qcsc-bitcount:latest"
    assert captured["req"].executable == "/usr/local/bin/get_counts_hist"
    assert captured["req"].namespace == "qcsc"
    assert captured["req"].work_volume_claim == "qcsc-pvc"
    assert captured["req"].work_mount_path == "/work"


def test_run_job_from_blocks_kubernetes_missing_image_raises(tmp_path: Path, monkeypatch):
    command = _CommandBlockStub("bitcount-hist", "bitcount_hist", [])
    profile = _ExecutionProfileBlockStub(
        profile_name="bitcount-k8s",
        command_name="bitcount-hist",
    )
    hpc = _HPCProfileBlockStub(
        hpc_target="kubernetes",
        executable_map={"bitcount_hist": "/usr/local/bin/get_counts_hist"},
    )
    hpc.kubernetes_image = None

    _patch_block_loading(monkeypatch, command, profile, hpc)

    import pytest

    with pytest.raises(ValueError, match="must set 'kubernetes_image'"):
        asyncio.run(
            mod.run_job_from_blocks(
                command_block_name="cmd",
                execution_profile_block_name="exec",
                hpc_profile_block_name="hpc",
                work_dir=tmp_path,
            )
        )


def _k8s_blocks(monkeypatch):
    command = _CommandBlockStub("bitcount-hist", "bitcount_hist", [])
    profile = _ExecutionProfileBlockStub(
        profile_name="bitcount-k8s",
        command_name="bitcount-hist",
        launcher="single",
        num_nodes=1,
        mpiprocs=1,
    )
    hpc = _HPCProfileBlockStub(
        hpc_target="kubernetes",
        executable_map={"bitcount_hist": "/ldata/bin/get_counts_hist"},
    )
    hpc.kubernetes_image = "qcsc-bitcount:latest"
    hpc.kubernetes_work_volume_claim = "qcsc-pvc"
    hpc.kubernetes_work_mount_path = "/ldata"
    hpc.kubernetes_labels = {"team": "qc"}
    _patch_block_loading(monkeypatch, command, profile, hpc)


def _prepared_k8s_req(tmp_path: Path):
    prepared = asyncio.run(
        mod._prepare_job_from_blocks(
            command_block_name="cmd",
            execution_profile_block_name="exec",
            hpc_profile_block_name="hpc",
            work_dir=tmp_path,
            script_filename=None,
        )
    )
    return prepared.req


def test_kubernetes_compute_job_inherits_flow_run_label_and_owner_in_flow_pod(
    tmp_path: Path, monkeypatch
):
    _k8s_blocks(monkeypatch)
    monkeypatch.setenv("QCSC_FLOW_RUN_ID", "flow-1234abcd")
    monkeypatch.setenv("QCSC_OWNER_POD_NAME", "flow-1234abcd-xyz")
    monkeypatch.setenv("QCSC_OWNER_POD_UID", "uid-1")

    req = _prepared_k8s_req(tmp_path)

    assert req.labels == {"team": "qc", "qcsc-prefect/flow-run": "flow-1234abcd"}
    assert req.owner_references == (
        {"apiVersion": "v1", "kind": "Pod", "name": "flow-1234abcd-xyz", "uid": "uid-1"},
    )


def test_kubernetes_compute_job_has_no_owner_outside_flow_pod(tmp_path: Path, monkeypatch):
    _k8s_blocks(monkeypatch)
    for name in ("QCSC_FLOW_RUN_ID", "QCSC_OWNER_POD_NAME", "QCSC_OWNER_POD_UID"):
        monkeypatch.delenv(name, raising=False)

    req = _prepared_k8s_req(tmp_path)

    assert req.labels == {"team": "qc"}
    assert req.owner_references == ()


def test_kubernetes_owner_reference_needs_both_name_and_uid(tmp_path: Path, monkeypatch):
    _k8s_blocks(monkeypatch)
    monkeypatch.delenv("QCSC_FLOW_RUN_ID", raising=False)
    monkeypatch.setenv("QCSC_OWNER_POD_NAME", "flow-pod")
    monkeypatch.delenv("QCSC_OWNER_POD_UID", raising=False)

    assert _prepared_k8s_req(tmp_path).owner_references == ()
