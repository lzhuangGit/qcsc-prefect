from __future__ import annotations

import pytest
from qcsc_prefect_adapters.kubernetes.builder import (
    KubernetesJobRequest,
    build_job_manifest,
    build_kubernetes_command,
    parse_walltime_seconds,
    sanitize_job_name,
)
from qcsc_prefect_core.models.execution_profile import ExecutionProfile


def _profile(**overrides) -> ExecutionProfile:
    values = {"command_key": "test", "num_nodes": 1, "launcher": "single"}
    values.update(overrides)
    return ExecutionProfile(**values)


def test_manifest_is_plain_batch_v1_job():
    profile = _profile(
        walltime="00:10:00",
        ompthreads=4,
        arguments=["--x", "1"],
        environments={"FOO": "bar"},
    )
    req = KubernetesJobRequest(
        image="registry.example/solver:1",
        executable="/opt/solver",
        namespace="science",
        job_name="My_Job",
        cpu="2",
        memory="4Gi",
        extra_resources={"nvidia.com/gpu": "1"},
        node_selector={"disk": "ssd"},
        tolerations=({"key": "gpu", "operator": "Exists"},),
        image_pull_secrets=("regcred",),
        service_account_name="runner",
        pod_spec_overrides={"runtimeClassName": "gvisor"},
    )

    manifest = build_job_manifest(exec_profile=profile, req=req)

    assert manifest["apiVersion"] == "batch/v1"
    assert manifest["kind"] == "Job"
    assert manifest["metadata"]["name"] == "my-job"
    assert manifest["metadata"]["namespace"] == "science"
    spec = manifest["spec"]
    assert spec["backoffLimit"] == 0
    assert spec["activeDeadlineSeconds"] == 600
    assert spec["ttlSecondsAfterFinished"] == 3600
    pod = spec["template"]["spec"]
    assert pod["restartPolicy"] == "Never"
    assert pod["serviceAccountName"] == "runner"
    assert pod["imagePullSecrets"] == [{"name": "regcred"}]
    assert pod["nodeSelector"] == {"disk": "ssd"}
    assert pod["tolerations"] == [{"key": "gpu", "operator": "Exists"}]
    assert pod["runtimeClassName"] == "gvisor"
    container = pod["containers"][0]
    assert container["image"] == "registry.example/solver:1"
    assert container["command"] == ["/opt/solver", "--x", "1"]
    assert {"name": "FOO", "value": "bar"} in container["env"]
    assert {"name": "OMP_NUM_THREADS", "value": "4"} in container["env"]
    assert container["resources"]["limits"] == {
        "cpu": "2",
        "memory": "4Gi",
        "nvidia.com/gpu": "1",
    }
    assert manifest["metadata"]["labels"]["app.kubernetes.io/managed-by"] == "qcsc-prefect"


def test_manifest_mounts_shared_work_volume_and_passthrough_volumes():
    req = KubernetesJobRequest(
        image="img",
        executable="/bin/true",
        work_volume_claim="shared-rwx",
        work_mount_path="/data/work",
        volumes=({"name": "scratch", "emptyDir": {}},),
        volume_mounts=({"name": "scratch", "mountPath": "/scratch"},),
    )

    manifest = build_job_manifest(exec_profile=_profile(), req=req)

    pod = manifest["spec"]["template"]["spec"]
    container = pod["containers"][0]
    assert pod["volumes"] == [
        {"name": "scratch", "emptyDir": {}},
        {"name": "qcsc-work", "persistentVolumeClaim": {"claimName": "shared-rwx"}},
    ]
    assert container["volumeMounts"] == [
        {"name": "scratch", "mountPath": "/scratch"},
        {"name": "qcsc-work", "mountPath": "/data/work"},
    ]
    assert container["workingDir"] == "/data/work"


def test_manifest_custom_working_dir_overrides_mount_path():
    req = KubernetesJobRequest(
        image="img",
        executable="/bin/true",
        work_volume_claim="shared-rwx",
        work_mount_path="/data/work",
        working_dir="/data/work/subfolder/job-1",
    )

    manifest = build_job_manifest(exec_profile=_profile(), req=req)

    container = manifest["spec"]["template"]["spec"]["containers"][0]
    assert container["workingDir"] == "/data/work/subfolder/job-1"


def test_manifest_without_storage_has_no_volume_keys():
    manifest = build_job_manifest(
        exec_profile=_profile(), req=KubernetesJobRequest(image="img", executable="/x")
    )

    pod = manifest["spec"]["template"]["spec"]
    assert "volumes" not in pod
    assert "volumeMounts" not in pod["containers"][0]
    assert "workingDir" not in pod["containers"][0]


@pytest.mark.parametrize("mount_path", [None, "relative/path"])
def test_work_volume_claim_requires_absolute_mount_path(mount_path):
    req = KubernetesJobRequest(
        image="img",
        executable="/x",
        work_volume_claim="shared-rwx",
        work_mount_path=mount_path,
    )

    with pytest.raises(ValueError, match="work_mount_path"):
        build_job_manifest(exec_profile=_profile(), req=req)


def test_manifest_generates_unique_valid_name_and_omits_optional_fields():
    req = KubernetesJobRequest(image="img", executable="/bin/true", ttl_seconds_after_finished=None)

    first = build_job_manifest(exec_profile=_profile(), req=req)
    second = build_job_manifest(exec_profile=_profile(), req=req)

    assert first["metadata"]["name"] != second["metadata"]["name"]
    assert "activeDeadlineSeconds" not in first["spec"]
    assert "ttlSecondsAfterFinished" not in first["spec"]
    container = first["spec"]["template"]["spec"]["containers"][0]
    assert "resources" not in container
    assert "env" not in container


def test_command_uses_launcher_without_shell():
    profile = _profile(launcher="mpiexec", mpi_options=["-n", "4"], arguments=["a b"])

    assert build_kubernetes_command(exec_profile=profile, executable="/opt/solver") == [
        "mpiexec",
        "-n",
        "4",
        "/opt/solver",
        "a b",
    ]


@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"modules": ["openmpi"]}, "modules"),
        ({"pre_commands": ["echo hi"]}, "pre_commands"),
        ({"num_nodes": 2}, "num_nodes"),
    ],
)
def test_command_rejects_unsupported_profile(overrides, match):
    with pytest.raises(ValueError, match=match):
        build_kubernetes_command(exec_profile=_profile(**overrides), executable="/x")


def test_empty_image_rejected():
    with pytest.raises(ValueError, match="image"):
        build_job_manifest(
            exec_profile=_profile(), req=KubernetesJobRequest(image=" ", executable="/x")
        )


def test_sanitize_job_name():
    assert sanitize_job_name("Hello_World.v2") == "hello-world-v2"
    assert len(sanitize_job_name("a" * 200)) <= 52
    with pytest.raises(ValueError):
        sanitize_job_name("___")


def test_parse_walltime_seconds():
    assert parse_walltime_seconds("01:02:03") == 3723
    assert parse_walltime_seconds("05:00") == 300
    with pytest.raises(ValueError):
        parse_walltime_seconds("soon")
