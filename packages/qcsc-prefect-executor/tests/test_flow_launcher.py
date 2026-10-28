from __future__ import annotations

import base64
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from qcsc_prefect_executor.kubernetes import flow_launcher as mod
from qcsc_prefect_executor.kubernetes.flow_profile import (
    flow_pod_settings,
    settings_fingerprint,
)

API_URL = "https://api.prefect.cloud/api/accounts/a/workspaces/w"
BASE_ARGS = [
    "--env-from-secret",
    "prefect-credentials",
    "--run-id",
    "flow-abc12345",
]
FLOW_COMMAND = ["--", "/ldata/.venv/bin/python", "/ldata/flow.py", "--hpc-profile-block", "hpc-k8s"]


def _block(**overrides) -> SimpleNamespace:
    values = {
        "hpc_target": "kubernetes",
        "kubernetes_image": "qcsc-hpc-base:latest",
        "kubernetes_namespace": "qcsc",
        "kubernetes_service_account_name": "qcsc-prefect-runner",
        "kubernetes_image_pull_policy": "IfNotPresent",
        "kubernetes_image_pull_secrets": [],
        "kubernetes_node_selector": {"use": "infra", "arch": "arm64"},
        "kubernetes_tolerations": [
            {"key": "use", "operator": "Equal", "value": "infra", "effect": "NoSchedule"}
        ],
        "kubernetes_work_volume_claim": "ldata-pvc",
        "kubernetes_work_mount_path": "/ldata",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class _Block(SimpleNamespace):
    """A block that records the name it was loaded with."""

    def __init__(self, loaded: list[str]) -> None:
        super().__init__(**vars(_block()))
        object.__setattr__(self, "_loaded", loaded)


class _Recorder:
    def __init__(self, returncode: int = 0) -> None:
        self.calls: list[list[str]] = []
        self.returncode = returncode

    def __call__(self, cmd, check=False):
        self.calls.append(list(cmd))
        return SimpleNamespace(returncode=self.returncode)


def _main(
    tmp_path: Path,
    extra: list[str] | None = None,
    *,
    answer: str | None = None,
    interactive=True,
    block=None,
    secret_data: dict[str, str] | None = None,
    laptop_url: str | None = API_URL,
    command: list[str] | None = None,
):
    run = _Recorder()
    prompts: list[str] = []
    secret_reads: list[tuple[str, str, str | None]] = []
    block = block or _block()
    data = {"PREFECT_API_URL": API_URL} if secret_data is None else secret_data

    def input_fn(prompt: str) -> str:
        prompts.append(prompt)
        return answer or ""

    def read_secret(secret, namespace, context):
        secret_reads.append((secret, namespace, context))
        return data

    def load_block(name):
        if isinstance(block, _Block):
            block._loaded.append(name)
        return block

    code = mod.main(
        [
            *BASE_ARGS,
            "--runs-dir",
            str(tmp_path),
            *(extra or []),
            *(FLOW_COMMAND if command is None else command),
        ],
        input_fn=input_fn,
        run=run,
        interactive=interactive,
        load_block=load_block,
        read_secret=read_secret,
        current_api=lambda: (laptop_url, "dev"),
    )
    return code, run, prompts, secret_reads


def _manifest(tmp_path: Path) -> dict:
    return yaml.safe_load((tmp_path / "flow-abc12345" / "flow-job.yaml").read_text())


def _env(tmp_path: Path) -> dict:
    container = _manifest(tmp_path)["spec"]["template"]["spec"]["containers"][0]
    return {e["name"]: e.get("value") for e in container["env"]}


def test_manifest_is_built_from_the_block_and_runs_the_flow_command(tmp_path: Path):
    _main(tmp_path, ["--no-launch"])

    manifest = _manifest(tmp_path)
    pod = manifest["spec"]["template"]["spec"]
    container = pod["containers"][0]
    assert manifest["metadata"]["name"] == "flow-abc12345"
    assert manifest["metadata"]["namespace"] == "qcsc"
    assert manifest["metadata"]["labels"]["qcsc-prefect/flow-run"] == "flow-abc12345"
    assert manifest["spec"]["backoffLimit"] == 0
    assert container["image"] == "qcsc-hpc-base:latest"
    assert pod["serviceAccountName"] == "qcsc-prefect-runner"
    assert pod["nodeSelector"] == {"use": "infra", "arch": "arm64"}
    assert pod["tolerations"][0]["key"] == "use"
    assert pod["volumes"][0]["persistentVolumeClaim"]["claimName"] == "ldata-pvc"
    assert container["volumeMounts"][0]["mountPath"] == "/ldata"
    assert container["command"][-2:] == ["--hpc-profile-block", "hpc-k8s"]
    assert container["envFrom"] == [{"secretRef": {"name": "prefect-credentials"}}]
    env = {e["name"]: e for e in container["env"]}
    assert env["QCSC_FLOW_RUN_ID"]["value"] == "flow-abc12345"
    assert env["QCSC_OWNER_POD_NAME"]["valueFrom"]["fieldRef"]["fieldPath"] == "metadata.name"
    assert env["QCSC_OWNER_POD_UID"]["valueFrom"]["fieldRef"]["fieldPath"] == "metadata.uid"
    assert container["workingDir"] == "/ldata"


def test_manifest_records_block_name_fingerprint_and_api_url(tmp_path: Path):
    _main(tmp_path, ["--no-launch"])

    manifest = _manifest(tmp_path)
    container = manifest["spec"]["template"]["spec"]["containers"][0]
    env = {e["name"]: e.get("value") for e in container["env"]}
    fingerprint = settings_fingerprint(flow_pod_settings(_block()))
    assert env["QCSC_HPC_PROFILE_BLOCK"] == "hpc-k8s"
    assert env["QCSC_HPC_PROFILE_FINGERPRINT"] == fingerprint
    annotations = manifest["metadata"]["annotations"]
    assert annotations["qcsc-prefect/hpc-profile-block"] == "hpc-k8s"
    assert annotations["qcsc-prefect/hpc-profile-fingerprint"] == fingerprint
    assert annotations["qcsc-prefect/prefect-api-url"] == API_URL


def test_secret_is_read_from_the_blocks_namespace_with_the_kubectl_context(tmp_path: Path):
    *_, secret_reads = _main(tmp_path, ["--no-launch", "--context", "eks"])

    assert secret_reads == [("prefect-credentials", "qcsc", "eks")]


def test_no_launch_flag_writes_manifest_without_prompt_or_kubectl(tmp_path: Path):
    code, run, prompts, _ = _main(tmp_path, ["--no-launch"])

    assert code == 0
    assert prompts == []
    assert run.calls == []
    assert (tmp_path / "flow-abc12345" / "flow-job.yaml").exists()


def test_launch_flag_applies_run_directory_without_prompt(tmp_path: Path):
    code, run, prompts, _ = _main(tmp_path, ["--launch", "--context", "eks"])

    assert code == 0
    assert prompts == []
    assert run.calls == [
        ["kubectl", "--context", "eks", "apply", "-f", str(tmp_path / "flow-abc12345")]
    ]


@pytest.mark.parametrize("answer", ["y", "YES", " yes "])
def test_prompt_yes_launches(tmp_path: Path, answer: str):
    _code, run, prompts, _ = _main(tmp_path, [], answer=answer)

    assert len(prompts) == 1
    assert len(run.calls) == 1


@pytest.mark.parametrize("answer", ["", "n", "no", "maybe"])
def test_prompt_other_answers_do_not_launch(tmp_path: Path, answer: str):
    _code, run, prompts, _ = _main(tmp_path, [], answer=answer)

    assert len(prompts) == 1
    assert run.calls == []


def test_non_interactive_without_flag_does_not_launch_or_prompt(tmp_path: Path):
    _code, run, prompts, _ = _main(tmp_path, [], interactive=False)

    assert prompts == []
    assert run.calls == []


def test_kubectl_failure_code_is_returned(tmp_path: Path):
    run = _Recorder(returncode=1)

    code = mod.main(
        [*BASE_ARGS, "--runs-dir", str(tmp_path), "--launch", *FLOW_COMMAND],
        run=run,
        interactive=False,
        load_block=lambda name: _block(),
        read_secret=lambda *a: {"PREFECT_API_URL": API_URL},
        current_api=lambda: (API_URL, "dev"),
    )

    assert code == 1


def test_missing_flow_command_is_rejected(tmp_path: Path):
    with pytest.raises(ValueError, match="flow command"):
        _main(tmp_path, ["--no-launch"], command=[])


def test_flags_that_duplicate_block_fields_are_gone():
    with pytest.raises(SystemExit):
        mod.main([*BASE_ARGS, "--image", "other", *FLOW_COMMAND], interactive=False)


def test_working_dir_outside_mount_path_is_rejected_before_writing(tmp_path: Path):
    with pytest.raises(ValueError, match="not inside work_mount_path"):
        _main(tmp_path, ["--working-dir", "/data/elsewhere", "--no-launch"])

    assert not (tmp_path / "flow-abc12345").exists()


def test_existing_run_directory_is_not_overwritten(tmp_path: Path):
    (tmp_path / "flow-abc12345").mkdir()

    with pytest.raises(FileExistsError):
        _main(tmp_path, ["--no-launch"])


def test_generated_run_id_is_dns_safe_and_unique():
    first, second = mod.generate_run_id(), mod.generate_run_id()

    assert first != second
    assert first.startswith("flow-")
    assert len(first) <= 52


def test_written_yaml_has_no_anchors_or_aliases(tmp_path: Path):
    _main(tmp_path, ["--no-launch"])

    text = (tmp_path / "flow-abc12345" / "flow-job.yaml").read_text()

    assert "&id" not in text
    assert "*id" not in text


# --- the block must be usable for a flow pod ---------------------------------


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"hpc_target": "slurm"}, "needs 'kubernetes'"),
        ({"kubernetes_image": None}, "kubernetes_image"),
        ({"kubernetes_work_volume_claim": None}, "kubernetes_work_volume_claim"),
        ({"kubernetes_work_mount_path": None}, "kubernetes_work_mount_path"),
    ],
)
def test_unsuitable_block_is_rejected_before_writing(tmp_path: Path, overrides, message):
    with pytest.raises(ValueError, match=message):
        _main(tmp_path, ["--no-launch"], block=_block(**overrides))

    assert not (tmp_path / "flow-abc12345").exists()


# --- the flow command must name the same block ---------------------------------


def test_block_is_taken_from_the_flow_command(tmp_path: Path):
    loaded: list[str] = []
    (*_,) = _main(tmp_path, ["--no-launch"], block=_Block(loaded))

    assert loaded == ["hpc-k8s"]
    assert _env(tmp_path)["QCSC_HPC_PROFILE_BLOCK"] == "hpc-k8s"


def test_equals_form_in_the_flow_command_is_understood(tmp_path: Path):
    loaded: list[str] = []
    command = ["--", "python", "flow.py", "--hpc-profile-block=hpc-k8s"]

    _main(tmp_path, ["--no-launch"], block=_Block(loaded), command=command)

    assert loaded == ["hpc-k8s"]


def test_launcher_flag_supplies_the_block_when_the_flow_command_has_none(tmp_path: Path):
    loaded: list[str] = []

    _main(
        tmp_path,
        ["--no-launch", "--hpc-profile-block", "from-flag"],
        block=_Block(loaded),
        command=["--", "python", "flow.py"],
    )

    assert loaded == ["from-flag"]
    assert _env(tmp_path)["QCSC_HPC_PROFILE_BLOCK"] == "from-flag"


def test_flow_command_without_block_and_no_launcher_flag_is_rejected(tmp_path: Path):
    with pytest.raises(ValueError, match="cannot tell which profile block"):
        _main(tmp_path, ["--no-launch"], command=["--", "python", "flow.py"])

    assert not (tmp_path / "flow-abc12345").exists()


def test_launcher_flag_agreeing_with_the_flow_command_is_accepted(tmp_path: Path):
    code, *_ = _main(tmp_path, ["--no-launch", "--hpc-profile-block", "hpc-k8s"])

    assert code == 0


@pytest.mark.parametrize(
    "command",
    [
        ["--", "python", "flow.py", "--hpc-profile-block", "other-block"],
        ["--", "python", "flow.py", "--hpc-profile-block=other-block"],
    ],
)
def test_launcher_flag_disagreeing_with_the_flow_command_is_rejected(tmp_path: Path, command):
    with pytest.raises(ValueError, match="share one profile block"):
        _main(tmp_path, ["--no-launch", "--hpc-profile-block", "hpc-k8s"], command=command)

    assert not (tmp_path / "flow-abc12345").exists()


# --- the laptop and the flow pod must use the same Prefect API ---------------


def test_trailing_slash_difference_is_not_a_mismatch(tmp_path: Path):
    code, *_ = _main(tmp_path, ["--no-launch"], laptop_url=API_URL + "/")

    assert code == 0


def test_different_prefect_api_is_rejected_with_both_urls(tmp_path: Path):
    other = "https://api.prefect.cloud/api/accounts/a/workspaces/OTHER"

    with pytest.raises(ValueError) as excinfo:
        _main(tmp_path, ["--no-launch"], secret_data={"PREFECT_API_URL": other})

    text = str(excinfo.value)
    assert API_URL in text
    assert other in text
    assert "prefect-credentials" in text
    assert "profile 'dev'" in text
    assert not (tmp_path / "flow-abc12345").exists()


def test_secret_without_api_url_is_rejected(tmp_path: Path):
    with pytest.raises(ValueError, match="PREFECT_API_URL"):
        _main(tmp_path, ["--no-launch"], secret_data={"PREFECT_API_KEY": "k"})

    assert not (tmp_path / "flow-abc12345").exists()


def test_laptop_without_api_url_is_rejected(tmp_path: Path):
    with pytest.raises(ValueError, match="No PREFECT_API_URL"):
        _main(tmp_path, ["--no-launch"], laptop_url=None)


def test_later_secret_overrides_earlier_one_like_envfrom():
    values = {"a": {"PREFECT_API_URL": "https://old"}, "b": {"PREFECT_API_URL": API_URL}}

    url = mod.check_prefect_api_matches(
        secrets=["a", "b"],
        namespace="qcsc",
        context=None,
        read_secret=lambda secret, ns, ctx: values[secret],
        current_api=lambda: (API_URL, "dev"),
    )

    assert url == API_URL


def test_read_secret_data_decodes_values_and_uses_context(monkeypatch):
    calls = []
    payload = {"data": {"PREFECT_API_URL": base64.b64encode(API_URL.encode()).decode()}}

    def fake_run(cmd, capture_output, text, check):
        calls.append(cmd)
        return SimpleNamespace(returncode=0, stdout=__import__("json").dumps(payload), stderr="")

    monkeypatch.setattr(mod.subprocess, "run", fake_run)

    assert mod.read_secret_data("s", "qcsc", "eks") == {"PREFECT_API_URL": API_URL}
    assert calls == [
        ["kubectl", "--context", "eks", "get", "secret", "s", "-n", "qcsc", "-o", "json"]
    ]


def test_read_secret_data_reports_kubectl_failure(monkeypatch):
    monkeypatch.setattr(
        mod.subprocess,
        "run",
        lambda *a, **k: SimpleNamespace(returncode=1, stdout="", stderr="token expired"),
    )

    with pytest.raises(ValueError, match="token expired"):
        mod.read_secret_data("s", "qcsc", None)


def test_cli_prints_configuration_errors_without_traceback(monkeypatch, capsys):
    monkeypatch.setattr(mod.sys, "argv", ["flow_launcher", *BASE_ARGS])

    assert mod.cli() == 2
    assert "error: A flow command is required" in capsys.readouterr().err
