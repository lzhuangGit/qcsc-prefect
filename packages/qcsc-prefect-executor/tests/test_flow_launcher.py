from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from qcsc_prefect_executor.kubernetes import flow_launcher as mod

BASE_ARGS = [
    "--image",
    "qcsc-hpc-base:latest",
    "--namespace",
    "qcsc",
    "--work-volume-claim",
    "ldata-pvc",
    "--work-mount-path",
    "/ldata",
    "--service-account",
    "qcsc-prefect-runner",
    "--env-from-secret",
    "prefect-credentials",
    "--run-id",
    "flow-abc12345",
]
FLOW_COMMAND = ["--", "/ldata/.venv/bin/python", "/ldata/flow.py", "--quantum-source", "random"]


class _Recorder:
    def __init__(self, returncode: int = 0) -> None:
        self.calls: list[list[str]] = []
        self.returncode = returncode

    def __call__(self, cmd, check=False):
        self.calls.append(list(cmd))
        return SimpleNamespace(returncode=self.returncode)


def _main(tmp_path: Path, extra: list[str], *, answer: str | None = None, interactive=True):
    run = _Recorder()
    prompts: list[str] = []

    def input_fn(prompt: str) -> str:
        prompts.append(prompt)
        return answer or ""

    code = mod.main(
        [*BASE_ARGS, "--runs-dir", str(tmp_path), *extra, *FLOW_COMMAND],
        input_fn=input_fn,
        run=run,
        interactive=interactive,
    )
    return code, run, prompts


def _manifest(tmp_path: Path) -> dict:
    return yaml.safe_load((tmp_path / "flow-abc12345" / "flow-job.yaml").read_text())


def test_manifest_runs_flow_command_with_secret_labels_and_downward_api(tmp_path: Path):
    _main(tmp_path, ["--no-launch"])

    manifest = _manifest(tmp_path)
    pod = manifest["spec"]["template"]["spec"]
    container = pod["containers"][0]
    assert manifest["metadata"]["name"] == "flow-abc12345"
    assert manifest["metadata"]["namespace"] == "qcsc"
    assert manifest["metadata"]["labels"]["qcsc-prefect/flow-run"] == "flow-abc12345"
    assert manifest["spec"]["backoffLimit"] == 0
    assert pod["serviceAccountName"] == "qcsc-prefect-runner"
    assert container["command"] == [
        "/ldata/.venv/bin/python",
        "/ldata/flow.py",
        "--quantum-source",
        "random",
    ]
    assert container["envFrom"] == [{"secretRef": {"name": "prefect-credentials"}}]
    env = {e["name"]: e for e in container["env"]}
    assert env["QCSC_FLOW_RUN_ID"]["value"] == "flow-abc12345"
    assert env["QCSC_OWNER_POD_NAME"]["valueFrom"]["fieldRef"]["fieldPath"] == "metadata.name"
    assert env["QCSC_OWNER_POD_UID"]["valueFrom"]["fieldRef"]["fieldPath"] == "metadata.uid"
    assert container["workingDir"] == "/ldata"


def test_no_launch_flag_writes_manifest_without_prompt_or_kubectl(tmp_path: Path):
    code, run, prompts = _main(tmp_path, ["--no-launch"])

    assert code == 0
    assert prompts == []
    assert run.calls == []
    assert (tmp_path / "flow-abc12345" / "flow-job.yaml").exists()


def test_launch_flag_applies_run_directory_without_prompt(tmp_path: Path):
    code, run, prompts = _main(tmp_path, ["--launch", "--context", "eks"])

    assert code == 0
    assert prompts == []
    assert run.calls == [
        ["kubectl", "--context", "eks", "apply", "-f", str(tmp_path / "flow-abc12345")]
    ]


@pytest.mark.parametrize("answer", ["y", "YES", " yes "])
def test_prompt_yes_launches(tmp_path: Path, answer: str):
    _code, run, prompts = _main(tmp_path, [], answer=answer)

    assert len(prompts) == 1
    assert len(run.calls) == 1


@pytest.mark.parametrize("answer", ["", "n", "no", "maybe"])
def test_prompt_other_answers_do_not_launch(tmp_path: Path, answer: str):
    _code, run, prompts = _main(tmp_path, [], answer=answer)

    assert len(prompts) == 1
    assert run.calls == []


def test_non_interactive_without_flag_does_not_launch_or_prompt(tmp_path: Path):
    _code, run, prompts = _main(tmp_path, [], interactive=False)

    assert prompts == []
    assert run.calls == []


def test_kubectl_failure_code_is_returned(tmp_path: Path):
    run = _Recorder(returncode=1)

    code = mod.main(
        [*BASE_ARGS, "--runs-dir", str(tmp_path), "--launch", *FLOW_COMMAND],
        run=run,
        interactive=False,
    )

    assert code == 1


def test_missing_flow_command_is_rejected(tmp_path: Path):
    with pytest.raises(ValueError, match="flow command"):
        mod.main([*BASE_ARGS, "--runs-dir", str(tmp_path), "--no-launch"], interactive=False)


def test_working_dir_outside_mount_path_is_rejected_before_writing(tmp_path: Path):
    with pytest.raises(ValueError, match="not inside work_mount_path"):
        mod.main(
            [
                *BASE_ARGS,
                "--runs-dir",
                str(tmp_path),
                "--working-dir",
                "/data/elsewhere",
                "--no-launch",
                *FLOW_COMMAND,
            ],
            interactive=False,
        )

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


def test_node_selector_and_toleration_parsing():
    assert mod._parse_node_selector(["use=infra", "arch=arm64"]) == {
        "use": "infra",
        "arch": "arm64",
    }
    assert mod._parse_toleration("use=infra:NoSchedule") == {
        "key": "use",
        "operator": "Equal",
        "value": "infra",
        "effect": "NoSchedule",
    }
    assert mod._parse_toleration("gpu") == {"key": "gpu", "operator": "Exists"}
    with pytest.raises(ValueError):
        mod._parse_node_selector(["bad"])


def test_written_yaml_has_no_anchors_or_aliases(tmp_path: Path):
    _main(tmp_path, ["--no-launch"])

    text = (tmp_path / "flow-abc12345" / "flow-job.yaml").read_text()

    assert "&id" not in text
    assert "*id" not in text
