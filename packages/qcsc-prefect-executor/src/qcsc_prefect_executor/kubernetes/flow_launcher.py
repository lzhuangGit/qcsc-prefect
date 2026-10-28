"""Launch a Prefect flow as a batch Job inside a Kubernetes cluster.

The flow runs in a pod, next to the compute Jobs it creates, so it sees the
same shared volume at the same path. Run this module from a laptop (or any
machine with ``kubectl``) to render the Job manifest into
``<runs-dir>/<run-id>/flow-job.yaml`` and optionally apply it.

The flow pod is configured from the same ``HPCProfileBlock`` that its compute
Jobs use, so both always run in the same cluster, namespace and shared volume.
The block is the ``--hpc-profile-block`` of the flow command; pass the launcher's
own ``--hpc-profile-block`` only when the flow command does not name one. Two
safeguards back this up:

* The Prefect API URL in the pod's Secret must equal the URL of the laptop's
  active Prefect profile. Otherwise the pod would read different blocks.
* The block name and a fingerprint of its pod settings are stored in the pod.
  The flow fails before creating any Job if it is asked to use another block, or
  if the block was edited after the manifest was rendered.

Cleanup is two-layered:

* ``kubectl delete -f <runs-dir>/<run-id>/`` deletes the flow Job. Compute Jobs
  created by the flow carry an ``ownerReference`` to the flow pod, so Kubernetes
  garbage-collects them as well.
* If garbage collection does not remove them, every Job of the run carries the
  label ``qcsc-prefect/flow-run=<run-id>`` for
  ``kubectl delete jobs -l qcsc-prefect/flow-run=<run-id>``.

Example::

    python -m qcsc_prefect_executor.kubernetes.flow_launcher \\
        --env-from-secret prefect-credentials \\
        -- /ldata/qcsc-prefect/.venv/bin/python \\
           /ldata/qcsc-prefect/examples/prefect_bitcount_demo/flow_optimized.py \\
           --hpc-profile-block hpc-k8s-bitcount --quantum-source random
"""

from __future__ import annotations

import argparse
import base64
import inspect
import json
import shlex
import subprocess
import sys
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml
from qcsc_prefect_adapters.kubernetes.builder import (
    FLOW_RUN_ID_ENV,
    FLOW_RUN_LABEL,
    OWNER_POD_NAME_ENV,
    OWNER_POD_UID_ENV,
    KubernetesJobRequest,
    build_job_manifest,
)
from qcsc_prefect_core.models.execution_profile import ExecutionProfile

from qcsc_prefect_executor.kubernetes.flow_profile import (
    HPC_PROFILE_BLOCK_ANNOTATION,
    HPC_PROFILE_BLOCK_ENV,
    HPC_PROFILE_FINGERPRINT_ANNOTATION,
    HPC_PROFILE_FINGERPRINT_ENV,
    PREFECT_API_URL_ANNOTATION,
    flow_pod_settings,
    settings_fingerprint,
    validate_flow_block,
)

MANIFEST_FILENAME = "flow-job.yaml"
DEFAULT_RUNS_DIR = "runs"
PREFECT_API_URL_KEY = "PREFECT_API_URL"
_BLOCK_FLAG = "--hpc-profile-block"


def generate_run_id() -> str:
    """Return a unique, DNS-1123 safe id such as ``flow-3f9a1c2b``."""

    return f"flow-{uuid.uuid4().hex[:8]}"


def load_hpc_block(name: str) -> Any:
    """Load an ``HPCProfileBlock`` from the Prefect API of the active profile."""

    from qcsc_prefect_blocks.common.blocks import HPCProfileBlock

    block = HPCProfileBlock.load(name)
    if inspect.isawaitable(block):
        import asyncio

        block = asyncio.run(block)
    return block


def current_prefect_api_url() -> tuple[str | None, str]:
    """Return ``(PREFECT_API_URL, profile name)`` of the active Prefect profile."""

    from prefect.context import get_settings_context
    from prefect.settings import get_current_settings

    return get_current_settings().api.url, get_settings_context().profile.name


def read_secret_data(secret: str, namespace: str, context: str | None) -> dict[str, str]:
    """Return the decoded ``data`` of a Kubernetes Secret, read through ``kubectl``.

    Raises:
        ValueError: If ``kubectl`` cannot read the Secret.
    """

    cmd = [*_kubectl(context), "get", "secret", secret, "-n", namespace, "-o", "json"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except FileNotFoundError as exc:
        raise ValueError("kubectl was not found on PATH; it is needed to read the Secret.") from exc
    if result.returncode != 0:
        raise ValueError(
            f"Cannot read Secret '{secret}' in namespace '{namespace}' "
            f"({shlex.join(cmd)}): {result.stderr.strip() or 'kubectl failed'}"
        )
    data = json.loads(result.stdout).get("data") or {}
    return {key: base64.b64decode(value).decode() for key, value in data.items()}


def _normalize_url(url: str) -> str:
    return url.strip().rstrip("/")


def check_prefect_api_matches(
    *,
    secrets: Sequence[str],
    namespace: str,
    context: str | None,
    read_secret: Callable[[str, str, str | None], Mapping[str, str]],
    current_api: Callable[[], tuple[str | None, str]],
) -> str:
    """Require the pod's Prefect API URL to equal the laptop's active profile URL.

    Returns:
        The shared API URL.

    Raises:
        ValueError: If no Secret provides ``PREFECT_API_URL``, the laptop has no
            URL configured, or the two differ.
    """

    pod_url: str | None = None
    pod_secret: str | None = None
    for secret in secrets:  # later Secrets override earlier ones, as with envFrom
        value = read_secret(secret, namespace, context).get(PREFECT_API_URL_KEY)
        if value:
            pod_url, pod_secret = value, secret
    if pod_url is None:
        raise ValueError(
            f"None of the Secrets {list(secrets)} in namespace '{namespace}' has the key "
            f"{PREFECT_API_URL_KEY}. Without it the flow pod would not use the same Prefect "
            "API as this machine."
        )

    laptop_url, profile = current_api()
    if not laptop_url:
        raise ValueError(
            "No PREFECT_API_URL is configured for the active Prefect profile "
            f"'{profile}'. Run 'prefect profile use <name>' or 'prefect config set "
            f"{PREFECT_API_URL_KEY}=<url>' first."
        )
    if _normalize_url(laptop_url) != _normalize_url(pod_url):
        raise ValueError(
            "The laptop and the flow pod use different Prefect APIs, so they would read "
            "different blocks.\n"
            f"  this machine (profile '{profile}'): {laptop_url}\n"
            f"  flow pod (Secret '{pod_secret}' in namespace '{namespace}'): {pod_url}\n"
            "Switch with 'prefect profile use <name>', or update the Secret."
        )
    return _normalize_url(laptop_url)


def block_name_in_command(command: Sequence[str]) -> str | None:
    """Return the ``--hpc-profile-block`` value of the flow command, if it has one."""

    for index, token in enumerate(command):
        if token == _BLOCK_FLAG and index + 1 < len(command):
            return command[index + 1]
        if token.startswith(f"{_BLOCK_FLAG}="):
            return token.partition("=")[2]
    return None


def resolve_block_name(command: Sequence[str], flag_value: str | None) -> str:
    """Return the profile block shared by the flow pod and its compute Jobs.

    The flow command's own ``--hpc-profile-block`` is used when it has one, so the
    block is named once. ``flag_value`` (the launcher's ``--hpc-profile-block``) is
    needed only when the flow command does not name a block.

    Raises:
        ValueError: If neither names a block, or they name different blocks.
    """

    in_command = block_name_in_command(command)
    if in_command and flag_value and in_command != flag_value:
        raise ValueError(
            f"The flow command uses {_BLOCK_FLAG} '{in_command}' but the launcher was given "
            f"'{flag_value}'. The flow pod and its compute Jobs must share one profile block."
        )
    name = in_command or flag_value
    if not name:
        raise ValueError(
            f"The flow command has no {_BLOCK_FLAG}, so the launcher cannot tell which profile "
            f"block the flow's compute Jobs use. Add {_BLOCK_FLAG} NAME to the flow command, "
            "or pass it to the launcher."
        )
    return name


def build_flow_job_manifest(
    *,
    command: Sequence[str],
    hpc_block: Any,
    hpc_profile_block_name: str,
    run_id: str,
    prefect_api_url: str | None = None,
    working_dir: str | None = None,
    env_from_secrets: Sequence[str] = (),
    walltime: str | None = None,
    ttl_seconds_after_finished: int | None = 3600,
) -> dict[str, Any]:
    """Build the ``batch/v1`` Job that runs a flow command in a pod.

    Image, namespace, service account, node placement and the shared volume come
    from ``hpc_block``. The pod receives ``QCSC_FLOW_RUN_ID`` and, through the
    downward API, its own name and UID, which
    `qcsc_prefect_executor.from_blocks.run_job_from_blocks` uses to label the
    compute Jobs it creates and to make the flow pod their owner. It also receives
    the block name and a fingerprint of the block settings, which the executor
    checks before creating any Job.

    Raises:
        ValueError: If ``command`` is empty, the block cannot host a flow pod, or
            the builder rejects the request (for example ``working_dir`` outside
            the block's mount path).
    """

    if not command:
        raise ValueError("A flow command is required after '--'.")
    validate_flow_block(hpc_block, hpc_profile_block_name)
    settings = flow_pod_settings(hpc_block)
    fingerprint = settings_fingerprint(settings)

    profile = ExecutionProfile(
        command_key="flow",
        num_nodes=1,
        walltime=walltime,
        arguments=list(command[1:]),
    )
    annotations = {
        HPC_PROFILE_BLOCK_ANNOTATION: hpc_profile_block_name,
        HPC_PROFILE_FINGERPRINT_ANNOTATION: fingerprint,
    }
    if prefect_api_url:
        annotations[PREFECT_API_URL_ANNOTATION] = prefect_api_url
    req = KubernetesJobRequest(
        image=settings["image"],
        executable=command[0],
        namespace=settings["namespace"] or "default",
        job_name=run_id,
        service_account_name=settings["service_account_name"],
        image_pull_policy=settings["image_pull_policy"],
        image_pull_secrets=tuple(settings["image_pull_secrets"]),
        node_selector=settings["node_selector"],
        tolerations=tuple(settings["tolerations"]),
        labels={FLOW_RUN_LABEL: run_id},
        annotations=annotations,
        ttl_seconds_after_finished=ttl_seconds_after_finished,
        work_volume_claim=settings["work_volume_claim"],
        work_mount_path=settings["work_mount_path"],
        working_dir=working_dir,
        env_from_secrets=tuple(env_from_secrets),
        extra_env=(
            {"name": FLOW_RUN_ID_ENV, "value": run_id},
            {"name": OWNER_POD_NAME_ENV, "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
            {"name": OWNER_POD_UID_ENV, "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid"}}},
            {"name": HPC_PROFILE_BLOCK_ENV, "value": hpc_profile_block_name},
            {"name": HPC_PROFILE_FINGERPRINT_ENV, "value": fingerprint},
        ),
    )
    return build_job_manifest(exec_profile=profile, req=req)


class _NoAliasDumper(yaml.SafeDumper):
    """Repeat shared dictionaries instead of emitting ``&id001``/``*id001`` aliases."""

    def ignore_aliases(self, data: Any) -> bool:
        return True


def write_manifest(manifest: dict[str, Any], run_dir: Path) -> Path:
    """Write ``manifest`` to ``run_dir/flow-job.yaml`` and return the path."""

    run_dir.mkdir(parents=True, exist_ok=False)
    path = run_dir / MANIFEST_FILENAME
    path.write_text(yaml.dump(manifest, Dumper=_NoAliasDumper, sort_keys=False))
    return path


def _kubectl(context: str | None) -> list[str]:
    return ["kubectl", *(["--context", context] if context else [])]


def _split_command(argv: Sequence[str]) -> tuple[list[str], list[str]]:
    if "--" not in argv:
        return list(argv), []
    index = list(argv).index("--")
    return list(argv[:index]), list(argv[index + 1 :])


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Render (and optionally launch) a Kubernetes Job that runs a Prefect flow. "
        "The flow pod is configured from the HPCProfileBlock named in the flow command.",
        usage="%(prog)s --env-from-secret SECRET [--hpc-profile-block NAME] [options] "
        "-- FLOW_COMMAND [ARGS...]",
    )
    parser.add_argument(
        _BLOCK_FLAG,
        dest="hpc_profile_block",
        metavar="NAME",
        help="HPCProfileBlock shared by this flow pod and its compute Jobs. Taken from the "
        f"flow command's own {_BLOCK_FLAG} when it has one; needed only when it does not.",
    )
    parser.add_argument(
        "--env-from-secret",
        action="append",
        required=True,
        metavar="SECRET",
        help="Secret whose keys become env vars; one of them must hold PREFECT_API_URL "
        "(and usually PREFECT_API_KEY). Repeatable.",
    )
    parser.add_argument(
        "--working-dir",
        help="Container working directory; must be inside the block's work mount path.",
    )
    parser.add_argument("--walltime", help="HH:MM:SS; becomes activeDeadlineSeconds.")
    parser.add_argument("--runs-dir", default=DEFAULT_RUNS_DIR, type=Path)
    parser.add_argument("--run-id", help="Override the generated run id.")
    parser.add_argument("--context", help="kubectl context to use.")
    launch = parser.add_mutually_exclusive_group()
    launch.add_argument("--launch", dest="launch", action="store_true", default=None)
    launch.add_argument("--no-launch", dest="launch", action="store_false")
    return parser


def _should_launch(
    launch: bool | None, *, input_fn: Callable[[str], str], interactive: bool
) -> bool:
    if launch is not None:
        return launch
    if not interactive:
        return False
    answer = input_fn("Launch the flow Job now with kubectl apply? [y/N] ")
    return answer.strip().lower() in {"y", "yes"}


def main(
    argv: Sequence[str] | None = None,
    *,
    input_fn: Callable[[str], str] = input,
    run: Callable[..., Any] = subprocess.run,
    interactive: bool | None = None,
    load_block: Callable[[str], Any] = load_hpc_block,
    read_secret: Callable[[str, str, str | None], Mapping[str, str]] = read_secret_data,
    current_api: Callable[[], tuple[str | None, str]] = current_prefect_api_url,
) -> int:
    """Check the setup, render the manifest, then ask whether to apply it.

    ``--launch`` and ``--no-launch`` skip the prompt. Without a terminal and
    without either flag, the manifest is written and not applied. Nothing is
    written if the block is unsuitable or the Prefect API URLs differ.
    """

    option_args, command = _split_command(sys.argv[1:] if argv is None else argv)
    args = _build_parser().parse_args(option_args)
    if interactive is None:
        interactive = sys.stdin.isatty()
    if not command:
        raise ValueError("A flow command is required after '--'.")
    block_name = resolve_block_name(command, args.hpc_profile_block)

    hpc_block = load_block(block_name)
    validate_flow_block(hpc_block, block_name)
    namespace = hpc_block.kubernetes_namespace
    api_url = check_prefect_api_matches(
        secrets=args.env_from_secret,
        namespace=namespace,
        context=args.context,
        read_secret=read_secret,
        current_api=current_api,
    )

    run_id = args.run_id or generate_run_id()
    manifest = build_flow_job_manifest(
        command=command,
        hpc_block=hpc_block,
        hpc_profile_block_name=block_name,
        run_id=run_id,
        prefect_api_url=api_url,
        working_dir=args.working_dir,
        env_from_secrets=args.env_from_secret,
        walltime=args.walltime,
    )
    run_dir = args.runs_dir / run_id
    path = write_manifest(manifest, run_dir)

    kubectl = shlex.join(_kubectl(args.context))
    print(f"Wrote {path}")
    print(f"  Block:   {block_name} (settings captured now; the flow re-checks them)")
    print(f"  Prefect: {api_url}")
    print(f"  Start:   {kubectl} apply -f {run_dir}/")
    print(f"  Logs:    {kubectl} logs -n {namespace} -f job/{run_id}")
    print(f"  Stop:    {kubectl} delete -f {run_dir}/")
    print(
        f"  Escape:  {kubectl} delete jobs -n {namespace} -l {FLOW_RUN_LABEL}={run_id}"
        "   # if the owner-reference cleanup missed anything"
    )

    if not _should_launch(args.launch, input_fn=input_fn, interactive=interactive):
        print("Not launched.")
        return 0

    result = run([*_kubectl(args.context), "apply", "-f", str(run_dir)], check=False)
    return int(getattr(result, "returncode", 0))


def cli() -> int:
    """Console entry point: print configuration errors without a traceback."""

    try:
        return main()
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(cli())
