"""Launch a Prefect flow as a batch Job inside a Kubernetes cluster.

The flow runs in a pod, next to the compute Jobs it creates, so it sees the
same shared volume at the same path. Run this module from a laptop (or any
machine with ``kubectl``) to render the Job manifest into
``<runs-dir>/<run-id>/flow-job.yaml`` and optionally apply it.

Cleanup is two-layered:

* ``kubectl delete -f <runs-dir>/<run-id>/`` deletes the flow Job. Compute Jobs
  created by the flow carry an ``ownerReference`` to the flow pod, so Kubernetes
  garbage-collects them as well.
* If garbage collection does not remove them, every Job of the run carries the
  label ``qcsc-prefect/flow-run=<run-id>`` for
  ``kubectl delete jobs -l qcsc-prefect/flow-run=<run-id>``.

Example::

    python -m qcsc_prefect_executor.kubernetes.flow_launcher \\
        --image quay.io/example/qcsc-hpc-base:latest --namespace qcsc \\
        --work-volume-claim ldata-pvc --work-mount-path /ldata \\
        --service-account qcsc-prefect-runner \\
        --env-from-secret prefect-credentials \\
        -- /ldata/qcsc-prefect/.venv/bin/python \\
           /ldata/qcsc-prefect/examples/prefect_bitcount_demo/flow_optimized.py \\
           --quantum-source random
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
import uuid
from collections.abc import Callable, Sequence
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

MANIFEST_FILENAME = "flow-job.yaml"
DEFAULT_RUNS_DIR = "runs"


def generate_run_id() -> str:
    """Return a unique, DNS-1123 safe id such as ``flow-3f9a1c2b``."""

    return f"flow-{uuid.uuid4().hex[:8]}"


def _parse_node_selector(items: Sequence[str]) -> dict[str, str]:
    selector: dict[str, str] = {}
    for item in items:
        key, sep, value = item.partition("=")
        if not sep or not key.strip():
            raise ValueError(f"Node selector must be 'key=value', got: {item!r}")
        selector[key.strip()] = value.strip()
    return selector


def _parse_toleration(item: str) -> dict[str, str]:
    """Parse ``key[=value][:effect]``; no value means operator ``Exists``."""

    key_value, _, effect = item.partition(":")
    key, sep, value = key_value.partition("=")
    if not key.strip():
        raise ValueError(f"Toleration must be 'key[=value][:effect]', got: {item!r}")
    toleration = {"key": key.strip(), "operator": "Equal" if sep else "Exists"}
    if sep:
        toleration["value"] = value.strip()
    if effect.strip():
        toleration["effect"] = effect.strip()
    return toleration


def build_flow_job_manifest(
    *,
    command: Sequence[str],
    image: str,
    namespace: str,
    run_id: str,
    work_volume_claim: str | None = None,
    work_mount_path: str | None = None,
    working_dir: str | None = None,
    service_account_name: str | None = None,
    env_from_secrets: Sequence[str] = (),
    image_pull_policy: str | None = "IfNotPresent",
    image_pull_secrets: Sequence[str] = (),
    node_selector: dict[str, str] | None = None,
    tolerations: Sequence[dict[str, str]] = (),
    walltime: str | None = None,
    ttl_seconds_after_finished: int | None = 3600,
) -> dict[str, Any]:
    """Build the ``batch/v1`` Job that runs a flow command in a pod.

    The pod receives ``QCSC_FLOW_RUN_ID`` and, through the downward API, its own
    name and UID. `qcsc_prefect_executor.from_blocks.run_job_from_blocks` reads
    them to label the compute Jobs it creates and to make the flow pod their
    owner.

    Raises:
        ValueError: If ``command`` is empty, or the builder rejects the request
            (for example ``working_dir`` outside ``work_mount_path``).
    """

    if not command:
        raise ValueError("A flow command is required after '--'.")

    profile = ExecutionProfile(
        command_key="flow",
        num_nodes=1,
        walltime=walltime,
        arguments=list(command[1:]),
    )
    req = KubernetesJobRequest(
        image=image,
        executable=command[0],
        namespace=namespace,
        job_name=run_id,
        service_account_name=service_account_name,
        image_pull_policy=image_pull_policy,
        image_pull_secrets=tuple(image_pull_secrets),
        node_selector=dict(node_selector or {}),
        tolerations=tuple(tolerations),
        labels={FLOW_RUN_LABEL: run_id},
        ttl_seconds_after_finished=ttl_seconds_after_finished,
        work_volume_claim=work_volume_claim,
        work_mount_path=work_mount_path,
        working_dir=working_dir,
        env_from_secrets=tuple(env_from_secrets),
        extra_env=(
            {"name": FLOW_RUN_ID_ENV, "value": run_id},
            {"name": OWNER_POD_NAME_ENV, "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
            {"name": OWNER_POD_UID_ENV, "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid"}}},
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
        description="Render (and optionally launch) a Kubernetes Job that runs a Prefect flow.",
        usage="%(prog)s [options] -- FLOW_COMMAND [ARGS...]",
    )
    parser.add_argument("--image", required=True, help="Container image that runs the flow.")
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--work-volume-claim", help="Shared RWX PVC mounted into the flow pod.")
    parser.add_argument("--work-mount-path", help="Absolute mount path of the PVC.")
    parser.add_argument(
        "--working-dir", help="Container working directory; must be inside --work-mount-path."
    )
    parser.add_argument(
        "--service-account",
        help="Service account that may create Jobs, e.g. qcsc-prefect-runner.",
    )
    parser.add_argument(
        "--env-from-secret",
        action="append",
        default=[],
        metavar="SECRET",
        help="Secret whose keys become env vars (e.g. PREFECT_API_URL, PREFECT_API_KEY). "
        "Repeatable.",
    )
    parser.add_argument("--image-pull-policy", default="IfNotPresent")
    parser.add_argument("--image-pull-secret", action="append", default=[], metavar="SECRET")
    parser.add_argument("--node-selector", action="append", default=[], metavar="KEY=VALUE")
    parser.add_argument("--toleration", action="append", default=[], metavar="KEY[=VALUE][:EFFECT]")
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
) -> int:
    """Render the manifest, then ask whether to apply it.

    ``--launch`` and ``--no-launch`` skip the prompt. Without a terminal and
    without either flag, the manifest is written and not applied.
    """

    option_args, command = _split_command(sys.argv[1:] if argv is None else argv)
    args = _build_parser().parse_args(option_args)
    if interactive is None:
        interactive = sys.stdin.isatty()

    run_id = args.run_id or generate_run_id()
    manifest = build_flow_job_manifest(
        command=command,
        image=args.image,
        namespace=args.namespace,
        run_id=run_id,
        work_volume_claim=args.work_volume_claim,
        work_mount_path=args.work_mount_path,
        working_dir=args.working_dir,
        service_account_name=args.service_account,
        env_from_secrets=args.env_from_secret,
        image_pull_policy=args.image_pull_policy,
        image_pull_secrets=args.image_pull_secret,
        node_selector=_parse_node_selector(args.node_selector),
        tolerations=[_parse_toleration(t) for t in args.toleration],
        walltime=args.walltime,
    )
    run_dir = args.runs_dir / run_id
    path = write_manifest(manifest, run_dir)

    kubectl = shlex.join(_kubectl(args.context))
    print(f"Wrote {path}")
    print(f"  Start:   {kubectl} apply -f {run_dir}/")
    print(f"  Logs:    {kubectl} logs -n {args.namespace} -f job/{run_id}")
    print(f"  Stop:    {kubectl} delete -f {run_dir}/")
    print(
        f"  Escape:  {kubectl} delete jobs -n {args.namespace} -l {FLOW_RUN_LABEL}={run_id}"
        "   # if the owner-reference cleanup missed anything"
    )

    if not _should_launch(args.launch, input_fn=input_fn, interactive=interactive):
        print("Not launched.")
        return 0

    result = run([*_kubectl(args.context), "apply", "-f", str(run_dir)], check=False)
    return int(getattr(result, "returncode", 0))


if __name__ == "__main__":
    raise SystemExit(main())
