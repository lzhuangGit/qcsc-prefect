from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any


def _import_bitcounter_class():
    # Ensure package-style imports work even when executed as:
    # python examples/prefect_bitcount_demo/create_blocks.py ...
    project_root = str(Path(__file__).resolve().parents[2])
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    try:
        from examples.prefect_bitcount_demo.get_counts_integration import BitCounter

        return BitCounter
    except ModuleNotFoundError as exc:
        # If the failure is unrelated to module path resolution, surface it.
        if exc.name not in {
            "examples",
            "examples.prefect_bitcount_demo",
            "examples.prefect_bitcount_demo.get_counts_integration",
        }:
            raise
        # Supports direct script execution:
        # python examples/prefect_bitcount_demo/create_blocks.py ...
        from get_counts_integration import BitCounter

        return BitCounter


def _env_int(name: str) -> int | None:
    raw = os.getenv(name, "").strip()
    if not raw:
        return None
    return int(raw)


def _normalize_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if not text:
            return None
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
    raise ValueError(f"Expected bool-compatible value, got: {value!r}")


def _split_csv(raw: str) -> list[str] | None:
    text = raw.strip()
    if not text:
        return None
    return [item.strip() for item in text.split(",") if item.strip()]


def _env_str(*names: str) -> str | None:
    for name in names:
        raw = os.getenv(name, "").strip()
        if raw:
            return raw
    return None


def _env_first_int(*names: str) -> int | None:
    for name in names:
        value = _env_int(name)
        if value is not None:
            return value
    return None


def _env_csv(*names: str) -> list[str] | None:
    for name in names:
        values = _split_csv(os.getenv(name, ""))
        if values:
            return values
    return None


def _env_bool(*names: str) -> bool | None:
    for name in names:
        value = _normalize_bool(os.getenv(name, ""))
        if value is not None:
            return value
    return None


def _resolve_example_path() -> Path:
    return Path(__file__).resolve().parent


def _register_block_types(*custom_block_classes) -> None:
    from qcsc_prefect_blocks.common.blocks import (
        CommandBlock,
        ExecutionProfileBlock,
        HPCProfileBlock,
    )

    block_types = [
        CommandBlock,
        ExecutionProfileBlock,
        HPCProfileBlock,
        *custom_block_classes,
    ]
    for block_cls in block_types:
        register = getattr(block_cls, "register_type_and_schema", None)
        if callable(register):
            register()


def _set_variable(variable_name: str, shots: int, work_dir: str) -> None:
    value = json.dumps(
        {
            "sampler_options": {"params": {"shots": shots}},
            "work_dir": str(Path(work_dir).expanduser().resolve()),
        }
    )
    subprocess.run(
        ["prefect", "variable", "set", variable_name, value, "--overwrite"],
        check=True,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=("Create Prefect blocks for BitCount tutorial (Miyabi or Fugaku).")
    )
    parser.add_argument("--config", type=Path, help="Path to TOML/JSON config file.")

    parser.add_argument("--hpc-target", choices=["miyabi", "fugaku", "kubernetes"])
    parser.add_argument("--project")
    parser.add_argument("--group")
    parser.add_argument("--queue")
    parser.add_argument("--work-dir")
    parser.add_argument("--launcher")
    parser.add_argument("--walltime")
    parser.add_argument("--num-nodes", type=int)
    parser.add_argument("--mpiprocs", type=int)
    parser.add_argument("--ompthreads", type=int)
    parser.add_argument("--shots", type=int)
    parser.add_argument("--modules", nargs="+")
    parser.add_argument("--mpi-options", nargs="*")

    parser.add_argument("--fugaku-gfscache")
    parser.add_argument("--fugaku-spack-modules", nargs="+")
    parser.add_argument("--fugaku-mpi-options-for-pjm", nargs="*")

    parser.add_argument("--kubernetes-image")
    parser.add_argument("--kubernetes-namespace")
    parser.add_argument("--kubernetes-service-account-name")
    parser.add_argument("--kubernetes-image-pull-policy")
    parser.add_argument("--kubernetes-cpu")
    parser.add_argument("--kubernetes-memory")
    parser.add_argument("--kubernetes-work-volume-claim")
    parser.add_argument("--kubernetes-work-mount-path")

    parser.add_argument("--optimized-executable")
    parser.add_argument("--command-block-name")
    parser.add_argument("--execution-profile-block-name")
    parser.add_argument("--hpc-profile-block-name")
    parser.add_argument("--options-variable-name")
    parser.add_argument(
        "--create-legacy-tutorial-assets",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Create the legacy BitCounter/tutorial variable assets (opt-in; Miyabi only).",
    )
    parser.add_argument("--tutorial-variable-name")
    parser.add_argument("--bitcounter-block-name")
    return parser.parse_args()


def _normalize_str_list(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    raise ValueError(f"Expected list[str] or CSV string, got: {type(value)}")


def _load_config_file(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    if not path.exists():
        raise FileNotFoundError(f"Config file was not found: {path}")
    if path.suffix.lower() == ".json":
        return json.loads(path.read_text(encoding="utf-8"))
    with path.open("rb") as f:
        return tomllib.load(f)


def _pick_value(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _resolve_legacy_tutorial_asset_creation(
    *,
    requested: bool | None,
    tutorial_variable_name: str | None,
    bitcounter_block_name: str | None,
) -> bool:
    if requested is not None:
        return requested
    return tutorial_variable_name is not None or bitcounter_block_name is not None


def _env_values() -> dict[str, Any]:
    return {
        "hpc_target": _env_str("BITCOUNT_HPC_TARGET"),
        "project": _env_str("BITCOUNT_PROJECT", "MIYABI_PBS_PROJECT", "FUGAKU_PROJECT"),
        "group": _env_str("BITCOUNT_GROUP", "FUGAKU_GROUP", "FUGAKU_PROJECT"),
        "queue": _env_str("BITCOUNT_QUEUE", "MIYABI_PBS_QUEUE", "FUGAKU_RSCGRP"),
        "work_dir": _env_str(
            "BITCOUNT_WORK_DIR",
            "MIYABI_BITCOUNT_WORK_DIR",
            "FUGAKU_BITCOUNT_WORK_DIR",
        ),
        "launcher": _env_str("BITCOUNT_LAUNCHER", "MIYABI_LAUNCHER", "FUGAKU_LAUNCHER"),
        "walltime": _env_str("BITCOUNT_WALLTIME", "MIYABI_WALLTIME", "FUGAKU_WALLTIME"),
        "num_nodes": _env_first_int("BITCOUNT_NUM_NODES", "MIYABI_NUM_NODES", "FUGAKU_NUM_NODES"),
        "mpiprocs": _env_first_int("BITCOUNT_MPIPROCS", "MIYABI_MPIPROCS", "FUGAKU_MPIPROCS"),
        "ompthreads": _env_first_int(
            "BITCOUNT_OMPTHREADS", "MIYABI_OMPTHREADS", "FUGAKU_OMPTHREADS"
        ),
        "shots": _env_first_int("BITCOUNT_SHOTS"),
        "modules": _env_csv("BITCOUNT_MODULES", "MIYABI_MODULES"),
        "mpi_options": _env_csv("BITCOUNT_MPI_OPTIONS", "MIYABI_MPI_OPTIONS", "FUGAKU_MPI_OPTIONS"),
        "fugaku_gfscache": _env_str("FUGAKU_GFSCACHE"),
        "fugaku_spack_modules": _env_csv("FUGAKU_SPACK_MODULES"),
        "fugaku_mpi_options_for_pjm": _env_csv("FUGAKU_MPI_OPTIONS_FOR_PJM"),
        "kubernetes_image": _env_str("BITCOUNT_KUBERNETES_IMAGE", "KUBERNETES_IMAGE"),
        "kubernetes_namespace": _env_str("BITCOUNT_KUBERNETES_NAMESPACE", "KUBERNETES_NAMESPACE"),
        "kubernetes_service_account_name": _env_str(
            "BITCOUNT_KUBERNETES_SERVICE_ACCOUNT", "KUBERNETES_SERVICE_ACCOUNT"
        ),
        "kubernetes_image_pull_policy": _env_str(
            "BITCOUNT_KUBERNETES_IMAGE_PULL_POLICY", "KUBERNETES_IMAGE_PULL_POLICY"
        ),
        "kubernetes_cpu": _env_str("BITCOUNT_KUBERNETES_CPU", "KUBERNETES_CPU"),
        "kubernetes_memory": _env_str("BITCOUNT_KUBERNETES_MEMORY", "KUBERNETES_MEMORY"),
        "kubernetes_work_volume_claim": _env_str(
            "BITCOUNT_KUBERNETES_WORK_VOLUME_CLAIM", "KUBERNETES_WORK_VOLUME_CLAIM"
        ),
        "kubernetes_work_mount_path": _env_str(
            "BITCOUNT_KUBERNETES_WORK_MOUNT_PATH", "KUBERNETES_WORK_MOUNT_PATH"
        ),
        "optimized_executable": os.getenv("BITCOUNT_OPT_EXECUTABLE", "").strip() or None,
        "command_block_name": os.getenv("BITCOUNT_CMD_BLOCK_NAME", "").strip() or None,
        "execution_profile_block_name": (
            os.getenv("BITCOUNT_EXEC_PROFILE_BLOCK_NAME", "").strip() or None
        ),
        "hpc_profile_block_name": os.getenv("BITCOUNT_HPC_PROFILE_BLOCK_NAME", "").strip() or None,
        "options_variable_name": os.getenv("BITCOUNT_OPTIONS_VARIABLE", "").strip() or None,
        "create_legacy_tutorial_assets": _env_bool("BITCOUNT_CREATE_LEGACY_TUTORIAL_ASSETS"),
        "tutorial_variable_name": os.getenv("BITCOUNT_TUTORIAL_VARIABLE", "").strip() or None,
        "bitcounter_block_name": os.getenv("BITCOUNTER_BLOCK_NAME", "").strip() or None,
    }


def main() -> None:
    args = _parse_args()
    from qcsc_prefect_blocks.common.blocks import (
        CommandBlock,
        ExecutionProfileBlock,
        HPCProfileBlock,
    )

    config = _load_config_file(args.config)
    env = _env_values()

    hpc_target = (
        str(
            _pick_value(
                args.hpc_target,
                config.get("hpc_target"),
                env.get("hpc_target"),
                "miyabi",
            )
        )
        .strip()
        .lower()
    )
    if hpc_target not in {"miyabi", "fugaku", "kubernetes"}:
        raise RuntimeError("'hpc_target' must be 'miyabi', 'fugaku', or 'kubernetes'.")
    is_miyabi = hpc_target == "miyabi"
    is_kubernetes = hpc_target == "kubernetes"

    legacy_tutorial_requested = _resolve_legacy_tutorial_asset_creation(
        requested=_pick_value(
            args.create_legacy_tutorial_assets,
            _normalize_bool(config.get("create_legacy_tutorial_assets")),
            env.get("create_legacy_tutorial_assets"),
        ),
        tutorial_variable_name=_pick_value(
            args.tutorial_variable_name,
            config.get("tutorial_variable_name"),
            env.get("tutorial_variable_name"),
        ),
        bitcounter_block_name=_pick_value(
            args.bitcounter_block_name,
            config.get("bitcounter_block_name"),
            env.get("bitcounter_block_name"),
        ),
    )
    create_legacy_tutorial_assets = is_miyabi and legacy_tutorial_requested

    bit_counter_cls = _import_bitcounter_class() if create_legacy_tutorial_assets else None
    if bit_counter_cls is not None:
        _register_block_types(bit_counter_cls)
    else:
        _register_block_types()

    example_dir = _resolve_example_path()
    default_optimized_exec = str((example_dir / "bin/get_counts_hist").resolve())

    if is_kubernetes:
        project = _pick_value(
            args.project,
            config.get("project"),
            env.get("project"),
            args.group,
            config.get("group"),
            env.get("group"),
            "",
        )
    elif is_miyabi:
        project = _pick_value(
            args.project,
            config.get("project"),
            env.get("project"),
            args.group,
            config.get("group"),
            env.get("group"),
        )
    else:
        project = _pick_value(
            args.group,
            config.get("group"),
            env.get("group"),
            args.project,
            config.get("project"),
            env.get("project"),
        )
    if not project and not is_kubernetes:
        if is_miyabi:
            raise RuntimeError(
                "Set 'project' in --config/--project or environment "
                "(BITCOUNT_PROJECT, MIYABI_PBS_PROJECT)."
            )
        raise RuntimeError(
            "Set 'group' in --config/--group (preferred) or 'project' for compatibility, "
            "or use BITCOUNT_GROUP/FUGAKU_GROUP/FUGAKU_PROJECT."
        )

    queue = _pick_value(
        args.queue,
        config.get("queue"),
        env.get("queue"),
        "" if is_kubernetes else None,
    )
    if not queue and not is_kubernetes:
        raise RuntimeError(
            "Set 'queue' in --config/--queue or environment "
            "(BITCOUNT_QUEUE, MIYABI_PBS_QUEUE, FUGAKU_RSCGRP)."
        )

    if is_kubernetes:
        launcher_default = "single"
    elif is_miyabi:
        launcher_default = "mpiexec.hydra"
    else:
        launcher_default = "mpiexec"
    launcher = str(
        _pick_value(
            args.launcher,
            config.get("launcher"),
            env.get("launcher"),
            launcher_default,
        )
    ).strip()
    walltime = str(
        _pick_value(
            args.walltime,
            config.get("walltime"),
            env.get("walltime"),
            "00:15:00",
        )
    ).strip()

    default_nodes = 1 if is_kubernetes else 2
    default_mpiprocs = 1 if is_kubernetes else 5
    num_nodes = int(
        _pick_value(
            args.num_nodes,
            config.get("num_nodes"),
            env.get("num_nodes"),
            default_nodes,
        )
    )
    mpiprocs = int(
        _pick_value(
            args.mpiprocs,
            config.get("mpiprocs"),
            env.get("mpiprocs"),
            default_mpiprocs,
        )
    )
    ompthreads = int(
        _pick_value(args.ompthreads, config.get("ompthreads"), env.get("ompthreads"), 1)
    )
    shots = int(_pick_value(args.shots, config.get("shots"), env.get("shots"), 100000))

    modules_default = ["intel/2023.2.0", "impi/2021.10.0"] if is_miyabi else []
    modules = _normalize_str_list(
        _pick_value(args.modules, config.get("modules"), env.get("modules"), modules_default)
    )
    mpi_options = _normalize_str_list(
        _pick_value(args.mpi_options, config.get("mpi_options"), env.get("mpi_options"), [])
    )

    fugaku_gfscache = str(
        _pick_value(
            args.fugaku_gfscache,
            config.get("fugaku_gfscache"),
            env.get("fugaku_gfscache"),
            "/vol0002",
        )
    ).strip()
    fugaku_spack_modules = _normalize_str_list(
        _pick_value(
            args.fugaku_spack_modules,
            config.get("fugaku_spack_modules"),
            env.get("fugaku_spack_modules"),
            [],
        )
    )
    fugaku_mpi_options_for_pjm = _normalize_str_list(
        _pick_value(
            args.fugaku_mpi_options_for_pjm,
            config.get("fugaku_mpi_options_for_pjm"),
            env.get("fugaku_mpi_options_for_pjm"),
            [],
        )
    )

    kubernetes_image = str(
        _pick_value(
            args.kubernetes_image,
            config.get("kubernetes_image"),
            env.get("kubernetes_image"),
            "qcsc-hpc-base:latest",
        )
    ).strip()
    kubernetes_namespace = str(
        _pick_value(
            args.kubernetes_namespace,
            config.get("kubernetes_namespace"),
            env.get("kubernetes_namespace"),
            "default",
        )
    ).strip()
    kubernetes_service_account_name = _pick_value(
        args.kubernetes_service_account_name,
        config.get("kubernetes_service_account_name"),
        env.get("kubernetes_service_account_name"),
    )
    kubernetes_image_pull_policy = str(
        _pick_value(
            args.kubernetes_image_pull_policy,
            config.get("kubernetes_image_pull_policy"),
            env.get("kubernetes_image_pull_policy"),
            "IfNotPresent",
        )
    ).strip()
    kubernetes_cpu = _pick_value(
        args.kubernetes_cpu,
        config.get("kubernetes_cpu"),
        env.get("kubernetes_cpu"),
    )
    kubernetes_memory = _pick_value(
        args.kubernetes_memory,
        config.get("kubernetes_memory"),
        env.get("kubernetes_memory"),
    )
    kubernetes_work_volume_claim = _pick_value(
        args.kubernetes_work_volume_claim,
        config.get("kubernetes_work_volume_claim"),
        env.get("kubernetes_work_volume_claim"),
    )
    kubernetes_work_mount_path = _pick_value(
        args.kubernetes_work_mount_path,
        config.get("kubernetes_work_mount_path"),
        env.get("kubernetes_work_mount_path"),
    )

    optimized_exec = str(
        _pick_value(
            args.optimized_executable,
            config.get("optimized_executable"),
            env.get("optimized_executable"),
            default_optimized_exec,
        )
    ).strip()

    work_dir_raw = _pick_value(args.work_dir, config.get("work_dir"), env.get("work_dir"))
    if not work_dir_raw:
        raise RuntimeError(
            "Set 'work_dir' in --config, --work-dir, or "
            "BITCOUNT_WORK_DIR/MIYABI_BITCOUNT_WORK_DIR/FUGAKU_BITCOUNT_WORK_DIR."
        )
    work_dir = os.path.expandvars(str(work_dir_raw).strip())

    if is_kubernetes:
        default_exec_block_name = "exec-bitcount-k8s"
        default_hpc_block_name = "hpc-k8s-bitcount"
        default_options_variable = "k8s-bitcount-options"
    elif is_miyabi:
        default_exec_block_name = "exec-bitcount-mpi"
        default_hpc_block_name = "hpc-miyabi-bitcount"
        default_options_variable = "miyabi-bitcount-options"
    else:
        default_exec_block_name = "exec-bitcount-fugaku"
        default_hpc_block_name = "hpc-fugaku-bitcount"
        default_options_variable = "fugaku-bitcount-options"

    cmd_block_name = str(
        _pick_value(
            args.command_block_name,
            config.get("command_block_name"),
            env.get("command_block_name"),
            "cmd-bitcount-hist",
        )
    ).strip()
    exec_block_name = str(
        _pick_value(
            args.execution_profile_block_name,
            config.get("execution_profile_block_name"),
            env.get("execution_profile_block_name"),
            default_exec_block_name,
        )
    ).strip()
    hpc_block_name = str(
        _pick_value(
            args.hpc_profile_block_name,
            config.get("hpc_profile_block_name"),
            env.get("hpc_profile_block_name"),
            default_hpc_block_name,
        )
    ).strip()
    options_variable_name = str(
        _pick_value(
            args.options_variable_name,
            config.get("options_variable_name"),
            env.get("options_variable_name"),
            default_options_variable,
        )
    ).strip()

    tutorial_variable_name: str | None = None
    bitcounter_block_name: str | None = None
    if create_legacy_tutorial_assets:
        tutorial_variable_name = str(
            _pick_value(
                args.tutorial_variable_name,
                config.get("tutorial_variable_name"),
                env.get("tutorial_variable_name"),
                "miyabi-tutorial",
            )
        ).strip()
        bitcounter_block_name = str(
            _pick_value(
                args.bitcounter_block_name,
                config.get("bitcounter_block_name"),
                env.get("bitcounter_block_name"),
                "miyabi-tutorial",
            )
        ).strip()

    CommandBlock(
        command_name="bitcount-hist",
        executable_key="bitcount_hist",
        description="Optimized bitcount executable (binary histogram output)",
        default_args=[],
    ).save(cmd_block_name, overwrite=True)

    ExecutionProfileBlock(
        profile_name="bitcount-mpi",
        command_name="bitcount-hist",
        resource_class="cpu",
        num_nodes=num_nodes,
        mpiprocs=mpiprocs,
        ompthreads=ompthreads,
        walltime=walltime,
        launcher=launcher,
        mpi_options=mpi_options or [],
        modules=modules or [],
        environments={},
    ).save(exec_block_name, overwrite=True)

    if is_kubernetes:
        HPCProfileBlock(
            hpc_target="kubernetes",
            kubernetes_image=kubernetes_image,
            kubernetes_namespace=kubernetes_namespace,
            kubernetes_service_account_name=kubernetes_service_account_name,
            kubernetes_image_pull_policy=kubernetes_image_pull_policy,
            kubernetes_cpu=kubernetes_cpu,
            kubernetes_memory=kubernetes_memory,
            kubernetes_work_volume_claim=kubernetes_work_volume_claim,
            kubernetes_work_mount_path=kubernetes_work_mount_path,
            executable_map={"bitcount_hist": optimized_exec},
        ).save(hpc_block_name, overwrite=True)
    elif is_miyabi:
        HPCProfileBlock(
            hpc_target="miyabi",
            queue_cpu=str(queue),
            queue_gpu="regular-g",
            project_cpu=str(project),
            project_gpu=str(project),
            executable_map={"bitcount_hist": optimized_exec},
        ).save(hpc_block_name, overwrite=True)
    else:
        HPCProfileBlock(
            hpc_target="fugaku",
            queue_cpu=str(queue),
            queue_gpu=str(queue),
            project_cpu=str(project),
            project_gpu=str(project),
            executable_map={"bitcount_hist": optimized_exec},
            gfscache=fugaku_gfscache or None,
            spack_modules=fugaku_spack_modules or [],
            mpi_options_for_pjm=fugaku_mpi_options_for_pjm or [],
        ).save(hpc_block_name, overwrite=True)

    _set_variable(options_variable_name, shots, work_dir)

    if create_legacy_tutorial_assets:
        bit_counter_cls(
            root_dir=work_dir,
            command_block_name=cmd_block_name,
            execution_profile_block_name=exec_block_name,
            hpc_profile_block_name=hpc_block_name,
            script_filename="bitcount_facade.pbs",
            metrics_artifact_key="miyabi-bitcount-facade-metrics",
            bitlen=10,
            user_args=[],
        ).save(bitcounter_block_name, overwrite=True)

        if tutorial_variable_name and tutorial_variable_name != options_variable_name:
            _set_variable(tutorial_variable_name, shots, work_dir)

    print(f"Saved blocks and variables for BitCount demo (target={hpc_target})")
    print(f"  Command block: {cmd_block_name}")
    print(f"  Execution profile block: {exec_block_name}")
    print(f"  HPC profile block: {hpc_block_name}")
    print(f"  Options variable: {options_variable_name}")
    print(f"  Work directory: {work_dir}")
    print(f"  Optimized executable: {optimized_exec}")

    if create_legacy_tutorial_assets:
        print(f"  BitCounter block: {bitcounter_block_name}")
        if tutorial_variable_name:
            print(f"  Tutorial variable: {tutorial_variable_name}")
    elif legacy_tutorial_requested and not is_miyabi:
        print(
            "  Legacy tutorial-style assets were requested, but are only created "
            "when hpc_target=miyabi."
        )
        print("  Use a Miyabi config to create the BitCounter facade.")
        print(
            "  For cross-target demos, use flow_optimized.py and switch the "
            "execution/HPC profile pair."
        )
    elif is_miyabi:
        print("  Legacy tutorial-style BitCounter block is not created by default.")
        print(
            "  Pass --create-legacy-tutorial-assets to create the optional "
            "BitCounter facade and tutorial variable."
        )
    else:
        print("  Legacy tutorial-style BitCounter block is not created for fugaku target.")
        print(
            "  Use flow_optimized.py with "
            "--command-block/--execution-profile-block/--hpc-profile-block."
        )


if __name__ == "__main__":
    main()
