# Prefect BitCount Demo

This example provides two execution styles:

- `flow_optimized.py`: block-driven execution (works on Miyabi, Fugaku, Slurm, and Kubernetes)
- `flow_tutorial_style.py`: legacy `counter.get(bitstrings)` style with optional HPC profile override

## Files

- `/Users/hitomi/Project/qcsc-prefect/examples/prefect_bitcount_demo/create_blocks.py`
- `/Users/hitomi/Project/qcsc-prefect/examples/prefect_bitcount_demo/bitcount_blocks.example.toml`
- `/Users/hitomi/Project/qcsc-prefect/examples/prefect_bitcount_demo/flow_optimized.py`
- `/Users/hitomi/Project/qcsc-prefect/examples/prefect_bitcount_demo/flow_tutorial_style.py`
- `/Users/hitomi/Project/qcsc-prefect/examples/prefect_bitcount_demo/quantum_sampling.py`
- `/Users/hitomi/Project/qcsc-prefect/examples/prefect_bitcount_demo/get_counts_integration.py`
- `/Users/hitomi/Project/qcsc-prefect/examples/prefect_bitcount_demo/build_on_miyabi.sh`
- `/Users/hitomi/Project/qcsc-prefect/examples/prefect_bitcount_demo/build_on_fugaku.sh`

For a local Slurm walkthrough using `slurm-docker-cluster`, see
[`docs/tutorials/create_qcsc_workflow_for_local_slurm.md`](../../docs/tutorials/create_qcsc_workflow_for_local_slurm.md).

## Build executable

Miyabi:

```bash
cd /Users/hitomi/Project/qcsc-prefect
./examples/prefect_bitcount_demo/build_on_miyabi.sh
```

Fugaku:

```bash
cd /Users/hitomi/Project/qcsc-prefect
./examples/prefect_bitcount_demo/build_on_fugaku.sh
```

Kubernetes (Generic base image):

```bash
cd /Users/hitomi/Project/qcsc-prefect
./docker/build_hpc_base.sh qcsc-hpc-base:latest
```

Compile `get_counts_hist` for the shared work directory or PVC:

```bash
mkdir -p examples/prefect_bitcount_demo/bin
mpicxx -O3 -std=c++17 \
  -o examples/prefect_bitcount_demo/bin/get_counts_hist \
  examples/prefect_bitcount_demo/src/get_counts_hist.cpp
```

## Create blocks

```bash
cd /Users/hitomi/Project/qcsc-prefect
cp examples/prefect_bitcount_demo/bitcount_blocks.example.toml \
   examples/prefect_bitcount_demo/bitcount_blocks.toml
```

Miyabi defaults:

```bash
python examples/prefect_bitcount_demo/create_blocks.py \
  --config examples/prefect_bitcount_demo/bitcount_blocks.toml
```

This default setup creates the optimized-flow assets only:

- `cmd-bitcount-hist`
- `exec-bitcount-mpi`
- `hpc-miyabi-bitcount`
- `miyabi-bitcount-options`

Fugaku:

```bash
python examples/prefect_bitcount_demo/create_blocks.py \
  --config examples/prefect_bitcount_demo/bitcount_blocks.toml \
  --hpc-target fugaku
```

Kubernetes:

```bash
python examples/prefect_bitcount_demo/create_blocks.py \
  --hpc-target kubernetes \
  --kubernetes-image qcsc-hpc-base:latest \
  --work-dir /data/qcsc-prefect/work/bitcount_k8s \
  --optimized-executable /data/qcsc-prefect/examples/prefect_bitcount_demo/bin/get_counts_hist
```

Local Slurm:

The Slurm execution path is supported by `flow_optimized.py`, but the current
`create_blocks.py` helper does not yet generate Slurm assets automatically.
For now, create the Slurm blocks manually as described in
[`docs/tutorials/create_qcsc_workflow_for_local_slurm.md`](../../docs/tutorials/create_qcsc_workflow_for_local_slurm.md).

## Run optimized flow

Miyabi example:

```bash
python examples/prefect_bitcount_demo/flow_optimized.py \
  --quantum-source real-device \
  --runtime-block ibm-runner \
  --command-block cmd-bitcount-hist \
  --execution-profile-block exec-bitcount-mpi \
  --hpc-profile-block hpc-miyabi-bitcount \
  --options-variable miyabi-bitcount-options
```

Fugaku example:

```bash
python examples/prefect_bitcount_demo/flow_optimized.py \
  --quantum-source real-device \
  --runtime-block ibm-runner \
  --command-block cmd-bitcount-hist \
  --execution-profile-block exec-bitcount-fugaku \
  --hpc-profile-block hpc-fugaku-bitcount \
  --options-variable fugaku-bitcount-options
```

Kubernetes example:

```bash
python examples/prefect_bitcount_demo/flow_optimized.py \
  --quantum-source random \
  --command-block cmd-bitcount-hist \
  --execution-profile-block exec-bitcount-k8s \
  --hpc-profile-block hpc-k8s-bitcount \
  --options-variable k8s-bitcount-options \
  --work-dir /data/qcsc-prefect/work/bitcount_k8s
```

`flow_optimized.py` resolves the base work directory in this order:
1. `--work-dir` (if provided)
2. `work_dir` in the options variable (`miyabi-bitcount-options` / `fugaku-bitcount-options`)
3. fallback: `./work/prefect_bitcount_optimized`

The scheduler script suffix is resolved from the selected `HPCProfileBlock`, so the same flow can use `.pbs` on Miyabi and `.pjm` on Fugaku without changing the flow code.

If you want to run the tutorial without IBM Quantum Runtime, switch the quantum source:

```bash
python examples/prefect_bitcount_demo/flow_optimized.py \
  --quantum-source random \
  --random-seed 24 \
  --command-block cmd-bitcount-hist \
  --execution-profile-block exec-bitcount-mpi \
  --hpc-profile-block hpc-miyabi-bitcount \
  --options-variable miyabi-bitcount-options
```

In `random` mode, the flow skips `QuantumRuntime.load(...)` and generates deterministic pseudo-random bitstrings using the requested shot count.

## Run legacy tutorial-style flow

The legacy tutorial assets are opt-in. Create them first on Miyabi:

```bash
python examples/prefect_bitcount_demo/create_blocks.py \
  --config examples/prefect_bitcount_demo/bitcount_blocks.toml \
  --hpc-target miyabi \
  --create-legacy-tutorial-assets
```

```bash
python examples/prefect_bitcount_demo/flow_tutorial_style.py \
  --quantum-source real-device
```

That creates these backward-compatible names:

- BitCounter block: `miyabi-tutorial`
- Prefect Variable: `miyabi-tutorial`

In the legacy flow, you can also override only the `HPCProfileBlock` at runtime
when the stored execution profile is already compatible with the target:

```bash
python examples/prefect_bitcount_demo/flow_tutorial_style.py \
  --bitcounter-block miyabi-tutorial \
  --options-variable miyabi-tutorial \
  --quantum-source random \
  --random-seed 24 \
  --hpc-profile-block-override hpc-fugaku-bitcount
```

## Merged Demo with `flow_optimized.py`

If you want to show Miyabi/Fugaku switching on the recommended path, use `flow_optimized.py`.
`flow_tutorial_style.py` is the legacy-compatible path, so it is not the recommended demo route for backend switching.

Use target-specific `ExecutionProfileBlock` and `HPCProfileBlock` names, while keeping the command block shared.
You can also keep `options_variable_name` shared when sampler options and `work_dir` are compatible across both targets.

Example config naming:

```toml
# Miyabi config
execution_profile_block_name = "exec-bitcount-miyabi"
hpc_profile_block_name = "hpc-miyabi-bitcount"
options_variable_name = "bitcount-options"

# Fugaku config
execution_profile_block_name = "exec-bitcount-fugaku"
hpc_profile_block_name = "hpc-fugaku-bitcount"
options_variable_name = "bitcount-options"
```

Then create the target-specific execution/HPC profile pairs:

```bash
python examples/prefect_bitcount_demo/create_blocks.py \
  --config examples/prefect_bitcount_demo/bitcount_blocks.miyabi.toml \
  --hpc-target miyabi

python examples/prefect_bitcount_demo/create_blocks.py \
  --config examples/prefect_bitcount_demo/bitcount_blocks.fugaku.toml \
  --hpc-target fugaku
```

If `bitcount-options` stores different `work_dir` values per target, pin a common demo directory with `--work-dir`.

After that, the optimized flow demo switches by changing the execution/HPC profile pair at runtime:

```bash
python examples/prefect_bitcount_demo/flow_optimized.py \
  --quantum-source real-device \
  --runtime-block ibm-runner \
  --command-block cmd-bitcount-hist \
  --execution-profile-block exec-bitcount-miyabi \
  --hpc-profile-block hpc-miyabi-bitcount \
  --options-variable bitcount-options \
  --work-dir /path/to/shared/bitcount_demo

python examples/prefect_bitcount_demo/flow_optimized.py \
  --quantum-source real-device \
  --runtime-block ibm-runner \
  --command-block cmd-bitcount-hist \
  --execution-profile-block exec-bitcount-fugaku \
  --hpc-profile-block hpc-fugaku-bitcount \
  --options-variable bitcount-options \
  --work-dir /path/to/shared/bitcount_demo
```

In these two commands, the changing runtime parameters are `--execution-profile-block` and `--hpc-profile-block`.
