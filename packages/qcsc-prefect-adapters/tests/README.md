# qcsc-prefect-adapters — test suite

This directory contains all tests for the `qcsc-prefect-adapters` package.
Most tests are fast unit tests that run without any external infrastructure.
The kind-based tests described below are the only exception.

---

## Unit tests

| File | What it covers |
|---|---|
| `test_kubernetes_builder.py` | Manifest construction, name sanitisation, walltime parsing |
| `test_kubernetes_runtime.py` | `KubernetesRuntime` logic with a fake API client |
| `test_local_runtime.py` | Local subprocess runtime |
| `test_slurm_builder.py` | Slurm batch script rendering |
| `test_slurm_runtime.py` | Slurm runtime with a fake SSH/qstat shim |
| `test_miyabi_builder.py` | Miyabi (PBS) script rendering |
| `test_miyabi_runtime.py` | Miyabi runtime with a fake qsub/qstat shim |
| `test_fugaku_builder.py` | Fugaku (PJM) script rendering |
| `test_fugaku_queue.py` | Fugaku queue helpers |
| `test_fugaku_runtime.py` | Fugaku runtime with a fake pjsub/pjstat shim |

Run all unit tests from the repository root:

```bash
pytest
```

---

## Kind integration tests (`test_kubernetes_kind.py`)

These tests exercise the full `KubernetesRuntime` against a real, ephemeral
[kind](https://kind.sigs.k8s.io/) cluster running on the local machine.
They are **skipped by default** and must be opted in explicitly.

### Prerequisites

| Requirement | Minimum version | Install |
|---|---|---|
| [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation) | 0.20 | `brew install kind` / release binary |
| [kubectl](https://kubernetes.io/docs/tasks/tools/) | 1.27 | `brew install kubectl` / release binary |
| Docker (or containerd) | any recent | [Docker Desktop](https://www.docker.com/products/docker-desktop/) |
| `kubernetes_asyncio` Python package | ≥ 29.0 | `uv sync --all-packages --extra kubernetes` |

Verify your setup:

```bash
kind version
kubectl version --client
docker info
```

### Running the tests

Pass the `--kind` flag to enable the suite:

```bash
# From the repository root — run only the kind tests
pytest packages/qcsc-prefect-adapters/tests/test_kubernetes_kind.py --kind -v

# Alternatively, select by marker
pytest -m kind_integration --kind
```

The first run takes **60–90 seconds** while the kind cluster is created and
`busybox:1.36` is preloaded into it.  Subsequent runs inside the same session
reuse the same cluster (the fixture is session-scoped).  The cluster is always
deleted at the end of the session, even on test failure.

### What is tested

| Test | Scenario |
|---|---|
| `test_submit_returns_namespaced_id` | Job is created; `SubmitResult.job_id` has the `namespace/name` shape |
| `test_wait_final_status_success` | Exit-0 job → `State=COMPLETED`, `ExitCode=0:0`, non-empty `PodName` |
| `test_wait_final_status_failure` | `sh -c "exit 42"` → `State=FAILED`, `ExitCode=42:0` |
| `test_fetch_logs` | Pod stdout is returned verbatim via `fetch_logs()` |
| `test_cancel_deletes_job` | Running job is deleted; a second `cancel()` on the same ID is a no-op |
| `test_wait_final_status_timeout_cancels_job` | `WaitTimeout` is raised and the timed-out job is cleaned up from the cluster |

### How the fixture works

```
pytest session start
    └─ kind_kubeconfig (session-scoped fixture)
            ├─ kind create cluster --name qcsc-test-<hex8> --wait 90s
            ├─ kind load docker-image busybox:1.36   (best-effort pre-pull)
            ├─ yield /tmp/.../kind.kubeconfig        ← all tests run here
            └─ kind delete cluster --name qcsc-test-<hex8>   (teardown)
```

Each test gets a fresh `KubernetesRuntime` instance (function-scoped `runtime`
fixture) pointed at the shared cluster via
`KubernetesConnection(kubeconfig_path=..., in_cluster=False)`.

Job names include a short UUID suffix so parallel test runs do not collide.
`ttl_seconds_after_finished` is set to `None` on all test jobs so pods are
retained long enough for log assertions — the cluster teardown removes
everything anyway.

### Continuous integration

The kind tests run in a dedicated `kind` job in
[`.github/workflows/ci.yml`](../../../.github/workflows/ci.yml).
That job installs kind from the official release binary, then runs:

```bash
pytest packages/qcsc-prefect-adapters/tests/test_kubernetes_kind.py \
    --kind -v -m kind_integration
```

The `ci-ok` gate job depends on this job, so a kind test failure blocks merge.

### Troubleshooting

**Tests are skipped even with `--kind`**

Check that `kind` and `kubectl` are on your `PATH`:

```bash
which kind kubectl
```

**`kind create cluster` hangs or fails**

Ensure Docker is running and your system has enough resources (≥ 2 CPU cores,
≥ 2 GB RAM free).  Try creating a cluster manually first:

```bash
kind create cluster --name smoke-test --wait 90s
kind delete cluster --name smoke-test
```

**`ModuleNotFoundError: No module named 'kubernetes_asyncio'`**

Install the optional extra:

```bash
uv sync --all-packages --extra kubernetes
# or
pip install "kubernetes_asyncio>=29.0"
```

**Leftover clusters after a crash**

List and delete any orphaned kind clusters manually:

```bash
kind get clusters
kind delete cluster --name <name>
```
