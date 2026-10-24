import asyncio

from examples.prefect_bitcount_demo.quantum_sampling import (
    generate_random_bitstrings,
    resolve_shots,
    sample_bitstrings,
)


def test_resolve_shots_reads_sampler_option_value():
    shots = resolve_shots(
        sampler_options={"params": {"shots": 321}},
        default_shots=100_000,
    )

    assert shots == 321


def test_generate_random_bitstrings_is_reproducible():
    first = generate_random_bitstrings(bitlen=4, shots=5, seed=24)
    second = generate_random_bitstrings(bitlen=4, shots=5, seed=24)

    assert first == second
    assert len(first) == 5
    assert all(len(bits) == 4 for bits in first)


def test_generate_random_bitstrings_accepts_zero_shots():
    assert generate_random_bitstrings(bitlen=4, shots=0, seed=24) == []


def test_sample_bitstrings_random_mode_does_not_require_prefect_qiskit():
    bitstrings = asyncio.run(
        sample_bitstrings(
            quantum_source="random",
            runtime_block_name="unused-runner",
            sampler_options={"params": {"shots": 10}},
            bitlen=4,
            default_shots=10,
            random_seed=42,
        )
    )
    assert len(bitstrings) == 10
    assert all(len(bits) == 4 for bits in bitstrings)
