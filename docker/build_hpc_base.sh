#!/usr/bin/env bash
set -euo pipefail

IMAGE_TAG="${1:-qcsc-hpc-base:latest}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "Building generic HPC base image: ${IMAGE_TAG}"
docker build -t "${IMAGE_TAG}" -f "${SCRIPT_DIR}/Dockerfile.hpc-base" "${ROOT_DIR}"
echo "Successfully built ${IMAGE_TAG}"
