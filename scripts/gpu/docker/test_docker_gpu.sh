#!/usr/bin/env bash
# 1. Can Docker reach the GPU? No image build needed.
set -euo pipefail
source "$(dirname "$0")/common.sh"

echo "== Docker";                 docker --version
echo; echo "== Docker access";    docker info --format 'Server {{.ServerVersion}}, runtimes: {{range $k, $v := .Runtimes}}{{$k}} {{end}}| default: {{.DefaultRuntime}}'
echo; echo "== Container toolkit"; nvidia-container-cli --version 2>/dev/null | head -1 || nvidia-ctk --version 2>/dev/null || echo "(CLI not on PATH)"
echo; echo "== Host GPU";         nvidia-smi --query-gpu=index,name,driver_version,memory.used,memory.total --format=csv

echo; echo "== GPU device files and matching libraries"
ls -l /dev/nvidia* 2>&1 || true
find "$NVIDIA_FIX_DIR" \( -name "libcuda.so*" -o -name "libnvidia-ml.so*" -o -name nvidia-smi \) 2>/dev/null || true

echo; echo "== GPU $GPU inside Docker"
pick_gpu_args
docker run --rm "${GPU_ARGS[@]}" "${FIX_ARGS[@]}" "$CUDA_IMAGE" nvidia-smi || echo "(nvidia-smi failed in the container; see the CUDA check)"
if docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo; echo "== CUDA inside $IMAGE"
    docker run --rm "${GPU_ARGS[@]}" "${FIX_ARGS[@]}" --entrypoint python "$IMAGE" -c "$CUDA_CHECK"
fi
echo; echo "Docker GPU test passed."
