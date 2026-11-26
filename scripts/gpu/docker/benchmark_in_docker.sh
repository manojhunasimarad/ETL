#!/usr/bin/env bash
# 3+4. Check GPU and Java in the image, make the data if needed, then benchmark.
#   GPU=0 ORDERS=1000000 REPEAT=2 ./benchmark_in_docker.sh      (SMOKE=1: quick GPU-path check)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/common.sh"
ORDERS="${ORDERS:-1000000}"; REPEAT="${REPEAT:-2}"
if [[ "${SMOKE:-0}" = "1" ]]; then
    ORDERS=5000; DATA_DIR="$DATA_DIR/smoke"; export ECOM_RAPIDS_EXPLAIN=NOT_ON_GPU
fi
mkdir -p "$DATA_DIR"; export DATA_DIR
docker image inspect "$IMAGE" >/dev/null 2>&1 || { echo "No image $IMAGE yet: run -Docker build and wait for it to finish."; exit 1; }

echo "== GPU and Java inside $IMAGE ($(date '+%H:%M:%S'))"
pick_gpu_args
docker run --rm "${GPU_ARGS[@]}" "${FIX_ARGS[@]}" --entrypoint bash "$IMAGE" \
    -c 'nvidia-smi --query-gpu=name,driver_version,memory.free --format=csv || echo "(nvidia-smi unavailable)"; java -version 2>&1 | head -1'
docker run --rm "${GPU_ARGS[@]}" "${FIX_ARGS[@]}" --entrypoint python "$IMAGE" -c "$CUDA_CHECK"

manifest="$DATA_DIR/raw/synthetic/_manifest.json"
have=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['config']['orders'])" "$manifest" 2>/dev/null || echo 0)
echo; echo "== Data: $ORDERS orders in $DATA_DIR (have $have)"
if [[ "$have" != "$ORDERS" ]]; then
    "$HERE/run_in_docker.sh" generate --orders "$ORDERS"
fi

echo; echo "== Benchmark ($(date '+%H:%M:%S'))"
if [[ "${SMOKE:-0}" = "1" ]]; then
    "$HERE/run_in_docker.sh" gpu-benchmark --source synthetic --engines gpu --repeat 1 --no-warmup
else
    "$HERE/run_in_docker.sh" gpu-benchmark --source synthetic --repeat "$REPEAT"
fi
echo; echo "== Done ($(date '+%H:%M:%S')): $PROJECT_DIR/reports/gpu_benchmark_synthetic.md"
