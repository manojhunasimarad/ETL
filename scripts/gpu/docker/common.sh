# Shared settings for the Docker GPU scripts (sourced, not run).
PROJECT_DIR="${PROJECT_DIR:-/home/manoj/ETL}"
DATA_DIR="${DATA_DIR:-/home/manoj/ecom-data}"
RAPIDS_JAR="${RAPIDS_JAR:-/home/manoj/.cache/ecom-etl/rapids-4-spark_2.13-26.10.0.jar}"
NVIDIA_FIX_DIR="${NVIDIA_FIX_DIR:-$HOME/nvidia-580.173}"
CUDA_IMAGE="nvidia/cuda:12.9.1-base-ubuntu24.04"
IMAGE="${IMAGE:-ecom-etl-gpu:java17}"
GPU="${GPU:-0}"

# Scripts do not read ~/.bashrc: load the user-level driver-library fix for host commands.
if [[ -f "$HOME/nvidia-fix.sh" ]]; then
    # shellcheck disable=SC1091
    source "$HOME/nvidia-fix.sh"
fi

# How a container gets the GPU. Sets the arrays GPU_ARGS and FIX_ARGS.
#  1. toolkit: --gpus, the normal way.
#  2. direct:  when the toolkit fails (its host-side hook loads the host's mismatched
#     libnvidia-ml), pass the /dev/nvidia* device files ourselves and mount the matching
#     user-space libraries from ~/nvidia-580.173. This needs no root and no toolkit.
# GPU_MODE=toolkit|direct forces one.
pick_gpu_args() {
    GPU_ARGS=(); FIX_ARGS=()
    local mode="${GPU_MODE:-auto}"
    if [[ "$mode" != "direct" ]]; then
        if docker run --rm --gpus "\"device=$GPU\"" "$CUDA_IMAGE" true >/dev/null 2>&1; then
            GPU_ARGS=(--gpus "\"device=$GPU\"" -e "NVIDIA_VISIBLE_DEVICES=$GPU"); echo "GPU access: toolkit (--gpus device=$GPU)"; return 0
        fi
        [[ "$mode" == "toolkit" ]] && { echo "GPU access: toolkit failed (GPU_MODE=toolkit)"; return 1; }
    fi

    local lib dev
    lib=$(find "$NVIDIA_FIX_DIR" -name 'libnvidia-ml.so.1' 2>/dev/null | head -1 || true)
    [[ -n "$lib" ]] || { echo "GPU access: toolkit failed and no libnvidia-ml.so.1 under $NVIDIA_FIX_DIR"; return 1; }
    lib=$(dirname "$lib")
    [[ -e "$lib/libcuda.so.1" ]] || echo "WARNING: no libcuda.so.1 in $lib; nvidia-smi may work but Spark on the GPU will not"

    for dev in /dev/nvidiactl /dev/nvidia-uvm /dev/nvidia-uvm-tools /dev/nvidia-modeset /dev/nvidia[0-9]*; do
        [[ -e "$dev" ]] && GPU_ARGS+=(--device "$dev")
    done
    [[ -e /dev/nvidia-uvm ]] || echo "WARNING: /dev/nvidia-uvm is missing on the host; CUDA needs it (ask the admin to load nvidia-uvm)"
    # runc + NVIDIA_VISIBLE_DEVICES=void keep the failing NVIDIA hook out entirely.
    GPU_ARGS+=(--runtime runc -e NVIDIA_VISIBLE_DEVICES=void -e "CUDA_VISIBLE_DEVICES=$GPU")
    FIX_ARGS=(-v "$NVIDIA_FIX_DIR:$NVIDIA_FIX_DIR:ro" -e "LD_LIBRARY_PATH=$lib")
    local smi
    smi=$(find "$NVIDIA_FIX_DIR" -type f -name nvidia-smi 2>/dev/null | head -1 || true)
    # The fix folder has only libraries: reuse the host's nvidia-smi, which loads them via LD_LIBRARY_PATH.
    # The nvidia-smi on PATH may be a wrapper script (calls /usr/bin/nvidia-smi); mount the real binary.
    if [[ -z "$smi" ]]; then
        for cand in /usr/bin/nvidia-smi "$(command -v nvidia-smi || true)"; do
            cand=$(readlink -f "$cand" 2>/dev/null || true)
            if [[ -f "$cand" ]] && head -c 4 "$cand" | grep -q ELF; then smi="$cand"; break; fi
        done
    fi
    if [[ -n "$smi" ]]; then
        FIX_ARGS+=(-v "$smi:/usr/bin/nvidia-smi:ro")
    else
        echo "Note: no nvidia-smi binary found to mount; the CUDA check below is what counts"
    fi
    echo "GPU access: direct (toolkit hook fails on the host's driver mismatch); libraries from $lib"
}

# Real CUDA check (what Spark needs): cuInit + device count through libcuda, with only Python.
CUDA_CHECK='import ctypes; c = ctypes.CDLL("libcuda.so.1"); n = ctypes.c_int(); r = c.cuInit(0); c.cuDeviceGetCount(ctypes.byref(n)); v = ctypes.c_int(); c.cuDriverGetVersion(ctypes.byref(v)); print(f"CUDA: cuInit={r} (0 = OK), devices={n.value}, driver API {v.value // 1000}.{v.value % 1000 // 10}"); raise SystemExit(r)'
