#!/usr/bin/env bash
# CPU against GPU (NVIDIA RAPIDS Accelerator) for the Spark batch, on this machine.
#
# Run inside WSL2 Ubuntu, from the repository:
#   cd /mnt/d/sem3_4/ETL && bash scripts/gpu/run_gpu_benchmark.sh
#
# The first run installs, without sudo, a Java 17 runtime (if missing), a Python
# environment with PySpark 4.1.3 and the plugin jar: about 1.2 GB of downloads.
# Later runs reuse them. Everything printed is also written to
# reports/gpu_setup.log, and the result to reports/gpu_benchmark_synthetic.md.
#
# Modes: PREPARE_ONLY=1 installs everything and generates the data, without
# needing a working GPU (useful while a server waits for a reboot); SMOKE=1 runs
# a 5,000-order check of the GPU path first, printing every operator left on the CPU.
#
# Settings (environment variables): ORDERS (default 200000), REPEAT (2),
# INSTALL_CUDF=1 to also install cudf.pandas for `profile --gpu`, PYTHON (python3).
# On a shared GPU server, also: CUDA_VISIBLE_DEVICES=<a free GPU>,
# ECOM_SPARK_MASTER=local[8] (leave cores for others), ECOM_SPARK_DRIVER_MEMORY=16g,
# ECOM_RAPIDS_CONCURRENT_TASKS=2 and ECOM_RAPIDS_BATCH_BYTES=1g on a 16-32 GB GPU.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
VENV="${ECOM_GPU_VENV:-$HOME/.venvs/ecom-gpu}"
PY="${PYTHON:-python3}"   # on an old server: PYTHON=/path/to/python3.12 (or a conda env's python)
# Data on the Linux disk: Spark on /mnt/d (the Windows drive) is many times slower.
export ECOM_ETL_DATA_DIR="${ECOM_ETL_DATA_DIR:-$HOME/ecom-data}"
export ECOM_SPARK_DRIVER_MEMORY="${ECOM_SPARK_DRIVER_MEMORY:-2g}"
PREPARE_ONLY="${PREPARE_ONLY:-0}"
SMOKE="${SMOKE:-0}"
if [ "$SMOKE" = "1" ]; then
    ORDERS="${ORDERS:-5000}"; REPEAT=1
    export ECOM_RAPIDS_EXPLAIN=NOT_ON_GPU
    export ECOM_ETL_DATA_DIR="$ECOM_ETL_DATA_DIR/smoke"   # leave the big data set alone
else
    ORDERS="${ORDERS:-200000}"; REPEAT="${REPEAT:-2}"
fi
mkdir -p "$REPO/reports"
exec > >(tee "$REPO/reports/gpu_setup.log") 2>&1

step() { echo; echo "== $* ($(date '+%H:%M:%S'))"; }

step "GPU"
if ! nvidia-smi --query-gpu=name,compute_cap,memory.total,memory.used,driver_version --format=csv; then
    if [ "$PREPARE_ONLY" = "1" ]; then
        echo "No working GPU right now; preparing everything else (PREPARE_ONLY=1)."
    else
        echo "nvidia-smi fails. In WSL: update the NVIDIA driver on Windows (never install one inside WSL)."
        echo "On a server: a 'Driver/library version mismatch' needs a reboot by the administrator."
        echo "Meanwhile PREPARE_ONLY=1 installs everything and generates the data."
        exit 1
    fi
fi

if [ "$PREPARE_ONLY" != "1" ]; then
driver=$(nvidia-smi --query-gpu=driver_version --format=csv,noheader | head -1 | cut -d. -f1)
if [ "${driver:-0}" -lt 525 ]; then
    echo "The NVIDIA driver is ${driver}; the RAPIDS Accelerator needs 525 or newer (CUDA 12)."
    echo "On a shared server, ask the administrator; the CPU run would still work (--engines cpu)."
    exit 1
fi
gpu_free=$(nvidia-smi --query-gpu=memory.free --format=csv,noheader,nounits | head -1 | tr -d ' ')
if [ "${gpu_free:-0}" -lt 2000 ]; then
    echo "WARNING: only ${gpu_free} MiB of GPU memory is free; Windows apps (Chrome, Claude, video) use it too."
    echo "Close what you can; the plugin spills to host memory when the GPU fills, which is slower."
fi
fi

step "Memory"
free -h
mem_gb=$(awk '/MemTotal/ {printf "%d", $2/1024/1024}' /proc/meminfo)
if [ "$mem_gb" -lt 4 ]; then
    echo "WARNING: WSL has about ${mem_gb} GB of RAM; Spark wants about 4 GB."
    echo "Give WSL more (docs/GPU.md, .wslconfig) and close Chrome while this runs."
fi

step "Java 17"
# No sudo needed: if Java is missing, a Temurin 17 runtime goes into ~/.local/jdk-17.
JDK="$HOME/.local/jdk-17"
if ! command -v java >/dev/null; then
    if [ ! -x "$JDK/bin/java" ]; then
        mkdir -p "$JDK"
        curl -fL --retry 3 "https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jre/hotspot/normal/eclipse" \
            | tar -xz -C "$JDK" --strip-components=1
    fi
    export JAVA_HOME="$JDK" PATH="$JDK/bin:$PATH"
fi
java -version 2>&1 | head -1

step "Python environment in $VENV"
if ! "$PY" -c "import sys; sys.exit(sys.version_info < (3, 10))"; then
    echo "$PY is $("$PY" --version 2>&1); PySpark 4 needs Python 3.10 or newer."
    echo "Point PYTHON at a newer one, for example a conda env: conda create -n py312 python=3.12"
    exit 1
fi
if [ ! -x "$VENV/bin/python" ]; then
    if "$PY" -c "import ensurepip" 2>/dev/null; then
        "$PY" -m venv "$VENV"
    else  # Ubuntu without python3-venv: create the venv bare, then bootstrap pip (no sudo)
        "$PY" -m venv --without-pip "$VENV"
        curl -fsSL https://bootstrap.pypa.io/get-pip.py | "$VENV/bin/python" - -q
    fi
    if [ -x "$JDK/bin/java" ]; then  # make `activate` also put this Java on the PATH
        printf '\nexport JAVA_HOME="%s"\nexport PATH="$JAVA_HOME/bin:$PATH"\n' "$JDK" >> "$VENV/bin/activate"
    fi
fi
# shellcheck disable=SC1091
. "$VENV/bin/activate"
pip install -q --upgrade pip
pip install -q -e "$REPO[spark]" "pyspark==4.1.3"
if [ "${INSTALL_CUDF:-0}" = "1" ]; then
    pip install -q --extra-index-url=https://pypi.nvidia.com "cudf-cu12"
fi
python -c "import pyspark; print('pyspark', pyspark.__version__)"

step "RAPIDS Accelerator jar"
JAR=$(python -c "from ecom_etl.spark.gpu import default_jar_path; print(default_jar_path())")
URL=$(python -c "from ecom_etl.spark.gpu import rapids_jar_url; print(rapids_jar_url())")
if [ ! -s "$JAR" ]; then
    mkdir -p "$(dirname "$JAR")"
    curl -fL --retry 3 -o "$JAR.part" "$URL"
    mv "$JAR.part" "$JAR"
fi
expected=$(curl -fsSL "$URL.sha1" | cut -c1-40)
actual=$(sha1sum "$JAR" | cut -c1-40)
if [ "$expected" != "$actual" ]; then
    echo "Checksum mismatch for $JAR (expected $expected, got $actual); delete it and run again."
    exit 1
fi
echo "jar verified: $JAR ($(du -h "$JAR" | cut -f1))"

step "Data: $ORDERS synthetic orders in $ECOM_ETL_DATA_DIR"
have=$(python -c "import json; from ecom_etl import config; m = config.source_dir('synthetic') / '_manifest.json'; print(json.loads(m.read_text())['config']['orders'] if m.exists() else 0)")
if [ "$have" = "$ORDERS" ]; then
    echo "already generated, reusing it"
else
    python -m ecom_etl generate --orders "$ORDERS"
fi

if [ "$PREPARE_ONLY" = "1" ]; then
    step "Prepared"
    echo "Everything is installed and the data is ready. When the GPU works, run this script again without PREPARE_ONLY."
    exit 0
fi

if [ "$SMOKE" = "1" ]; then
    step "Smoke test: the GPU path on $ORDERS orders (the plugin lists every operator left on the CPU)"
    python -m ecom_etl gpu-benchmark --source synthetic --engines gpu --repeat 1 --no-warmup
    step "Done"
    echo "If that worked, run the full benchmark: bash scripts/gpu/run_gpu_benchmark.sh"
    exit 0
fi

step "Benchmark: CPU, then GPU"
python -m ecom_etl gpu-benchmark --source synthetic --repeat "$REPEAT"

step "Done"
echo "Report: $REPO/reports/gpu_benchmark_synthetic.md"
