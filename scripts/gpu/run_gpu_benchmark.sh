#!/usr/bin/env bash
# CPU against GPU (NVIDIA RAPIDS Accelerator) for the Spark batch, on this machine.
#
# Run inside WSL2 Ubuntu, from the repository:
#   cd /mnt/d/sem3_4/ETL && bash scripts/gpu/run_gpu_benchmark.sh
#
# The first run installs Java 17 (asks for your sudo password), a Python
# environment with PySpark 4.1.3 and the plugin jar: about 1.2 GB of downloads.
# Later runs reuse them. Everything printed is also written to
# reports/gpu_setup.log, and the result to reports/gpu_benchmark_synthetic.md.
#
# Settings (environment variables): ORDERS (default 200000), REPEAT (2),
# INSTALL_CUDF=1 to also install cudf.pandas for `profile --gpu`.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
VENV="${ECOM_GPU_VENV:-$HOME/.venvs/ecom-gpu}"
# Data on the Linux disk: Spark on /mnt/d (the Windows drive) is many times slower.
export ECOM_ETL_DATA_DIR="${ECOM_ETL_DATA_DIR:-$HOME/ecom-data}"
export ECOM_SPARK_DRIVER_MEMORY="${ECOM_SPARK_DRIVER_MEMORY:-2g}"
ORDERS="${ORDERS:-200000}"
REPEAT="${REPEAT:-2}"
mkdir -p "$REPO/reports"
exec > >(tee "$REPO/reports/gpu_setup.log") 2>&1

step() { echo; echo "== $* ($(date '+%H:%M:%S'))"; }

step "GPU"
if ! nvidia-smi --query-gpu=name,compute_cap,memory.total,memory.used,driver_version --format=csv; then
    echo "nvidia-smi does not work inside WSL. Update the NVIDIA driver on Windows;"
    echo "do not install a Linux NVIDIA driver inside WSL."
    exit 1
fi

step "Memory"
free -h
mem_gb=$(awk '/MemTotal/ {printf "%d", $2/1024/1024}' /proc/meminfo)
if [ "$mem_gb" -lt 4 ]; then
    echo "WARNING: WSL has about ${mem_gb} GB of RAM; Spark wants about 4 GB."
    echo "Give WSL more (docs/GPU.md, .wslconfig) and close Chrome while this runs."
fi

step "Java 17 and Python venv support"
if ! command -v java >/dev/null || ! python3 -c "import ensurepip" 2>/dev/null; then
    sudo apt-get update -y
    sudo apt-get install -y openjdk-17-jre-headless python3-venv curl
fi
java -version 2>&1 | head -1

step "Python environment in $VENV"
[ -d "$VENV" ] || python3 -m venv "$VENV"
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
python -m ecom_etl generate --orders "$ORDERS"

step "Benchmark: CPU, then GPU"
python -m ecom_etl gpu-benchmark --source synthetic --repeat "$REPEAT"

step "Done"
echo "Report: $REPO/reports/gpu_benchmark_synthetic.md"
