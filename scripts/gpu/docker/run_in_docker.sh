#!/usr/bin/env bash
# Run any ecom_etl command in the image on one GPU, with the repo, data and jar mounted.
#   GPU=0 ./run_in_docker.sh gpu-benchmark --source synthetic --repeat 2
set -euo pipefail
source "$(dirname "$0")/common.sh"

[[ $# -gt 0 ]] || { echo "Usage: GPU=0 $0 <ecom_etl command> [args]"; exit 1; }
for d in "$PROJECT_DIR" "$DATA_DIR"; do [[ -d "$d" ]] || { echo "ERROR: missing $d"; exit 1; }; done
[[ -f "$RAPIDS_JAR" ]] || { echo "ERROR: missing $RAPIDS_JAR"; exit 1; }

pick_gpu_args
echo "Image $IMAGE | GPU $GPU | project $PROJECT_DIR | data $DATA_DIR"

# HOME=/tmp inside, so ECOM_RAPIDS_JAR must be explicit (the code otherwise looks in ~/.cache)
# and ECOM_ETL_DATA_DIR must point at the mounted data (the default is <repo>/data).
# --user runs as the host uid, which the image's /etc/passwd does not know. Java then sets
# user.home to "?" and Spark's Ivy setup fails ("basedir must be absolute: ?/.ivy2.5.2/local"),
# and Hadoop's user lookup can fail too. Mount the host's passwd/group read-only so the uid
# resolves, and point Java's home (and Spark's Ivy cache) at the writable /tmp.
docker run --rm \
    "${GPU_ARGS[@]}" "${FIX_ARGS[@]}" \
    --user "$(id -u):$(id -g)" \
    -v /etc/passwd:/etc/passwd:ro \
    -v /etc/group:/etc/group:ro \
    --shm-size=4g \
    -e HOME=/tmp \
    -e USER="$(id -un)" \
    -e SPARK_USER="$(id -un)" \
    -e JAVA_TOOL_OPTIONS="-Duser.home=/tmp -Divy.home=/tmp/.ivy2" \
    -e SPARK_LOCAL_DIRS=/tmp/spark \
    -e ECOM_RAPIDS_JAR="$RAPIDS_JAR" \
    -e ECOM_ETL_DATA_DIR="$DATA_DIR" \
    -e ECOM_SPARK_MASTER="${ECOM_SPARK_MASTER:-local[8]}" \
    -e ECOM_SPARK_DRIVER_MEMORY="${ECOM_SPARK_DRIVER_MEMORY:-16g}" \
    -e ECOM_RAPIDS_CONCURRENT_TASKS="${ECOM_RAPIDS_CONCURRENT_TASKS:-2}" \
    -e ECOM_RAPIDS_BATCH_BYTES="${ECOM_RAPIDS_BATCH_BYTES:-1g}" \
    -e ECOM_RAPIDS_EXPLAIN="${ECOM_RAPIDS_EXPLAIN:-NONE}" \
    -v "$PROJECT_DIR:$PROJECT_DIR" \
    -v "$DATA_DIR:$DATA_DIR" \
    -v "$RAPIDS_JAR:$RAPIDS_JAR:ro" \
    -w "$PROJECT_DIR" \
    "$IMAGE" "$@"
