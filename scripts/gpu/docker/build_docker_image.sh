#!/usr/bin/env bash
# 2. Build the image from the repo on the server and check Java, PySpark and the CLI.
set -euo pipefail
source "$(dirname "$0")/common.sh"
HERE="$(cd "$(dirname "$0")" && pwd)"

echo "== Building $IMAGE (context $PROJECT_DIR)"
docker build --pull -f "$HERE/Dockerfile" -t "$IMAGE" "$PROJECT_DIR"

echo; echo "== Java";    docker run --rm --runtime runc --entrypoint java "$IMAGE" -version
echo; echo "== PySpark"; docker run --rm --runtime runc --entrypoint python "$IMAGE" -c 'import pyspark; print("PySpark", pyspark.__version__)'
echo; echo "== CLI";     docker run --rm --runtime runc "$IMAGE" --help | head -3
echo; echo "Image build completed: $(docker image inspect -f '{{.Size}}' "$IMAGE" | awk '{printf "%.1f GB", $1/1e9}')"
