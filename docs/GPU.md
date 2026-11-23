# Running the batch on a GPU

The Spark batch (bronze, silver, gold) can run on an NVIDIA GPU with the
**RAPIDS Accelerator for Apache Spark**, NVIDIA's Spark plugin. The DataFrame
code does not change: with `ECOM_GPU=1`, Spark starts with the plugin, which
replaces plan operators (CSV and Parquet scans and writes, filters,
projections, joins, aggregations, sorts, windows) with GPU versions and leaves
the rest on the CPU.

## Where a GPU helps here, and where it does not

| Part | GPU? | Why |
|---|---|---|
| Spark bronze, silver, gold | **yes**, at scale | columnar work on many rows; needs enough data to outweigh copying it to the GPU |
| Profiling (pandas) | a little | `profile --gpu` runs pandas through `cudf.pandas` |
| Postgres load and queries | no | database and network bound |
| Kafka stream | no | small micro-batches, latency bound |
| Airflow, dashboard | no | scheduling and display |
| Synthetic generator | no | plain Python |

The honest expectation on a laptop: a GPU with 4 GB of memory, sharing the
machine's RAM with Windows, may beat 12 CPU threads only modestly, or not at
all at small sizes. The benchmark measures it rather than assuming it, and
checks that both engines produce the same data.

## Requirements

- Linux; on Windows, **WSL2** (Ubuntu). The plugin does not run on Windows itself.
- An NVIDIA GPU of the Volta generation or newer (compute capability 7.0+):
  GTX 16xx, RTX 20xx and later. A GTX 10xx (Pascal) is too old.
- The Windows NVIDIA driver R525 or newer. Inside WSL, `nvidia-smi` works
  through the Windows driver; never install a Linux driver in WSL.
- A Spark version the plugin supports: the GPU environment pins PySpark 4.1.3.
- RAM: about 4 GB for WSL. WSL takes half of the machine's memory by default;
  with 8 GB in total, give it more in `C:\Users\<you>\.wslconfig`:

  ```ini
  [wsl2]
  memory=5GB
  swap=8GB
  ```

  then `wsl --shutdown` in PowerShell, and close Chrome while benchmarking.

To check a machine: `powershell -ExecutionPolicy Bypass -File scripts\gpu\check_gpu.ps1`
writes `reports\gpu_check.txt`.

## Run the benchmark

In WSL, from the repository (on Windows `D:\sem3_4\ETL` is `/mnt/d/sem3_4/ETL`):

```bash
cd /mnt/d/sem3_4/ETL
bash scripts/gpu/run_gpu_benchmark.sh               # 200,000 orders, 2 timed passes per engine
ORDERS=500000 bash scripts/gpu/run_gpu_benchmark.sh # bigger, if memory allows
```

The first run installs Java 17, a virtual environment in `~/.venvs/ecom-gpu`
with PySpark 4.1.3, and the plugin jar (checksum verified), about 1.2 GB.
Data goes to `~/ecom-data` on the Linux disk, because Spark on `/mnt/d` is
many times slower. The report is `reports/gpu_benchmark_synthetic.md`:

- median seconds per stage for each engine and the speed-up;
- whether both engines produced the same gold row counts, quarantine count
  and revenue totals;
- how many plan operators ran on the GPU, and which stayed on the CPU
  (`ECOM_RAPIDS_EXPLAIN=NOT_ON_GPU` prints why, operator by operator).

Each engine runs in its own process, because the plugin is loaded when the JVM
starts. Both use the tuned settings of step 7.

## On a shared GPU server (over SSH)

A server GPU such as a V100 (16 or 32 GB) has room for much larger runs. The
same script works there without sudo: it downloads a Java runtime and builds
its own Python environment in your home folder. It needs Linux, the NVIDIA
driver 525 or newer, Python 3.10+ and internet access for the downloads.

```bash
ssh you@gpu-server                       # after the college VPN, if needed
tmux new -s etl                          # keeps running if the connection drops
git clone https://github.com/manojhunasimarad/ETL.git && cd ETL   # later: git pull
nvidia-smi                               # pick a GPU nobody is using
export CUDA_VISIBLE_DEVICES=0            # that GPU's index
ORDERS=1000000 ECOM_SPARK_MASTER='local[8]' ECOM_SPARK_DRIVER_MEMORY=16g \
ECOM_RAPIDS_CONCURRENT_TASKS=2 ECOM_RAPIDS_BATCH_BYTES=1g \
bash scripts/gpu/run_gpu_benchmark.sh
```

`local[8]` limits Spark to 8 cores so others can use the machine; both
engines get the same cores, so the comparison stays fair. Detach from tmux
with Ctrl+B then D; `tmux attach -t etl` returns. Copy the report to your
laptop from PowerShell:

```powershell
scp you@gpu-server:ETL/reports/gpu_benchmark_synthetic.md D:\sem3_4\ETL\reports\
```

Or let one PowerShell script do the SSH part from your laptop (it starts the
run with `nohup`, so it keeps going after you disconnect):

```powershell
.\scripts\gpu\run_on_server.ps1 -Server you@gpu-server -List     # free GPUs
.\scripts\gpu\run_on_server.ps1 -Server you@gpu-server -Gpu 1    # start
.\scripts\gpu\run_on_server.ps1 -Server you@gpu-server -Status   # progress
.\scripts\gpu\run_on_server.ps1 -Server you@gpu-server -Fetch    # report into reports\server\
```

If the server's Python is older than 3.10, point the script at a newer one:
`PYTHON=$HOME/miniconda3/envs/py312/bin/python bash scripts/gpu/run_gpu_benchmark.sh`.

## Use it for anything else

```bash
. ~/.venvs/ecom-gpu/bin/activate
export ECOM_ETL_DATA_DIR=~/ecom-data ECOM_GPU=1
python -m ecom_etl run --source synthetic --no-load
```

| Variable | Default | Meaning |
|---|---|---|
| `ECOM_GPU` | off | `1` loads the RAPIDS Accelerator |
| `ECOM_RAPIDS_JAR` | `~/.cache/ecom-etl/rapids-4-spark_2.13-26.10.0.jar` if present, else Maven | plugin jar |
| `ECOM_RAPIDS_CONCURRENT_TASKS` | `1` | tasks sharing the GPU at once (2 with 8 GB of GPU memory or more) |
| `ECOM_RAPIDS_BATCH_BYTES` | `256m` | GPU batch size, kept small for a 4 GB GPU |
| `ECOM_RAPIDS_PINNED_POOL` | unset | pinned host memory; WSL2 limits it |
| `ECOM_RAPIDS_EXPLAIN` | `NONE` | `NOT_ON_GPU` prints every CPU fallback |

## Profiling with cudf.pandas

`INSTALL_CUDF=1 bash scripts/gpu/run_gpu_benchmark.sh` also installs cuDF.
Then `python -m ecom_etl profile --gpu` runs the profiler's pandas code on the
GPU; compare its time with `profile` without `--gpu`.

## Learn more

- NVIDIA, *RAPIDS Accelerator for Apache Spark* documentation: getting started, supported operators, tuning guide.
- *Learning Spark, 2nd Ed.*, ch. 7, for what the plan operators are.
