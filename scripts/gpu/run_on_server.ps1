# Run the GPU benchmark on a remote GPU server over SSH, and bring the report back.
# Connect the VPN first if the server needs it. You type your own SSH password
# (or use a key); nothing here stores it.
#
#   .\scripts\gpu\run_on_server.ps1 -Server you@gpu-server -List      # which GPUs are free
#   .\scripts\gpu\run_on_server.ps1 -Server you@gpu-server -Gpu 1     # start (keeps running after you disconnect)
#   .\scripts\gpu\run_on_server.ps1 -Server you@gpu-server -Status    # progress
#   .\scripts\gpu\run_on_server.ps1 -Server you@gpu-server -Fetch     # copy the report to reports\server\
param(
    [Parameter(Mandatory = $true)][string]$Server,
    [int]$Gpu = 0,
    [int]$Orders = 1000000,
    [int]$Cores = 8,
    [string]$DriverMemory = "16g",
    [switch]$List,
    [switch]$Status,
    [switch]$Fetch
)
$ErrorActionPreference = "Stop"
$repo = Resolve-Path (Join-Path $PSScriptRoot "..\..")

function Invoke-Remote([string]$script) {
    # Windows line endings would break bash on the server
    ssh $Server ($script -replace "`r", "")
}

if ($List) {
    Invoke-Remote "nvidia-smi --query-gpu=index,name,memory.used,memory.total,utilization.gpu,driver_version --format=csv; nproc; free -g | head -2"
    exit
}
if ($Status) {
    Invoke-Remote "tail -n 15 ETL/reports/gpu_setup.log 2>/dev/null || echo 'not started'"
    exit
}
if ($Fetch) {
    $dest = Join-Path $repo "reports\server"
    New-Item -ItemType Directory -Force -Path $dest | Out-Null
    scp "${Server}:ETL/reports/gpu_benchmark_synthetic.md" "${Server}:ETL/reports/gpu_benchmark_synthetic.json" "${Server}:ETL/reports/gpu_setup.log" "$dest\"
    Get-Content (Join-Path $dest "gpu_benchmark_synthetic.md")
    exit
}

$start = @"
set -e
if [ -d ETL/.git ]; then git -C ETL pull -q; else git clone -q https://github.com/manojhunasimarad/ETL.git; fi
cd ETL && mkdir -p reports
nohup env CUDA_VISIBLE_DEVICES=$Gpu ORDERS=$Orders ECOM_SPARK_MASTER='local[$Cores]' ECOM_SPARK_DRIVER_MEMORY=$DriverMemory ECOM_RAPIDS_CONCURRENT_TASKS=2 ECOM_RAPIDS_BATCH_BYTES=1g bash scripts/gpu/run_gpu_benchmark.sh > reports/server_run.out 2>&1 < /dev/null &
echo "Started on GPU $Gpu with $Orders orders. It keeps running if you disconnect."
"@
Invoke-Remote $start
Write-Host "Check progress:  .\scripts\gpu\run_on_server.ps1 -Server $Server -Status"
Write-Host "When it is done: .\scripts\gpu\run_on_server.ps1 -Server $Server -Fetch"
