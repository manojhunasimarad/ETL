# Run the GPU benchmark on a remote GPU server over SSH, and bring the report back.
# Connect the VPN first if the server needs it. You type your own SSH password
# (or use a key); nothing here stores it.
#
#   .\scripts\gpu\run_on_server.ps1 -Server you@gpu-server -List      # which GPUs are free
#   .\scripts\gpu\run_on_server.ps1 -Server you@gpu-server -Prepare   # install and generate data; no GPU needed
#   .\scripts\gpu\run_on_server.ps1 -Server you@gpu-server -Gpu 1 -Smoke  # 2-minute check of the GPU path
#   .\scripts\gpu\run_on_server.ps1 -Server you@gpu-server -Gpu 1     # the benchmark (keeps running after you disconnect)
#   .\scripts\gpu\run_on_server.ps1 -Server you@gpu-server -Status    # progress
#   .\scripts\gpu\run_on_server.ps1 -Server you@gpu-server -Fetch     # copy the report to reports\server\
#   .\scripts\gpu\run_on_server.ps1 -Server you@gpu-server -Diagnose  # why nvidia-smi fails (to reports\server\)
param(
    [Parameter(Mandatory = $true)][string]$Server,
    [int]$Gpu = 0,
    [int]$Orders = 1000000,
    [int]$Cores = 8,
    [string]$DriverMemory = "16g",
    [switch]$List,
    [switch]$Status,
    [switch]$Fetch,
    [switch]$Diagnose,
    [switch]$Prepare,
    [switch]$Smoke
)
$ErrorActionPreference = "Stop"
$repo = Resolve-Path (Join-Path $PSScriptRoot "..\..")

function Invoke-Remote([string]$script) {
    # Windows line endings would break bash on the server. The remote scripts use only
    # single quotes: Windows PowerShell 5.1 drops double quotes inside native arguments.
    ssh $Server ($script -replace "`r", "")
}

if ($List) {
    Invoke-Remote "nvidia-smi --query-gpu=index,name,memory.used,memory.total,utilization.gpu,driver_version --format=csv; nproc; free -g | head -2"
    exit
}
if ($Diagnose) {
    # Read-only checks: which NVIDIA kernel module is loaded, which is installed, which user-space libraries.
    $dest = Join-Path $repo "reports\server"
    New-Item -ItemType Directory -Force -Path $dest | Out-Null
    $checks = @'
echo '== kernel'; uname -r; cat /etc/os-release 2>/dev/null | head -2; uptime
echo '== loaded kernel module'; cat /proc/driver/nvidia/version 2>&1
echo '== module on disk for this kernel'; modinfo nvidia 2>&1 | grep -E '^(filename|version|vermagic)'
echo '== dkms'; dkms status 2>&1
echo '== user-space libraries'; ls -l /usr/lib/x86_64-linux-gnu/libnvidia-ml.so* /usr/lib64/libnvidia-ml.so* 2>&1
echo '== packages'; (dpkg -l 2>/dev/null | grep -E 'nvidia-(driver|dkms|kernel|utils|compute)|libnvidia-(compute|gl)' | awk '{print $2, $3}') || rpm -qa 2>/dev/null | grep -i nvidia
echo '== loaded modules'; lsmod | grep -i nvidia
echo '== nvidia-smi'; nvidia-smi 2>&1 | head -5
echo '== GPUs on the PCI bus'; lspci 2>/dev/null | grep -i nvidia
'@
    Invoke-Remote $checks | Tee-Object -FilePath (Join-Path $dest "diagnose.txt")
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

$mode = ""
$what = "the benchmark on GPU $Gpu with $Orders orders"
if ($Prepare) { $mode = "PREPARE_ONLY=1"; $what = "the preparation (install and $Orders orders of data)" }
if ($Smoke) { $mode = "SMOKE=1"; $what = "the smoke test on GPU $Gpu" }
$start = @"
set -e
if [ -d ETL/.git ]; then git -C ETL pull -q; else git clone -q https://github.com/manojhunasimarad/ETL.git; fi
cd ETL && mkdir -p reports
nohup env $mode CUDA_VISIBLE_DEVICES=$Gpu ORDERS=$(if ($Smoke) { 5000 } else { $Orders }) ECOM_SPARK_MASTER='local[$Cores]' ECOM_SPARK_DRIVER_MEMORY=$DriverMemory ECOM_RAPIDS_CONCURRENT_TASKS=2 ECOM_RAPIDS_BATCH_BYTES=1g bash scripts/gpu/run_gpu_benchmark.sh > reports/server_run.out 2>&1 < /dev/null &
echo 'Started $what. It keeps running if you disconnect.'
"@
Invoke-Remote $start
Write-Host "Check progress:  .\scripts\gpu\run_on_server.ps1 -Server $Server -Status"
Write-Host "When it is done: .\scripts\gpu\run_on_server.ps1 -Server $Server -Fetch"
