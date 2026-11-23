# What the ETL project needs to use the GPU: writes reports\gpu_check.txt.
# Run from the repository folder:
#   powershell -ExecutionPolicy Bypass -File scripts\gpu\check_gpu.ps1
$ErrorActionPreference = "Continue"
$env:WSL_UTF8 = "1"
$repo = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$out = Join-Path $repo "reports\gpu_check.txt"
New-Item -ItemType Directory -Force -Path (Split-Path $out) | Out-Null

& {
    "== Windows"
    Get-CimInstance Win32_OperatingSystem | Select-Object Caption, Version | Format-List | Out-String
    "RAM total GB: " + [math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory / 1GB, 1)
    "RAM free GB:  " + [math]::Round((Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory / 1MB, 1)
    Get-CimInstance Win32_Processor | Select-Object Name, NumberOfCores, NumberOfLogicalProcessors | Format-List | Out-String
    "== GPU (Windows)"
    nvidia-smi --query-gpu=name,compute_cap,memory.total,driver_version --format=csv 2>&1 | Out-String
    nvidia-smi 2>&1 | Out-String
    "== WSL"
    wsl --version 2>&1 | Out-String
    wsl -l -v 2>&1 | Out-String
    "== Inside the default WSL distribution"
    wsl -e bash -lc 'echo distro: $(. /etc/os-release; echo $PRETTY_NAME); nvidia-smi --query-gpu=name,compute_cap,memory.total,driver_version --format=csv 2>&1; echo cores: $(nproc); free -g | head -2; df -h ~ | tail -1; java -version 2>&1 | head -1; python3 --version; which pip3 docker 2>&1' 2>&1 | Out-String
    "== Docker (Windows)"
    docker version --format "{{.Server.Version}}" 2>&1 | Out-String
} *>&1 | Out-File -Encoding utf8 $out

Write-Host "Wrote $out"
