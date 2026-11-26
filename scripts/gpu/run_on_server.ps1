
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
    [switch]$Smoke,
    # Docker on the server: test (GPU in a container), build (image), smoke, run (benchmark), log
    [ValidateSet("test", "build", "smoke", "run", "log")][string]$Docker,
    # With -Docker log: show only this step's log (default: all of them), and how many lines
    [ValidateSet("test", "build", "smoke", "run")][string]$Log,
    [int]$Lines = 60,
    [int]$Repeat = 2
)

$ErrorActionPreference = "Stop"

$repo = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$plinkPath = (Get-Command plink.exe -ErrorAction Stop).Source
$pscpPath = (Get-Command pscp.exe -ErrorAction Stop).Source

$headnode = "10.100.36.74"
$v100 = "10.100.36.75"
$port = 2222
$dpapiPath = Join-Path $env:USERPROFILE ".ssh\linux-password.dpapi"

$tunnel = $null
$pwfile = Join-Path $env:TEMP ("ssh-pw-" + [guid]::NewGuid().ToString("N") + ".txt")
$tunnelOut = "$pwfile.out"
$tunnelErr = "$pwfile.err"

function New-TemporaryPasswordFile {
    if (!(Test-Path $dpapiPath)) {
        throw "Encrypted password file not found: $dpapiPath"
    }

    $encrypted = Get-Content $dpapiPath -Raw
    $secure = ConvertTo-SecureString $encrypted
    $bstr = [IntPtr]::Zero

    try {
        $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
        $plain = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)

        [IO.File]::WriteAllText(
            $pwfile,
            $plain,
            [Text.UTF8Encoding]::new($false)
        )

        $identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
        & icacls.exe $pwfile /inheritance:r /grant:r "${identity}:(R,W)" | Out-Null

        if ($LASTEXITCODE -ne 0) {
            throw "Could not restrict permissions on temporary password file."
        }
    }
    finally {
        if ($bstr -ne [IntPtr]::Zero) {
            [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
        }

        if ($secure) {
            $secure.Dispose()
        }

        Remove-Variable plain, encrypted, secure -ErrorAction SilentlyContinue
    }
}

function Start-SshTunnel {
    $arguments = '-ssh -batch -no-antispoof -l manoj -pwfile "{0}" -L 127.0.0.1:{1}:{2}:22 -N {3}' -f `
        $pwfile, $port, $v100, $headnode

    $script:tunnel = Start-Process `
        -FilePath $plinkPath `
        -ArgumentList $arguments `
        -WindowStyle Hidden `
        -RedirectStandardOutput $tunnelOut `
        -RedirectStandardError $tunnelErr `
        -PassThru

    for ($i = 0; $i -lt 60; $i++) {
        if ($tunnel.HasExited) {
            $details = ""
            if (Test-Path $tunnelErr) {
                $details = Get-Content $tunnelErr -Raw
            }
            throw "SSH tunnel stopped unexpectedly. $details"
        }

        $client = New-Object System.Net.Sockets.TcpClient

        try {
            $async = $client.BeginConnect("127.0.0.1", $port, $null, $null)

            if ($async.AsyncWaitHandle.WaitOne(300)) {
                $client.EndConnect($async)
                $async.AsyncWaitHandle.Close()
                return
            }

            $async.AsyncWaitHandle.Close()
        }
        catch {
        }
        finally {
            $client.Close()
        }

        Start-Sleep -Milliseconds 250
    }

    throw "Timed out waiting for the SSH tunnel on port $port."
}

function Invoke-Remote([string]$remoteScript) {
    $commandFile = Join-Path $env:TEMP ("ssh-command-" + [guid]::NewGuid().ToString("N") + ".txt")

    try {
        [IO.File]::WriteAllText(
            $commandFile,
            ($remoteScript -replace "`r", ""),
            [Text.UTF8Encoding]::new($false)
        )

        & $plinkPath -ssh -batch -no-antispoof -T `
            -P $port -l manoj -pwfile $pwfile `
            -m $commandFile 127.0.0.1

        if ($LASTEXITCODE -ne 0) {
            throw "Remote command failed with exit code $LASTEXITCODE."
        }
    }
    finally {
        Remove-Item $commandFile -Force -ErrorAction SilentlyContinue
    }
}

try {
    New-TemporaryPasswordFile
    Start-SshTunnel

    Write-Host "Connected to V100 through headnode."

    # List GPU information
    if ($List) {
        Invoke-Remote @'
nvidia-smi --query-gpu=index,name,memory.used,memory.total,utilization.gpu,driver_version --format=csv
echo
nproc
free -g | head -2
'@
        return
    }

    # NVIDIA driver diagnostics
    if ($Diagnose) {
        $dest = Join-Path $repo "reports\server"
        New-Item -ItemType Directory -Force -Path $dest | Out-Null

        $checks = @'
echo '== kernel'
uname -r
cat /etc/os-release 2>/dev/null | head -2
uptime

echo '== loaded kernel module'
cat /proc/driver/nvidia/version 2>&1

echo '== module on disk for this kernel'
modinfo nvidia 2>&1 | grep -E '^(filename|version|vermagic)'

echo '== dkms'
dkms status 2>&1

echo '== user-space libraries'
ls -l /usr/lib/x86_64-linux-gnu/libnvidia-ml.so* /usr/lib64/libnvidia-ml.so* 2>&1

echo '== packages'
(dpkg -l 2>/dev/null | grep -E 'nvidia-(driver|dkms|kernel|utils|compute)|libnvidia-(compute|gl)' | awk '{print $2, $3}') || rpm -qa 2>/dev/null | grep -i nvidia

echo '== loaded modules'
lsmod | grep -i nvidia

echo '== nvidia-smi'
nvidia-smi 2>&1 | head -5

echo '== GPUs on the PCI bus'
lspci 2>/dev/null | grep -i nvidia
'@

        Invoke-Remote $checks | Tee-Object -FilePath (Join-Path $dest "diagnose.txt")
        return
    }

    # Benchmark status and detailed logs
    if ($Status) {
        $checks = @'
echo '== Benchmark output (last 100 lines)'
tail -n 100 ETL/reports/server_run.out 2>&1

echo
echo '== GPU setup log (last 60 lines)'
tail -n 60 ETL/reports/gpu_setup.log 2>&1

echo
echo '== Running benchmark processes'
pgrep -af 'ecom_etl|spark-submit|run_gpu_benchmark' || true

echo
echo '== Java installations'
ls -lah /usr/lib/jvm/ 2>&1
echo
find /usr/lib/jvm /opt /home/manoj -type f -path '*/bin/java' 2>/dev/null | head -40

echo
echo '== Installed Java packages'
dpkg-query -W -f='${binary:Package} ${Version}\n' 'openjdk-*' 2>/dev/null || true

echo
echo '== Docker version'
docker --version 2>&1 || true

echo
echo '== Docker daemon'
docker info 2>&1 | head -25

echo
echo '== Docker permission'
id
groups

echo
echo '== NVIDIA container toolkit'
nvidia-container-cli --version 2>&1 || true
nvidia-ctk --version 2>&1 || true

echo
echo '== Container runtime'
docker info --format '{{json .Runtimes}}' 2>&1 || true

echo
echo '== Host GPU'
nvidia-smi
echo

echo '== Java alternatives'
update-alternatives --list java 2>&1 || true
echo
echo '== Actual Java version'
java -version 2>&1

echo
echo '== JAVA_HOME'
echo "${JAVA_HOME:-not set}"

echo
echo '== Python environment'
/home/manoj/.venvs/ecom-gpu/bin/python --version
/home/manoj/.venvs/ecom-gpu/bin/python -c 'import pyspark; print("PySpark:", pyspark.__version__)' 2>&1

echo
echo '== RAPIDS Accelerator JAR'
find /home/manoj -type f -name '*rapids*spark*.jar' 2>/dev/null | head -20

echo
echo '== Benchmark reports'
ls -lh ETL/reports/gpu_benchmark* 2>&1 || true
'@

        Invoke-Remote $checks
        return
    }

    # Fetch benchmark results
    if ($Fetch) {
        $dest = Join-Path $repo "reports\server"
        New-Item -ItemType Directory -Force -Path $dest | Out-Null

        # pscp takes one remote file per call; a missing file is reported, not fatal.
        foreach ($name in @("gpu_benchmark_synthetic.md", "gpu_benchmark_synthetic.json", "gpu_setup.log",
                            "docker_test.log", "docker_build.log", "docker_smoke.log", "docker_run.log")) {
            & $pscpPath -batch -q -P $port -l manoj -pwfile $pwfile "127.0.0.1:ETL/reports/$name" $dest
            if ($LASTEXITCODE -eq 0) { Write-Host "fetched $name" } else { Write-Host "not on the server yet: $name" }
        }

        $report = Join-Path $dest "gpu_benchmark_synthetic.md"
        if (Test-Path $report) { Get-Content $report }
        return
    }

    # Docker workflow: upload scripts/gpu/docker to ~/ecom-docker (outside the git clone,
    # so later pulls never clash with it) and run one step there.
    if ($Docker) {
        if ($Docker -eq "log") {
            $pattern = if ($Log) { "ETL/reports/docker_$Log.log" } else { "ETL/reports/docker_*.log" }
            Invoke-Remote @"
for f in $pattern; do [ -f "`$f" ] || { echo "no log yet: `$f"; continue; }; echo "== `$f (last $Lines lines, updated `$(date -r "`$f" '+%H:%M:%S'))"; tail -n $Lines "`$f"; echo; done
echo '== Running containers'
docker ps --format '{{.ID}}  {{.Image}}  {{.Status}}  {{.Command}}'
"@
            return
        }

        Invoke-Remote "mkdir -p ecom-docker ETL/reports"
        $files = @(Get-ChildItem (Join-Path $PSScriptRoot "docker") -File | ForEach-Object { $_.FullName })
        & $pscpPath -batch -q -P $port -l manoj -pwfile $pwfile @files "127.0.0.1:ecom-docker/"
        if ($LASTEXITCODE -ne 0) {
            throw "PSCP upload failed with exit code $LASTEXITCODE."
        }
        Write-Host "Uploaded $($files.Count) files to ~/ecom-docker"

        if ($Docker -eq "test") {
            Invoke-Remote "chmod +x ecom-docker/*.sh && GPU=$Gpu ecom-docker/test_docker_gpu.sh 2>&1 | tee ETL/reports/docker_test.log"
            return
        }

        $script = "benchmark_in_docker.sh"
        $envs = "GPU=$Gpu ORDERS=$Orders REPEAT=$Repeat ECOM_SPARK_MASTER='local[$Cores]' ECOM_SPARK_DRIVER_MEMORY=$DriverMemory"
        if ($Docker -eq "build") { $script = "build_docker_image.sh"; $envs = "" }
        if ($Docker -eq "smoke") { $envs = "$envs SMOKE=1" }
        $log = "ETL/reports/docker_$Docker.log"

        Invoke-Remote @"
set -e
if [ -d ETL/.git ]; then git -C ETL pull -q; else git clone -q https://github.com/manojhunasimarad/ETL.git; fi
chmod +x ecom-docker/*.sh
nohup env $envs ecom-docker/$script > $log 2>&1 < /dev/null &
echo "Started docker $Docker (log: ~/$log). It keeps running if you disconnect."
"@
        Write-Host "Follow it: .\scripts\gpu\run_on_server.ps1 -Server $Server -Docker log"
        return
    }

    # Benchmark execution configuration
    $mode = ""
    $what = "the benchmark on GPU $Gpu with $Orders orders"
    $actualOrders = $Orders

    if ($Prepare) {
        $mode = "PREPARE_ONLY=1"
        $what = "the preparation (install and $Orders orders of data)"
    }

    if ($Smoke) {
        $mode = "SMOKE=1"
        $what = "the smoke test on GPU $Gpu"
        $actualOrders = 5000
    }

    # Launch benchmark in background
    $start = @"
set -e

if [ -d ETL/.git ]; then
    git -C ETL pull -q
else
    git clone -q https://github.com/manojhunasimarad/ETL.git
fi

cd ETL
mkdir -p reports

nohup env JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PATH=/usr/lib/jvm/java-17-openjdk-amd64/bin:`$PATH $mode CUDA_VISIBLE_DEVICES=$Gpu ORDERS=$actualOrders ECOM_SPARK_MASTER='local[$Cores]' ECOM_SPARK_DRIVER_MEMORY=$DriverMemory ECOM_RAPIDS_CONCURRENT_TASKS=2 ECOM_RAPIDS_BATCH_BYTES=1g bash scripts/gpu/run_gpu_benchmark.sh > reports/server_run.out 2>&1 < /dev/null &
echo 'Started $what. It keeps running if you disconnect.'
"@

    Invoke-Remote $start

    Write-Host "Check progress:  .\scripts\gpu\run_on_server.ps1 -Server $Server -Status"
    Write-Host "When it is done: .\scripts\gpu\run_on_server.ps1 -Server $Server -Fetch"
}
finally {
    if ($tunnel -and !$tunnel.HasExited) {
        Stop-Process -Id $tunnel.Id -Force -ErrorAction SilentlyContinue
    }

    Remove-Item $pwfile, $tunnelOut, $tunnelErr -Force -ErrorAction SilentlyContinue
}
