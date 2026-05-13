param(
    [Parameter(Mandatory = $true)]
    [string]$OutputPrefix,

    [int]$Port = 4296,

    [int]$Seconds = 45
)

$ErrorActionPreference = "Continue"

$repo = Split-Path -Parent $PSScriptRoot
$captureDir = Join-Path $repo "captures"
New-Item -ItemType Directory -Force -Path $captureDir | Out-Null

$log = Join-Path $captureDir "$OutputPrefix-pktmon-admin.log"
$etl = Join-Path $captureDir "$OutputPrefix.etl"
$pcapng = Join-Path $captureDir "$OutputPrefix.pcapng"

"Starting pktmon capture. Prefix=$OutputPrefix Port=$Port Seconds=$Seconds" | Out-File -FilePath $log -Encoding utf8
"Repo=$repo" | Out-File -FilePath $log -Append -Encoding utf8

try {
    pktmon stop 2>&1 | Out-File -FilePath $log -Append -Encoding utf8
    pktmon filter remove 2>&1 | Out-File -FilePath $log -Append -Encoding utf8
    pktmon filter add -p $Port 2>&1 | Out-File -FilePath $log -Append -Encoding utf8
    pktmon start --capture --pkt-size 0 --comp all --file-name $etl 2>&1 | Out-File -FilePath $log -Append -Encoding utf8
    Start-Sleep -Seconds $Seconds
    pktmon stop 2>&1 | Out-File -FilePath $log -Append -Encoding utf8
    pktmon etl2pcap $etl -o $pcapng 2>&1 | Out-File -FilePath $log -Append -Encoding utf8
    pktmon filter remove 2>&1 | Out-File -FilePath $log -Append -Encoding utf8
    "Capture complete: $etl" | Out-File -FilePath $log -Append -Encoding utf8
    "Formatted output: $pcapng" | Out-File -FilePath $log -Append -Encoding utf8
} catch {
    "Capture failed: $_" | Out-File -FilePath $log -Append -Encoding utf8
    try { pktmon stop 2>&1 | Out-File -FilePath $log -Append -Encoding utf8 } catch {}
    try { pktmon filter remove 2>&1 | Out-File -FilePath $log -Append -Encoding utf8 } catch {}
    exit 1
}
