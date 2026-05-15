param(
    [Parameter(Mandatory = $true)]
    [string]$OutputPrefix,

    [int]$Port = 4296,

    [int]$Seconds = 45,

    [string]$WiresharkDir = "C:\Program Files\Wireshark",

    [string[]]$Interfaces = @("4", "5", "10", "12")
)

$ErrorActionPreference = "Stop"

$repo = Split-Path -Parent $PSScriptRoot
$captureDir = Join-Path $repo "captures"
New-Item -ItemType Directory -Force -Path $captureDir | Out-Null

$dumpcap = Join-Path $WiresharkDir "dumpcap.exe"
$tshark = Join-Path $WiresharkDir "tshark.exe"
$pcapng = Join-Path $captureDir "$OutputPrefix.pcapng"
$tsv = Join-Path $captureDir "$OutputPrefix-udp$Port.tsv"
$log = Join-Path $captureDir "$OutputPrefix-dumpcap.log"

if (!(Test-Path $dumpcap)) {
    throw "dumpcap.exe was not found at $dumpcap"
}
if (!(Test-Path $tshark)) {
    throw "tshark.exe was not found at $tshark"
}

$args = @("-q", "-a", "duration:$Seconds", "-w", $pcapng)
foreach ($iface in $Interfaces) {
    $args += @("-i", $iface)
}

"Starting dumpcap capture. Prefix=$OutputPrefix Port=$Port Seconds=$Seconds Interfaces=$($Interfaces -join ',')" | Out-File -FilePath $log -Encoding utf8
& $dumpcap @args 2>&1 | Out-File -FilePath $log -Append -Encoding utf8

& $tshark -r $pcapng -Y "udp.port == $Port" -T fields `
    -e frame.number -e frame.interface_id -e frame.time_relative `
    -e ip.src -e udp.srcport -e ip.dst -e udp.dstport -e udp.length -e data.data |
    Out-File -FilePath $tsv -Encoding utf8

"Capture complete: $pcapng" | Out-File -FilePath $log -Append -Encoding utf8
"Filtered UDP TSV: $tsv" | Out-File -FilePath $log -Append -Encoding utf8
