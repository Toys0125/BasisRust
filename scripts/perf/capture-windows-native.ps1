param(
    [Parameter(Mandatory = $true)][string]$OutputDirectory,
    [Parameter(Mandatory = $true)][string]$TriggerFile,
    [Parameter(Mandatory = $true)][string]$RtkPath,
    [int]$CaptureSeconds = 30,
    [switch]$IncludeClr,
    [string]$FirewallRemoteAddress = '',
    [string]$FirewallLocalAddress = '',
    [int]$FirewallUdpPort = 0
)

$ErrorActionPreference = 'Stop'
$captureRoot = [IO.Path]::GetFullPath($OutputDirectory)
$triggerPath = [IO.Path]::GetFullPath($TriggerFile)
$statePath = Join-Path $captureRoot 'native-status.json'
$tracePath = Join-Path $captureRoot 'cpu-stacks.etl'
$abortPath = Join-Path $captureRoot 'native-abort.marker'
$profilePath = Join-Path $PSScriptRoot 'windows-cpu-sampling.wprp'
$instance = 'BasisRustNative-' + [Guid]::NewGuid().ToString('N')
$recording = $false
$firewallCreated = $false
$firewallName = $instance + '-UDP'
$captureState = @{ instance = $instance; captureSeconds = $CaptureSeconds }

function Save-CaptureState([string]$phase) {
    $captureState.phase = $phase
    $captureState.updatedUtc = [DateTime]::UtcNow.ToString('o')
    $captureState | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $statePath -Encoding UTF8
}

try {
    $principal = [Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw 'Windows CPU stack tracing requires an administrator token.'
    }
    if ($CaptureSeconds -lt 5 -or $CaptureSeconds -gt 120) { throw 'CaptureSeconds must be between 5 and 120.' }
    if (-not (Test-Path -LiteralPath $captureRoot -PathType Container)) { throw 'Capture output directory must already exist.' }
    if (Test-Path -LiteralPath $tracePath) { throw 'Refusing to overwrite an existing native trace.' }
    if ($FirewallRemoteAddress) {
        if ($FirewallUdpPort -lt 1 -or $FirewallUdpPort -gt 65535 -or -not $FirewallLocalAddress) {
            throw 'A valid UDP port and local LAN address are required for a remote capture.'
        }
        New-NetFirewallRule -Name $firewallName -DisplayName $firewallName -Direction Inbound -Action Allow `
            -Protocol UDP -LocalPort $FirewallUdpPort -RemoteAddress $FirewallRemoteAddress `
            -LocalAddress $FirewallLocalAddress -Program (Join-Path $captureRoot 'symbols/basis-server-console.exe') | Out-Null
        $firewallCreated = $true
        $captureState.firewallRuleName = $firewallName
    }
    Save-CaptureState 'armed'
    $readyDeadline = [DateTime]::UtcNow.AddMinutes(8)
    while (-not (Test-Path -LiteralPath $triggerPath)) {
        if (Test-Path -LiteralPath $abortPath) { throw 'Capture aborted before workload readiness.' }
        if ([DateTime]::UtcNow -gt $readyDeadline) { throw 'Workload did not reach its measurement window within eight minutes.' }
        Start-Sleep -Milliseconds 200
    }
    $profileName = if ($IncludeClr) { 'BasisCpuClr' } else { 'BasisCpu' }
    $captureState.profile = $profilePath + '!' + $profileName
    $captureState.includeClr = [bool]$IncludeClr
    $captureState.startOutput = (& $RtkPath proxy wpr -start $captureState.profile -filemode -instancename $instance 2>&1 | Out-String)
    if ($LASTEXITCODE -ne 0) { throw "WPR start failed with exit code $LASTEXITCODE." }
    $recording = $true
    $captureState.startedUtc = [DateTime]::UtcNow.ToString('o')
    Save-CaptureState 'recording'
    $captureDeadline = [DateTime]::UtcNow.AddSeconds($CaptureSeconds)
    while ([DateTime]::UtcNow -lt $captureDeadline) {
        if (Test-Path -LiteralPath $abortPath) { throw 'Capture aborted during recording.' }
        Start-Sleep -Milliseconds 200
    }
    $captureState.stoppedUtc = [DateTime]::UtcNow.ToString('o')
    Save-CaptureState 'merging'
    $captureState.stopOutput = (& $RtkPath proxy wpr -stop $tracePath -instancename $instance 2>&1 | Out-String)
    if ($LASTEXITCODE -ne 0) { throw "WPR stop failed with exit code $LASTEXITCODE." }
    $recording = $false
    $captureState.traceBytes = (Get-Item -LiteralPath $tracePath).Length
    Save-CaptureState 'complete'
} catch {
    $captureState.error = $_.Exception.Message
    Save-CaptureState 'failed'
    exit 1
} finally {
    if ($recording) {
        & $RtkPath proxy wpr -cancel -instancename $instance *> (Join-Path $captureRoot 'native-cancel.log')
    }
    if ($firewallCreated) {
        # Keep the narrowly scoped rule through the outer workload window,
        # even if ETL merging finishes before that window does.
        $cleanupDeadline = [DateTime]::UtcNow.AddMinutes(5)
        while ($captureState.phase -eq 'complete' -and
               -not (Test-Path -LiteralPath (Join-Path $captureRoot 'workload-done.marker')) -and
               -not (Test-Path -LiteralPath $abortPath) -and
               [DateTime]::UtcNow -lt $cleanupDeadline) {
            Start-Sleep -Milliseconds 200
        }
        try {
            Remove-NetFirewallRule -Name $firewallName
            $captureState.firewallRemoved = $true
            Save-CaptureState $captureState.phase
        } catch {
            $captureState.error = 'Temporary firewall rule cleanup failed: ' + $_.Exception.Message
            Save-CaptureState 'failed'
        }
    }
}
