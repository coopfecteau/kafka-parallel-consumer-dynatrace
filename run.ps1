# Restart the three demo JVMs for one experiment.
#   .\run.ps1 -Mode sdk    -> TRACING=sdk,  DT_TAGS=primary_tags.instrumentation=sdk
#   .\run.ps1 -Mode otel   -> TRACING=otel, DT_TAGS=primary_tags.instrumentation=otel
#   .\run.ps1 -Mode none   -> native OneAgent sensors only
# DT_TAGS is read by OneAgent at process start, so a tag change always needs a restart.
param([ValidateSet('none', 'sdk', 'otel')][string]$Mode = 'none')

$root = $PSScriptRoot
$jar = Join-Path $root 'target\pc-oneagent-demo.jar'
New-Item -ItemType Directory -Force (Join-Path $root 'logs') | Out-Null

# Only stop our own JVMs (matched by jar on the command line), never the Kafka broker.
Get-CimInstance Win32_Process -Filter "Name='java.exe'" |
    Where-Object { $_.CommandLine -like '*pc-oneagent-demo.jar*' } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force }

$env:TRACING = $Mode
$env:DT_TAGS = "primary_tags.instrumentation=$Mode"

foreach ($role in 'producer', 'parallel-consumer', 'plain-consumer') {
    $p = Start-Process java -ArgumentList '-jar', $jar, $role -WorkingDirectory $root -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $root "logs\$role.log") `
        -RedirectStandardError (Join-Path $root "logs\$role.err.log")
    "{0,-18} pid={1}  TRACING={2}  DT_TAGS={3}" -f $role, $p.Id, $Mode, $env:DT_TAGS
    # Producer creates the topic; give it a head start so consumers don't race it.
    if ($role -eq 'producer') { Start-Sleep 5 }
}
