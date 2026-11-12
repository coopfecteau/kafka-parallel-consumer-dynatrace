# One-time local setup on Windows without Docker/WSL: Kafka (KRaft, single node) + Maven
# into .\tools, patched to run under cmd on Windows 11. Idempotent; re-run is safe.
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$root = Split-Path $PSScriptRoot -Parent
$tools = Join-Path $root 'tools'
$kafkaVer = '4.4.0'; $mavenVer = '3.9.16'
New-Item -ItemType Directory -Force $tools | Out-Null

$kafka = Join-Path $tools 'kafka'
if (-not (Test-Path $kafka)) {
    $tgz = Join-Path $tools "kafka.tgz"
    Invoke-WebRequest "https://archive.apache.org/dist/kafka/$kafkaVer/kafka_2.13-$kafkaVer.tgz" -OutFile $tgz
    tar -xzf $tgz -C $tools
    Rename-Item (Join-Path $tools "kafka_2.13-$kafkaVer") 'kafka'
    Remove-Item $tgz
}

$maven = Join-Path $tools "apache-maven-$mavenVer"
if (-not (Test-Path $maven)) {
    $zip = Join-Path $tools 'maven.zip'
    Invoke-WebRequest "https://archive.apache.org/dist/maven/maven-3/$mavenVer/binaries/apache-maven-$mavenVer-bin.zip" -OutFile $zip
    Expand-Archive $zip -DestinationPath $tools
    Remove-Item $zip
}

# The stock script appends every jar to CLASSPATH one by one, which overflows cmd's
# 8191-char line limit ("The input line is too long"). A wildcard classpath avoids it.
$runClass = Join-Path $kafka 'bin\windows\kafka-run-class.bat'
$text = Get-Content $runClass -Raw
$loop = '(?s)for %%i in \("%BASE_DIR%\\libs\\\*"\) do \(\s*call :concat "%%i"\s*\)'
if ($text -match $loop) {
    ($text -replace $loop, 'set CLASSPATH=%BASE_DIR%\libs\*') | Set-Content -Encoding ascii $runClass
}

$data = (Join-Path $tools 'kafka-data') -replace '\\', '/'
$props = Join-Path $kafka 'config\local.properties'
(Get-Content (Join-Path $kafka 'config\server.properties')) `
    -replace '^log.dirs=.*', "log.dirs=$data" `
    -replace '^num.partitions=.*', 'num.partitions=6' | Set-Content -Encoding ascii $props

if (-not (Test-Path (Join-Path $tools 'kafka-data\meta.properties'))) {
    & (Join-Path $kafka 'bin\windows\kafka-storage.bat') format --standalone -t 'q1Kafka-local-cluster0' -c $props
}

Write-Host "Done. Start the broker with:"
Write-Host '  $env:KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"   # skips the wmic call removed in Windows 11 24H2+'
Write-Host "  $kafka\bin\windows\kafka-server-start.bat $props"
