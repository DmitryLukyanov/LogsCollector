$env:FILE_SOURCE_PATH="./logs/*.log"
$env:HTTP_SINK_URI="http://localhost:7245/api/LogsTransmitter"
$env:HTTP_SOURCE_URI="http://localhost:7175/api/LogsSource"

vector validate --no-environment --config-yaml .\config\vector.yaml
if ($LASTEXITCODE -ne 0) {
    Write-Host "Config validation has been failed with code: $LASTEXITCODE"
    exit 0
}

If (!(test-path .\data)) {
    Write-Host 'Creating a data folder..'
    New-Item -ItemType Directory -Path .\data
}

Write-Host 'Starting flow..'

# require-healthy: Exit on startup if any sinks fail healthchecks

vector `
    --config .\config\vector.yaml `
    --require-healthy true `
    --color always `
    --no-graceful-shutdown-limit `
    --watch-config `
    --verbose `
