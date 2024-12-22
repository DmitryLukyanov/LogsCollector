vector validate --no-environment --config-yaml .\config\vector.yaml
if ($LASTEXITCODE -ne 0) {
    Write-Host "Config validation has been failed with code: $LASTEXITCODE"
    exit 0
}

# require-healthy: Exit on startup if any sinks fail healthchecks

# glob_minimum_cooldown_ms
# The delay between file discovery calls.
# This controls the interval at which files are searched. 
# A higher value results in greater chances of some short-lived files being missed between searches, 
# but a lower value increases the performance impact of file discovery.

vector `
    -c .\config\vector.yaml `
    --require-healthy true `
    --color always `
    --no-graceful-shutdown-limit `
    --watch-config `
    --verbose `
