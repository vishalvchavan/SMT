# Deployment script for local Kafka Connect
# Run this manually or via GitHub Actions self-hosted runner

param(
    [string]$JarPath = ".\target\connect-smt-1.0.0.jar",
    [string]$TargetPath = "C:\Users\vihan\confluent_local\kafka-connect-plugins\custom-smt",
    [string]$DockerComposePath = "C:\Users\vihan\confluent_local",
    [string]$ContainerName = "connect",
    [switch]$RestartConnectors
)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Kafka Connect SMT Deployment Script" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Check if JAR exists
if (-not (Test-Path $JarPath)) {
    Write-Host "ERROR: JAR not found at $JarPath" -ForegroundColor Red
    Write-Host "Run 'mvn clean package -DskipTests' first" -ForegroundColor Yellow
    exit 1
}

# Create target directory if not exists
if (-not (Test-Path $TargetPath)) {
    Write-Host "Creating directory: $TargetPath" -ForegroundColor Yellow
    New-Item -ItemType Directory -Path $TargetPath -Force | Out-Null
}

# Copy JAR
Write-Host "`nStep 1: Copying JAR to plugins folder..." -ForegroundColor Green
Copy-Item -Path $JarPath -Destination $TargetPath -Force
Write-Host "  Copied to: $TargetPath\connect-smt-1.0.0.jar" -ForegroundColor Gray

# Restart Connect container
Write-Host "`nStep 2: Restarting Connect container..." -ForegroundColor Green
docker restart $ContainerName

# Wait for Connect to be ready
Write-Host "`nStep 3: Waiting for Connect to be ready..." -ForegroundColor Green
$maxRetries = 30
$retryCount = 0
$isReady = $false

while (-not $isReady -and $retryCount -lt $maxRetries) {
    Start-Sleep -Seconds 2
    $retryCount++
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:8083/connectors" -Method Get -ErrorAction SilentlyContinue
        $isReady = $true
        Write-Host "  Connect is ready! (attempt $retryCount)" -ForegroundColor Gray
    } catch {
        Write-Host "  Waiting... (attempt $retryCount/$maxRetries)" -ForegroundColor Gray
    }
}

if (-not $isReady) {
    Write-Host "ERROR: Connect failed to start within timeout" -ForegroundColor Red
    exit 1
}

# List connectors
Write-Host "`nStep 4: Current connectors:" -ForegroundColor Green
$connectors = Invoke-RestMethod -Uri "http://localhost:8083/connectors" -Method Get
$connectors | ForEach-Object { Write-Host "  - $_" -ForegroundColor Gray }

# Optionally restart connectors
if ($RestartConnectors) {
    Write-Host "`nStep 5: Restarting all connectors..." -ForegroundColor Green
    $connectors | ForEach-Object {
        Write-Host "  Restarting: $_" -ForegroundColor Gray
        Invoke-RestMethod -Uri "http://localhost:8083/connectors/$_/restart" -Method Post -ErrorAction SilentlyContinue
    }
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Deployment Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
