# Setting Up GitHub Self-Hosted Runner

This guide explains how to set up a self-hosted runner on your local machine for the CI/CD pipeline.

## Prerequisites

- Windows 10/11 with PowerShell
- Docker Desktop running
- GitHub account with access to the repository

## Step 1: Navigate to Runner Settings

1. Go to your repository: https://github.com/vishalvchavan/SMT
2. Click **Settings** → **Actions** → **Runners**
3. Click **New self-hosted runner**
4. Select **Windows** and **x64**

## Step 2: Download and Configure Runner

Open PowerShell as Administrator and run:

```powershell
# Create a folder for the runner
mkdir C:\actions-runner && cd C:\actions-runner

# Download the runner package (check GitHub for latest version)
Invoke-WebRequest -Uri https://github.com/actions/runner/releases/download/v2.311.0/actions-runner-win-x64-2.311.0.zip -OutFile actions-runner-win-x64-2.311.0.zip

# Extract
Add-Type -AssemblyName System.IO.Compression.FileSystem
[System.IO.Compression.ZipFile]::ExtractToDirectory("$PWD\actions-runner-win-x64-2.311.0.zip", "$PWD")
```

## Step 3: Configure the Runner

```powershell
# Configure (use the token from GitHub Settings page)
.\config.cmd --url https://github.com/vishalvchavan/SMT --token YOUR_TOKEN_HERE
```

When prompted:
- **Runner group**: Press Enter for default
- **Runner name**: Enter a name like `local-windows`
- **Work folder**: Press Enter for default

## Step 4: Run as Service

```powershell
# Install as Windows service (recommended)
.\svc.cmd install

# Start the service
.\svc.cmd start
```

Or run interactively:
```powershell
.\run.cmd
```

## Step 5: Verify Runner

1. Go to **Settings** → **Actions** → **Runners**
2. You should see your runner with status "Idle"

## Troubleshooting

### Runner not connecting
```powershell
# Check service status
Get-Service actions.runner.*

# Restart service
.\svc.cmd stop
.\svc.cmd start
```

### Docker commands failing
Make sure Docker Desktop is running and the runner service account has access.

## Uninstall Runner

```powershell
.\svc.cmd stop
.\svc.cmd uninstall
.\config.cmd remove --token YOUR_TOKEN_HERE
```
