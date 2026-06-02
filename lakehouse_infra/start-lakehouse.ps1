[CmdletBinding()]
param(
    [switch]$ForceRegenerateSecrets,
    [switch]$SkipUp,
    [string]$ComposeFile = "docker-compose.yml"
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

$envFile = Join-Path $scriptDir ".env"
$composePath = Join-Path $scriptDir $ComposeFile

$composeProgram = $null
$composeBaseArgs = @()

if (-not (Test-Path $composePath)) {
    throw "Compose file not found: $composePath"
}

function New-RandomString {
    param([int]$Length = 24)

    $chars = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    $bytes = New-Object byte[] $Length
    $rng.GetBytes($bytes)

    $result = New-Object System.Text.StringBuilder
    foreach ($b in $bytes) {
        [void]$result.Append($chars[$b % $chars.Length])
    }

    return $result.ToString()
}

function New-FernetLikeKey {
    $bytes = New-Object byte[] 32
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    $rng.GetBytes($bytes)
    return [Convert]::ToBase64String($bytes)
}

function Read-EnvFile {
    param([string]$Path)

    $map = @{}

    if (-not (Test-Path $Path)) {
        return $map
    }

    foreach ($line in Get-Content -Path $Path -Encoding UTF8) {
        $trimmed = $line.Trim()
        if ($trimmed -eq "" -or $trimmed.StartsWith("#")) {
            continue
        }

        $idx = $line.IndexOf("=")
        if ($idx -lt 1) {
            continue
        }

        $key = $line.Substring(0, $idx).Trim()
        $key = $key.Trim([char]0xFEFF)
        $value = $line.Substring($idx + 1)
        $map[$key] = $value
    }

    return $map
}

function Write-EnvFileNoBom {
    param(
        [string]$Path,
        [string[]]$Content
    )

    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllLines($Path, $Content, $utf8NoBom)
}

function Resolve-ComposeCommand {
    $dockerCmd = Get-Command docker -ErrorAction SilentlyContinue
    if ($dockerCmd) {
        & docker compose version *> $null
        if ($LASTEXITCODE -eq 0) {
            return @{
                Program = "docker"
                BaseArgs = @("compose")
                Label = "docker compose"
            }
        }
    }

    $dockerComposeCmd = Get-Command docker-compose -ErrorAction SilentlyContinue
    if ($dockerComposeCmd) {
        & docker-compose version *> $null
        if ($LASTEXITCODE -eq 0) {
            return @{
                Program = "docker-compose"
                BaseArgs = @()
                Label = "docker-compose"
            }
        }
    }

    throw "Docker Compose not found. Install Docker Desktop (Compose v2) or docker-compose v1 and ensure it is in PATH"
}

function Invoke-Compose {
    param([string[]]$Args)

    & $script:composeProgram @script:composeBaseArgs @Args
    if ($LASTEXITCODE -ne 0) {
        throw "Compose command failed with exit code $LASTEXITCODE"
    }
}

function Get-DefaultValue {
    param([string]$Name)

    switch ($Name) {
        "POSTGRES_USER" { return "lakehouse" }
        "POSTGRES_PASSWORD" { return New-RandomString -Length 24 }
        "POSTGRES_DB" { return "lakehouse" }
        "MINIO_ROOT_USER" { return "minioadmin" }
        "MINIO_ROOT_PASSWORD" { return New-RandomString -Length 24 }
        "AIRFLOW_DB_NAME" { return "airflow" }
        "AIRFLOW_FERNET_KEY" { return New-FernetLikeKey }
        "AIRFLOW_ADMIN_USERNAME" { return "admin" }
        "AIRFLOW_ADMIN_PASSWORD" { return New-RandomString -Length 24 }
        default { throw "No default value rule for: $Name" }
    }
}

$requiredVars = @(
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_DB",
    "MINIO_ROOT_USER",
    "MINIO_ROOT_PASSWORD",
    "AIRFLOW_DB_NAME",
    "AIRFLOW_FERNET_KEY",
    "AIRFLOW_ADMIN_USERNAME",
    "AIRFLOW_ADMIN_PASSWORD"
)

$secretVars = @(
    "POSTGRES_PASSWORD",
    "MINIO_ROOT_PASSWORD",
    "AIRFLOW_FERNET_KEY",
    "AIRFLOW_ADMIN_PASSWORD"
)

$envMap = Read-EnvFile -Path $envFile
$changes = New-Object System.Collections.Generic.List[string]

foreach ($name in $requiredVars) {
    $existing = $null
    if ($envMap.ContainsKey($name)) {
        $existing = $envMap[$name]
    }

    $shouldSet = [string]::IsNullOrWhiteSpace($existing)
    $shouldRotate = $ForceRegenerateSecrets -and ($secretVars -contains $name)

    if ($shouldSet -or $shouldRotate) {
        $envMap[$name] = Get-DefaultValue -Name $name
        if ($shouldSet) {
            $changes.Add("set $name")
        }
        elseif ($shouldRotate) {
            $changes.Add("rotated $name")
        }
    }
}

$lines = New-Object System.Collections.Generic.List[string]
$lines.Add("# Auto-generated for lakehouse_infra/docker-compose.yml")
$lines.Add("# Re-run start-lakehouse.ps1 to fill missing variables")

foreach ($name in $requiredVars) {
    $lines.Add("$name=$($envMap[$name])")
}

$extraKeys = $envMap.Keys | Where-Object { $requiredVars -notcontains $_ } | Sort-Object
if ($extraKeys.Count -gt 0) {
    $lines.Add("")
    $lines.Add("# Existing custom variables")
    foreach ($name in $extraKeys) {
        $lines.Add("$name=$($envMap[$name])")
    }
}

Write-EnvFileNoBom -Path $envFile -Content $lines

if ($changes.Count -gt 0) {
    Write-Host "Updated .env:"
    foreach ($change in $changes) {
        Write-Host "  - $change"
    }
}
else {
    Write-Host ".env already contains all required variables"
}

if (-not $SkipUp) {
    $composeInfo = Resolve-ComposeCommand
    $composeProgram = $composeInfo.Program
    $composeBaseArgs = $composeInfo.BaseArgs

    Write-Host "Using compose command: $($composeInfo.Label)"

    Write-Host "Checking docker compose configuration..."
    Invoke-Compose -Args @("--env-file", $envFile, "-f", $composePath, "config") | Out-Null

    Write-Host "Starting infrastructure..."
    Invoke-Compose -Args @("--env-file", $envFile, "-f", $composePath, "up", "-d")

    Write-Host "Done. Infrastructure is starting in background."
    Write-Host "Useful checks:"
    Write-Host "  $($composeInfo.Label) --env-file .env -f docker-compose.yml ps"
    Write-Host "  $($composeInfo.Label) --env-file .env -f docker-compose.yml logs -f"
}
else {
    Write-Host "Skipped docker compose up because -SkipUp was provided"
}
