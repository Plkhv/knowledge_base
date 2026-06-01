$ErrorActionPreference = "Stop"

$repoRoot = $PSScriptRoot
$python = Join-Path $repoRoot ".venv-1\Scripts\python.exe"

if (-not (Test-Path $python)) {
    $python = "python"
}

& $python -m PyInstaller `
    --noconfirm `
    --clean `
    --onefile `
    --windowed `
    --name LakehouseAdminPanel `
    --distpath (Join-Path $repoRoot "dist") `
    --workpath (Join-Path $repoRoot "build") `
    --specpath (Join-Path $repoRoot "build\specs") `
    (Join-Path $repoRoot "admin\main.py")
