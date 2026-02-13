param(
    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'

Write-Host '[smoke] Python version check'
python -c "import sys; print(sys.version)"
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$repoRoot = Split-Path -Parent $PSScriptRoot
$launcher = Join-Path $repoRoot 'DiscMapper_unified\App\discmapper_unified_v03.py'

Write-Host '[smoke] Unified health check'
python $launcher health
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

if ($DryRun) {
    Write-Host '[smoke] Movies dry-run check (no disc required)'
    python $launcher dry-run --yes
    exit $LASTEXITCODE
}

Write-Host '[smoke] Note: use -DryRun to validate rip flow without hardware.'
