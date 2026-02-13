param(
    [ValidateSet('health','refresh-all','build-queue','run','movies','tv','dry-run')]
    [string]$Command = 'health',
    [switch]$Yes,
    [switch]$VerboseLogs
)

$ErrorActionPreference = 'Stop'

function Resolve-DiscMapperRoots {
    param([string]$StartPath)

    $start = (Resolve-Path $StartPath).Path
    $scriptRoot = Split-Path -Parent $PSCommandPath

    $candidates = @($start, $scriptRoot)
    foreach ($candidate in $candidates) {
        $unifiedDirect = Join-Path $candidate 'DiscMapper_unified'
        if ((Test-Path (Join-Path $candidate 'App')) -and (Test-Path (Join-Path $candidate 'Inputs'))) {
            return @{
                RepoRoot = (Split-Path -Parent $candidate)
                UnifiedRoot = $candidate
            }
        }
        if ((Test-Path (Join-Path $unifiedDirect 'App')) -and (Test-Path (Join-Path $unifiedDirect 'Inputs'))) {
            return @{
                RepoRoot = $candidate
                UnifiedRoot = $unifiedDirect
            }
        }
    }

    throw "Could not locate DiscMapper_unified folder from: $StartPath"
}

$roots = Resolve-DiscMapperRoots -StartPath (Get-Location)
$python = Get-Command python -ErrorAction SilentlyContinue
if (-not $python) {
    throw 'python was not found in PATH. Install Python 3.10+ and retry.'
}

$scriptPath = Join-Path $roots.UnifiedRoot 'App\discmapper_unified_v03.py'
if (-not (Test-Path $scriptPath)) {
    throw "Missing unified launcher: $scriptPath"
}

$args = @($scriptPath, $Command)
if ($Yes) { $args += '--yes' }
if ($VerboseLogs) { $args += '--verbose' }

Write-Host "[DiscMapper] Repo root: $($roots.RepoRoot)"
Write-Host "[DiscMapper] Unified root: $($roots.UnifiedRoot)"
Write-Host "[DiscMapper] Running: python $($args -join ' ')"

Push-Location $roots.UnifiedRoot
try {
    & python @args
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
