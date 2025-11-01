param(
    [switch]$LoadEnv = $true
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Push-Location $PSScriptRoot
try {
    if ($LoadEnv) {
        if (Test-Path .\env.ps1) {
            . .\env.ps1
        } else {
            Write-Host "Aucun env.ps1 trouvé. Copie d'abord env.example.ps1 en env.ps1 et remplis tes valeurs." -ForegroundColor Yellow
        }
    }

    if (-not (Test-Path .\.venv\Scripts\python.exe)) {
        throw "Python .venv introuvable."
    }

    .\.venv\Scripts\python.exe .\scraper.py
}
finally {
    Pop-Location
}

