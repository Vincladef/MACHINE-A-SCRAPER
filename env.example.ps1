# Renseigne tes valeurs puis exécute:  `powershell -ExecutionPolicy Bypass -File .\env.ps1`

$env:CSE_API_KEY = "<TA_CLE_API>"
$env:CSE_CX_ID = "<TON_CX_ID>"
$env:GOOGLE_SHEET_ID = "<TON_SHEET_ID>"

# Charge le JSON COMPLET du service account local
$env:GOOGLE_CREDENTIALS = Get-Content -Raw -Path .\service_account.json

# Préférences (facultatif)
$env:DEEP_SCRAPE = "true"
$env:MAX_RESULTS = "100"
$env:REQUEST_DELAY = "1.0"
$env:HTTP_TIMEOUT = "10"

Write-Host "Variables d'environnement chargées pour cette session."

