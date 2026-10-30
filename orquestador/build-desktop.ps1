# Build del .exe de escritorio (onedir: arranque rápido, sin auto-extracción a Temp).
# Usa siempre DB-Detector-Orquestador.spec para evitar que un rebuild manual
# vuelva a --onefile (causa el warning "Failed to remove temporary directory").
#
# Uso: .\build-desktop.ps1

Set-Location $PSScriptRoot

# Defender escanea los .dll recien escritos (binarios nuevos, sin firma) y a
# veces los deja bloqueados un buen rato - "Acceso denegado" al reconstruir,
# incluso con la app cerrada. Cada build va a una carpeta de staging con
# nombre UNICO (timestamp) para no depender nunca de poder borrar/reusar la
# del build anterior (que puede seguir trabada). Despues del build intentamos
# el swap atomico hacia dist\DB-Detector-Orquestador (rename, no delete).
$stamp       = Get-Date -Format "yyyyMMdd_HHmmss"
$stagingRoot = Join-Path $PSScriptRoot "dist_staging_$stamp"
$distDir     = Join-Path $PSScriptRoot "dist\DB-Detector-Orquestador"

python -m PyInstaller DB-Detector-Orquestador.spec --noconfirm --distpath $stagingRoot
$buildOk = $LASTEXITCODE -eq 0
$stagedExe = Join-Path $stagingRoot "DB-Detector-Orquestador\DB-Detector-Orquestador.exe"

Write-Output ""
if (-not $buildOk -or -not (Test-Path $stagedExe)) {
    Write-Output "Build FALLO (exit code $LASTEXITCODE) - revisa el log de arriba."
    exit 1
}

# Limpiar carpetas de staging/swap viejas que ya se liberaron (best-effort,
# nunca bloquea el build de hoy si alguna sigue trabada).
Get-ChildItem $PSScriptRoot -Directory -Filter "dist_staging_*" -ErrorAction SilentlyContinue |
    Where-Object { $_.FullName -ne $stagingRoot } |
    ForEach-Object { Remove-Item -Recurse -Force $_.FullName -ErrorAction SilentlyContinue }
Get-ChildItem (Join-Path $PSScriptRoot "dist") -Directory -Filter "DB-Detector-Orquestador_old_*" -ErrorAction SilentlyContinue |
    ForEach-Object { Remove-Item -Recurse -Force $_.FullName -ErrorAction SilentlyContinue }

$swapped = $false
if (Test-Path $distDir) {
    for ($i = 0; $i -lt 4 -and -not $swapped; $i++) {
        try {
            Rename-Item -Path $distDir -NewName "DB-Detector-Orquestador_old_$(Get-Date -Format 'HHmmss')_$i" -ErrorAction Stop
            $swapped = $true
        } catch {
            Start-Sleep -Seconds 3
        }
    }
    if (-not $swapped) {
        Write-Output "Build listo pero la carpeta vieja sigue bloqueada - no se pudo reemplazar."
        Write-Output "Usa este .exe por ahora: $stagedExe"
        Write-Output "(corre este script de nuevo mas tarde para que haga el swap solo)"
        exit 0
    }
} else {
    $swapped = $true
}

Move-Item -Path (Join-Path $stagingRoot "DB-Detector-Orquestador") -Destination $distDir
Remove-Item -Recurse -Force $stagingRoot -ErrorAction SilentlyContinue

Write-Output "Build listo: dist\DB-Detector-Orquestador\DB-Detector-Orquestador.exe"
