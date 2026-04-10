# Ordo installer for Windows — downloads a prebuilt binary from GitHub Releases.
#
# Usage:
#   irm https://raw.githubusercontent.com/uwni/ordo/main/install.ps1 | iex
#   install.ps1 -To C:\tools -Version v0.1.0

param(
    [string]$To = "",
    [string]$Version = "",
    [switch]$Help
)

$ErrorActionPreference = "Stop"
$Repo = "uwni/ordo"
$BinName = "ordo.exe"

function Show-Usage {
    Write-Host @"
Ordo installer for Windows

Usage:
    install.ps1 [OPTIONS]

Options:
    -To <dir>        Install directory (default: %LOCALAPPDATA%\Programs\ordo)
    -Version <tag>   Install a specific release tag (default: latest)
    -Help            Show this help
"@
}

if ($Help) {
    Show-Usage
    exit 0
}

# Resolve latest version
if (-not $Version) {
    $release = Invoke-RestMethod -Uri "https://api.github.com/repos/$Repo/releases/latest"
    $Version = $release.tag_name
    if (-not $Version) {
        Write-Error "Could not determine latest release; pass -Version explicitly"
        exit 1
    }
}

# Resolve install directory
if (-not $To) {
    $To = Join-Path $env:LOCALAPPDATA "Programs\ordo"
}

$Target = "x86_64-windows"
$Archive = "ordo-$Version-$Target.zip"
$Url = "https://github.com/$Repo/releases/download/$Version/$Archive"

Write-Host "  target:  $Target"
Write-Host "  version: $Version"
Write-Host "  url:     $Url"
Write-Host "  dest:    $To"
Write-Host ""

# Download
$TmpDir = Join-Path ([System.IO.Path]::GetTempPath()) ("ordo-install-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $TmpDir -Force | Out-Null

try {
    $ArchivePath = Join-Path $TmpDir $Archive
    Write-Host "Downloading $Archive ..."
    Invoke-WebRequest -Uri $Url -OutFile $ArchivePath -UseBasicParsing

    # Extract
    $ExtractDir = Join-Path $TmpDir "extract"
    Expand-Archive -Path $ArchivePath -DestinationPath $ExtractDir -Force

    # Install
    if (-not (Test-Path $To)) {
        New-Item -ItemType Directory -Path $To -Force | Out-Null
    }
    Copy-Item (Join-Path $ExtractDir $BinName) (Join-Path $To $BinName) -Force

    Write-Host ""
    Write-Host "Installed $Version to $To\$BinName"
}
finally {
    Remove-Item -Recurse -Force $TmpDir -ErrorAction SilentlyContinue
}

# PATH hint
$UserPath = [Environment]::GetEnvironmentVariable("Path", "User")
if ($UserPath -notlike "*$To*") {
    Write-Host ""
    Write-Host "NOTE: $To is not in your PATH."
    Write-Host "Add it with:"
    Write-Host ""
    Write-Host "  [Environment]::SetEnvironmentVariable('Path', `"$To;`$env:Path`", 'User')"
    Write-Host ""
}
