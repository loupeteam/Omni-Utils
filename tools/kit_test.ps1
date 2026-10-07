<#
.SYNOPSIS
Run the Kit tests of loupe.simulation.bridge through omni.kit.test.

.DESCRIPTION
Starts an empty Kit app from a kit-app-template build with omni.kit.test
enabled and lets it run the extension's test suite (the [[test]] section of
extension.toml) in its own process. omni.kit.test comes from the build's
extension cache or the registry. Exits with omni.kit.test's code: 0 when every
test passed.

Kit is run from a temp folder: it puts its working directory on sys.path, and
from the repo root the bare plc_bridge\ folder would import as an empty
namespace package ahead of the installed one.

.EXAMPLE
tools\kit_test.ps1 -Kit D:\kit-app-template\_build\windows-x86_64\release
#>
[CmdletBinding()]
param(
    # Folder holding kit\kit.exe (a kit-app-template _build\<platform>\release).
    [string]$Kit = $env:FIXCHECK_KIT_ROOT,
    # Extension folder (default: this repo's exts\).
    [string]$Exts = $env:FIXCHECK_EXTS,
    # Where to keep Kit's output (default: kit_test.log in the current folder).
    [string]$Log = "kit_test.log"
)
$ErrorActionPreference = "Stop"
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
if (-not $Kit) { throw "need -Kit <kit build root> or FIXCHECK_KIT_ROOT" }
if (-not $Exts) { $Exts = Join-Path $repo "exts" }
if (-not [System.IO.Path]::IsPathRooted($Log)) { $Log = Join-Path (Get-Location).Path $Log }
function Native([string]$p) { (Resolve-Path $p).Path -replace "\\", "/" }
$Kit = Native $Kit
$Exts = Native $Exts

$work = Join-Path ([System.IO.Path]::GetTempPath()) ("kittest-" + [System.IO.Path]::GetRandomFileName())
New-Item -ItemType Directory -Path $work | Out-Null
Push-Location $work
try {
    & "$Kit/kit/kit.exe" --empty --enable omni.kit.test `
        --ext-folder "$Kit/exts" --ext-folder "$Kit/extscache" --ext-folder "$Kit/apps" --ext-folder $Exts `
        --/app/extensions/registryEnabled=true `
        --/exts/omni.kit.test/testExts/0="loupe.simulation.bridge" `
        --/exts/omni.kit.test/runTestsAndQuit=true --no-window *> $Log
    $code = $LASTEXITCODE
}
finally {
    Pop-Location
    Remove-Item -Recurse -Force $work -ErrorAction SilentlyContinue
}
Get-Content $Log | Select-String -Pattern "^\|\| (test_|Ran |OK|FAILED)|^\[\s*(pass|fail)|Failing tests|^\[ERROR\]|^\[OK\]"
Write-Host "kit exit code $code (0 = all tests passed); log $Log"
exit $code
