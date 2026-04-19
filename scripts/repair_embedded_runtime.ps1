param(
    [string]$ProjectRoot = "",
    [string]$TargetRoot = "",
    [string]$PythonVersion = "3.11.9"
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

function Write-Log {
    param([string]$Text)
    Write-Output $Text
}

function Resolve-ArchTag {
    $arch = [Environment]::GetEnvironmentVariable("PROCESSOR_ARCHITECTURE", "Process")
    if (-not $arch) {
        return "amd64"
    }
    $normalized = $arch.Trim().ToLowerInvariant()
    if ($normalized -eq "arm64") {
        return "arm64"
    }
    return "amd64"
}

function Ensure-Directory {
    param([string]$PathValue)
    if (-not (Test-Path -LiteralPath $PathValue)) {
        New-Item -Path $PathValue -ItemType Directory -Force | Out-Null
    }
}

if (-not $ProjectRoot) {
    $ProjectRoot = Split-Path -Parent $PSScriptRoot
}
if (-not $TargetRoot) {
    $TargetRoot = Join-Path $ProjectRoot "runtime\python"
}

$ProjectRoot = [System.IO.Path]::GetFullPath($ProjectRoot)
$TargetRoot = [System.IO.Path]::GetFullPath($TargetRoot)
$tempRoot = Join-Path $ProjectRoot ".runtime_repair_tmp"
$downloadPath = Join-Path $tempRoot "python-embed.zip"
$extractRoot = Join-Path $tempRoot "extract"
$archTag = Resolve-ArchTag
$embedUrl = "https://www.python.org/ftp/python/$PythonVersion/python-$PythonVersion-embed-$archTag.zip"

Write-Log "[runtime-repair] preparing runtime/python"
Write-Log "[runtime-repair] python_version=$PythonVersion arch=$archTag"
Write-Log "[runtime-repair] download_url=$embedUrl"

Ensure-Directory $tempRoot
if (Test-Path -LiteralPath $downloadPath) {
    Remove-Item -LiteralPath $downloadPath -Force -ErrorAction SilentlyContinue
}
if (Test-Path -LiteralPath $extractRoot) {
    Remove-Item -LiteralPath $extractRoot -Recurse -Force -ErrorAction SilentlyContinue
}

$ProgressPreference = "SilentlyContinue"
Invoke-WebRequest -Uri $embedUrl -OutFile $downloadPath -UseBasicParsing

Expand-Archive -LiteralPath $downloadPath -DestinationPath $extractRoot -Force

if (Test-Path -LiteralPath $TargetRoot) {
    Remove-Item -LiteralPath $TargetRoot -Recurse -Force -ErrorAction Stop
}
New-Item -Path $TargetRoot -ItemType Directory -Force | Out-Null
Copy-Item -Path (Join-Path $extractRoot "*") -Destination $TargetRoot -Recurse -Force

$pthFile = Get-ChildItem -LiteralPath $TargetRoot -Filter "python*._pth" | Select-Object -First 1
if ($null -ne $pthFile) {
    $lines = Get-Content -LiteralPath $pthFile.FullName -Encoding UTF8
    $updated = @()
    $hasDot = $false
    foreach ($line in $lines) {
        $trimmed = $line.Trim()
        if ($trimmed -eq ".") {
            $hasDot = $true
            $updated += "."
            continue
        }
        if ($trimmed -eq "#import site") {
            $updated += "import site"
            continue
        }
        $updated += $line
    }
    if (-not $hasDot) {
        $updated += "."
    }
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllLines($pthFile.FullName, $updated, $utf8NoBom)
}

$sitePackages = Join-Path $TargetRoot "Lib\site-packages"
Ensure-Directory $sitePackages

$metaPath = Join-Path $TargetRoot ".qjpt_runtime.json"
$meta = @{
    prepared_at = (Get-Date).ToString("yyyy-MM-ddTHH:mm:sszzz")
    source = "downloaded_embeddable"
    python_version = $PythonVersion
    arch = $archTag
    download_url = $embedUrl
}
$meta | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $metaPath -Encoding UTF8

$pythonExe = Join-Path $TargetRoot "python.exe"
if (-not (Test-Path -LiteralPath $pythonExe)) {
    throw "repaired runtime/python is missing python.exe"
}

& $pythonExe -c "import encodings, json, sqlite3, ssl, sys; print(sys.version)"
if ($LASTEXITCODE -ne 0) {
    throw "repaired runtime/python health probe failed"
}

Write-Log "[runtime-repair] runtime/python repair completed"
exit 0
