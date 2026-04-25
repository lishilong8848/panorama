param(
    [string]$TargetSitePackages = "\\172.16.1.2\share\程序文件存放位置\QJPT_V3 260409\QJPT_V3_code\runtime\python\Lib\site-packages",
    [switch]$DryRun,
    [switch]$NoVerify
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$BundledPython = Join-Path $ProjectRoot "runtime\python\python.exe"
$SyncScript = Join-Path $ProjectRoot "scripts\sync_pywin32_runtime.py"

if (-not (Test-Path -LiteralPath $BundledPython)) {
    throw "未找到项目自带 Python: $BundledPython"
}
if (-not (Test-Path -LiteralPath $SyncScript)) {
    throw "未找到同步脚本: $SyncScript"
}

$ArgsList = @(
    $SyncScript,
    "--target-site-packages",
    $TargetSitePackages
)
if ($DryRun) {
    $ArgsList += "--dry-run"
}
if ($NoVerify) {
    $ArgsList += "--no-verify"
}

Write-Host "[pywin32同步] 目标内网端 site-packages: $TargetSitePackages"
& $BundledPython @ArgsList
exit $LASTEXITCODE
