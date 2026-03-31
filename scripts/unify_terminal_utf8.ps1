param(
    [switch]$OnlyCurrentUser,
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message)
    Write-Host "[UTF8-SETUP] $Message"
}

function Test-IsAdmin {
    $current = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($current)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Ensure-Directory {
    param([string]$Path)
    $dir = Split-Path -Path $Path -Parent
    if (-not (Test-Path -LiteralPath $dir)) {
        if ($DryRun) {
            Write-Step "DRY-RUN: would create directory $dir"
        } else {
            New-Item -ItemType Directory -Path $dir -Force | Out-Null
        }
    }
}

function Set-CmdAutoRun {
    param(
        [string]$RegRoot
    )
    $regPath = "$RegRoot\Software\Command Processor"
    $name = "AutoRun"
    $addCmd = "chcp 65001>nul"

    $existing = ""
    try {
        $existing = (Get-ItemProperty -Path "Registry::$regPath" -Name $name -ErrorAction Stop).$name
    } catch {
        $existing = ""
    }
    $existing = [string]$existing

    if ($existing -match "chcp\s+65001") {
        Write-Step "$RegRoot CMD AutoRun already contains UTF-8 switch."
        return
    }

    $newValue = $addCmd
    if (-not [string]::IsNullOrWhiteSpace($existing)) {
        $newValue = "$existing & $addCmd"
    }

    if ($DryRun) {
        Write-Step "DRY-RUN: would set Registry::$regPath\\$name = $newValue"
        return
    }

    if (-not (Test-Path -LiteralPath "Registry::$regPath")) {
        New-Item -Path "Registry::$regPath" -Force | Out-Null
    }
    New-ItemProperty -Path "Registry::$regPath" -Name $name -PropertyType String -Value $newValue -Force | Out-Null
    Write-Step "$RegRoot CMD AutoRun configured."
}

function Set-EnvVar {
    param(
        [string]$Name,
        [string]$Value,
        [ValidateSet("User", "Machine")]
        [string]$Scope
    )
    if ($DryRun) {
        Write-Step "DRY-RUN: would set env [$Scope] $Name=$Value"
        return
    }
    [Environment]::SetEnvironmentVariable($Name, $Value, $Scope)
    Write-Step "Env [$Scope] $Name set."
}

function Upsert-ProfileBlock {
    param(
        [string]$ProfilePath
    )
    $begin = "# >>> UTF8-UNIFY START >>>"
    $end = "# <<< UTF8-UNIFY END <<<"
    $block = @(
        $begin
        "try {"
        "  [Console]::InputEncoding  = [System.Text.UTF8Encoding]::new(`$false)"
        "  [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new(`$false)"
        "  `$OutputEncoding = [Console]::OutputEncoding"
        "} catch {}"
        "try { chcp 65001 > `$null } catch {}"
        "`$env:PYTHONUTF8 = '1'"
        "`$env:PYTHONIOENCODING = 'utf-8'"
        $end
        ""
    ) -join "`r`n"

    Ensure-Directory -Path $ProfilePath

    $content = ""
    if (Test-Path -LiteralPath $ProfilePath) {
        $content = Get-Content -LiteralPath $ProfilePath -Raw -Encoding UTF8
    }

    if ($content -match [regex]::Escape($begin) -and $content -match [regex]::Escape($end)) {
        $pattern = "(?s)" + [regex]::Escape($begin) + ".*?" + [regex]::Escape($end) + "\r?\n?"
        $content = [regex]::Replace($content, $pattern, ($block -replace '\\', '\\'))
        if ($DryRun) {
            Write-Step "DRY-RUN: would update UTF8 block in $ProfilePath"
            return
        }
        Set-Content -LiteralPath $ProfilePath -Value $content -Encoding UTF8
        Write-Step "Updated profile block: $ProfilePath"
        return
    }

    if (-not [string]::IsNullOrWhiteSpace($content) -and -not $content.EndsWith("`r`n")) {
        $content += "`r`n"
    }
    $newContent = $content + $block
    if ($DryRun) {
        Write-Step "DRY-RUN: would append UTF8 block to $ProfilePath"
        return
    }
    Set-Content -LiteralPath $ProfilePath -Value $newContent -Encoding UTF8
    Write-Step "Appended profile block: $ProfilePath"
}

function Resolve-ProfileTargets {
    param([bool]$IsAdminMode)

    $docs = [Environment]::GetFolderPath("MyDocuments")
    $targets = @(
        (Join-Path $docs "WindowsPowerShell\profile.ps1"),
        (Join-Path $docs "PowerShell\profile.ps1")
    )

    if ($IsAdminMode -and -not $OnlyCurrentUser) {
        $targets += $PROFILE.AllUsersAllHosts
        if (Test-Path "C:\Program Files\PowerShell\7") {
            $targets += "C:\Program Files\PowerShell\7\profile.ps1"
        }
    }

    return $targets | Select-Object -Unique
}

function Show-Verify {
    Write-Host ""
    Write-Step "Verification (current session):"
    try {
        $cp = (chcp)
        Write-Host "  $cp"
    } catch {
        Write-Host "  chcp check failed: $($_.Exception.Message)"
    }

    try {
        $enc = python -c "import sys;print(sys.stdout.encoding)" 2>$null
        if ($LASTEXITCODE -eq 0 -and $enc) {
            Write-Host "  python stdout encoding: $enc"
        } else {
            Write-Host "  python not found or python encoding check failed."
        }
    } catch {
        Write-Host "  python check failed: $($_.Exception.Message)"
    }
}

Write-Step "Start terminal UTF-8 unification..."
$isAdmin = Test-IsAdmin
Write-Step ("Running as admin: " + $isAdmin)

# 1) CMD AutoRun (current user)
Set-CmdAutoRun -RegRoot "HKEY_CURRENT_USER"

# 2) CMD AutoRun (all users)
if (-not $OnlyCurrentUser) {
    if ($isAdmin) {
        Set-CmdAutoRun -RegRoot "HKEY_LOCAL_MACHINE"
    } else {
        Write-Step "Skip HKLM AutoRun (not admin). Re-run as admin to apply for all users."
    }
}

# 3) Environment variables
Set-EnvVar -Name "PYTHONUTF8" -Value "1" -Scope "User"
Set-EnvVar -Name "PYTHONIOENCODING" -Value "utf-8" -Scope "User"

if (-not $OnlyCurrentUser) {
    if ($isAdmin) {
        Set-EnvVar -Name "PYTHONUTF8" -Value "1" -Scope "Machine"
        Set-EnvVar -Name "PYTHONIOENCODING" -Value "utf-8" -Scope "Machine"
    } else {
        Write-Step "Skip machine env vars (not admin)."
    }
}

# 4) PowerShell profile block
$profileTargets = Resolve-ProfileTargets -IsAdminMode:$isAdmin
foreach ($pf in $profileTargets) {
    Upsert-ProfileBlock -ProfilePath $pf
}

# 5) Apply current console code page immediately
if ($DryRun) {
    Write-Step "DRY-RUN: would run chcp 65001 in current session."
} else {
    chcp 65001 > $null
}

Write-Step "Completed."
Write-Host "Close all terminal windows and reopen to take full effect."
Show-Verify

