$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$TaskName = "DouyinLiveResearch"
$ScriptPath = Join-Path $ProjectRoot "scripts\run_server.ps1"
$Pwsh = (Get-Command pwsh -ErrorAction SilentlyContinue).Source
if (!$Pwsh) {
    $Pwsh = (Get-Command powershell.exe).Source
}

New-Item -ItemType Directory -Force -Path (Join-Path $ProjectRoot "data") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $ProjectRoot "logs") | Out-Null

$Action = New-ScheduledTaskAction -Execute $Pwsh -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$ScriptPath`""
$Trigger = New-ScheduledTaskTrigger -AtStartup
$Principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest

Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Principal $Principal | Out-Null

New-NetFirewallRule -DisplayName "Douyin Live Research 8791" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 8791 -ErrorAction SilentlyContinue | Out-Null

Start-ScheduledTask -TaskName $TaskName
Write-Output "installed_and_started"
