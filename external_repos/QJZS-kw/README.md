# QJZS Power Alert Sync

Daily Feishu Base synchronization for power-alert statistics.

The program reads the daily source table, calculates the four derived reports, and replaces only records whose `数据时间` equals the current report date. Older dates are left untouched.

## Reports

- Single branch over `6.25KW`
- Cabinet over `18KW`
- Line-head cabinet over `107.5KW`
- Row line over `215KW`

## Setup

1. Install and authenticate `lark-cli`.
2. Copy the example config:

```powershell
Copy-Item config\power-alert-sync.config.example.json config\power-alert-sync.config.json
```

3. Fill `config/power-alert-sync.config.json` with your Base token, table IDs, and view IDs.

The real config file is ignored by git. Do not commit Base tokens, table IDs, view IDs, exported data, or `.env` files to a public repository.

## Usage

Preview without writing:

```powershell
node scripts\sync-power-alerts.js --dry-run
```

Run for yesterday in Asia/Shanghai:

```powershell
node scripts\sync-power-alerts.js
```

Run for a specific data date:

```powershell
node scripts\sync-power-alerts.js --date 2026/05/12
```

Use an alternate config path:

```powershell
node scripts\sync-power-alerts.js --config path\to\config.json
```

## Data Rules

- `数据时间` defaults to the previous calendar day in `Asia/Shanghai`.
- If records for the same `数据时间` already exist in a target table, only that date is deleted and regenerated.
- Records from other dates are preserved.
- `楼栋` is written as `A楼/B楼/C楼`.
- `序号` is generated in order for the newly generated date.
- Continuous over-threshold hours count as one `次数`; separated over-threshold runs count separately.
- `时长` is the total count of over-threshold hourly points, formatted as `Nh`.
- `支路编号` uses the format `X列-AC/DC### #支路号`.

## Safety

This repository intentionally contains only the synchronization logic and a placeholder config. Secrets and customer-specific Base identifiers must stay in local ignored config or environment variables.
