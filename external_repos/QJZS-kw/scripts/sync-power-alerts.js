#!/usr/bin/env node

/**
 * Sync daily power-alert statistics from the Feishu Base source table into
 * the four derived statistic tables.
 *
 * Default behavior is write mode:
 *   node scripts/sync-power-alerts.js
 *
 * Safe preview:
 *   node scripts/sync-power-alerts.js --dry-run
 *
 * Override report date:
 *   node scripts/sync-power-alerts.js --date 2026/05/12
 */

const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');

const TMP_DIR = '.power-alert-sync';
const HOURS = Array.from({ length: 24 }, (_, i) => i);
const DEFAULT_CONFIG_PATH = path.join('config', 'power-alert-sync.config.json');

const args = parseArgs(process.argv.slice(2));
const CONFIG = loadConfig(args.config || process.env.POWER_ALERT_CONFIG || DEFAULT_CONFIG_PATH);
const BASE_TOKEN = process.env.FEISHU_BASE_TOKEN || CONFIG.baseToken;
const LARK_CLI = process.env.LARK_CLI || CONFIG.larkCli || 'lark-cli';
const DATA_CENTER_NAME = process.env.POWER_ALERT_DATA_CENTER || CONFIG.dataCenterName || 'EA118';
const TABLES = buildTables(CONFIG);

const FIELD_NAMES = {
  source: {
    building: '机楼',
    room: '包间',
    line: '机列',
    pdu: 'PDU编号',
    branchNo: '支路编号',
    power: hour => `功率-${hour}:00`,
  },
  branch: [
    '序号',
    '数据时间',
    '机房',
    '楼栋',
    '房间',
    'PDU编号',
    '支路号',
    '支路编号',
    '支路功率',
    '对侧PDU编号',
    '对侧支路功率',
    '采集时间点',
    '时长',
    '备注',
  ],
  cabinet: [
    '序号',
    '数据时间',
    '机房',
    '楼栋',
    '房间',
    '机柜号',
    '机柜功率',
    'PDU编号',
    '电流值',
    '是否负载不均匀',
    '次数',
    '时长',
    '备注',
  ],
  lineHead: [
    '序号',
    '数据时间',
    '机房',
    '楼栋',
    '房间',
    '机列',
    '功率',
    '对侧机列',
    '对侧机列最大功率',
    '次数',
    '时长',
    '备注',
  ],
  rowLine: [
    '序号',
    '数据时间',
    '机房',
    '楼栋',
    '房间',
    '机列',
    '功率',
    '次数',
    '时长',
    '备注',
  ],
};

const reportDate = normalizeDate(args.date || process.env.REPORT_DATE || yesterdayInShanghai());
const dryRun = Boolean(args.dryRun);

main().catch(error => {
  console.error(error.stack || error.message || String(error));
  process.exit(1);
});

async function main() {
  fs.mkdirSync(TMP_DIR, { recursive: true });

  console.log(`[sync] base=${maskSecret(BASE_TOKEN)}`);
  console.log(`[sync] reportDate=${reportDate}`);
  console.log(`[sync] mode=${dryRun ? 'dry-run' : 'write'}`);

  const sourceFields = await listFields(TABLES.source.tableId);
  const sourceFieldMap = buildFieldMap(sourceFields);
  const targetFieldMaps = {
    branch: buildFieldMap(await listFields(TABLES.branch.tableId)),
    cabinet: buildFieldMap(await listFields(TABLES.cabinet.tableId)),
    lineHead: buildFieldMap(await listFields(TABLES.lineHead.tableId)),
    rowLine: buildFieldMap(await listFields(TABLES.rowLine.tableId)),
  };

  validateFields(sourceFieldMap, targetFieldMaps);

  const sourceRows = await readSourceRows(sourceFieldMap);
  if (sourceRows.length === 0) {
    throw new Error('Source table returned 0 rows; refusing to overwrite target tables.');
  }

  const generated = generateAllTargets(sourceRows);
  printPlan(generated);

  for (const [kind, rows] of Object.entries(generated)) {
    await replaceRowsForDate(TABLES[kind], targetFieldMaps[kind], FIELD_NAMES[kind], rows);
  }

  cleanupTmp();
  console.log('[sync] done');
}

function parseArgs(argv) {
  const parsed = {};
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--dry-run') parsed.dryRun = true;
    else if (arg === '--date') parsed.date = argv[++i];
    else if (arg.startsWith('--date=')) parsed.date = arg.slice('--date='.length);
    else if (arg === '--config') parsed.config = argv[++i];
    else if (arg.startsWith('--config=')) parsed.config = arg.slice('--config='.length);
    else if (arg === '--help' || arg === '-h') {
      console.log([
        'Usage:',
        '  node scripts/sync-power-alerts.js [--dry-run] [--date YYYY/MM/DD] [--config path]',
        '',
        'Environment:',
        '  FEISHU_BASE_TOKEN       Override Base token',
        '  LARK_CLI                Override lark-cli executable',
        '  REPORT_DATE             Override report date',
        '  POWER_ALERT_DATA_CENTER Override output data center name (default EA118)',
        '  POWER_ALERT_CONFIG      Override config path',
      ].join('\n'));
      process.exit(0);
    } else {
      throw new Error(`Unknown argument: ${arg}`);
    }
  }
  return parsed;
}

function loadConfig(configPath) {
  const resolved = path.resolve(configPath);
  if (!fs.existsSync(resolved)) {
    throw new Error(
      [
        `Missing config file: ${resolved}`,
        'Create one from config/power-alert-sync.config.example.json.',
        'This file is intentionally git-ignored so public repositories do not contain Base tokens or table IDs.',
      ].join('\n'),
    );
  }
  const config = JSON.parse(fs.readFileSync(resolved, 'utf8'));
  validateConfig(config, resolved);
  return config;
}

function validateConfig(config, configPath) {
  if (!config.baseToken && !process.env.FEISHU_BASE_TOKEN) {
    throw new Error(`Config ${configPath} is missing baseToken, and FEISHU_BASE_TOKEN is not set.`);
  }
  for (const key of ['source', 'branch', 'cabinet', 'lineHead', 'rowLine']) {
    if (!config.tables?.[key]?.tableId) {
      throw new Error(`Config ${configPath} is missing tables.${key}.tableId`);
    }
  }
}

function buildTables(config) {
  const tables = config.tables;
  return {
    source: {
      name: tables.source.name || 'Source daily detail',
      tableId: tables.source.tableId,
      viewId: tables.source.viewId,
    },
    branch: {
      name: tables.branch.name || 'Branch over power',
      tableId: tables.branch.tableId,
      viewId: tables.branch.viewId,
      threshold: Number(tables.branch.threshold ?? 6.25),
    },
    cabinet: {
      name: tables.cabinet.name || 'Cabinet over power',
      tableId: tables.cabinet.tableId,
      viewId: tables.cabinet.viewId,
      threshold: Number(tables.cabinet.threshold ?? 18),
    },
    lineHead: {
      name: tables.lineHead.name || 'Line-head cabinet over power',
      tableId: tables.lineHead.tableId,
      viewId: tables.lineHead.viewId,
      threshold: Number(tables.lineHead.threshold ?? 107.5),
    },
    rowLine: {
      name: tables.rowLine.name || 'Row line over power',
      tableId: tables.rowLine.tableId,
      viewId: tables.rowLine.viewId,
      threshold: Number(tables.rowLine.threshold ?? 215),
    },
  };
}

function maskSecret(value) {
  const text = String(value || '');
  if (text.length <= 8) return '***';
  return `${text.slice(0, 4)}...${text.slice(-4)}`;
}

function yesterdayInShanghai() {
  const now = new Date();
  const shanghaiNow = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Shanghai' }));
  shanghaiNow.setDate(shanghaiNow.getDate() - 1);
  const y = shanghaiNow.getFullYear();
  const m = String(shanghaiNow.getMonth() + 1).padStart(2, '0');
  const d = String(shanghaiNow.getDate()).padStart(2, '0');
  return `${y}/${m}/${d}`;
}

function normalizeDate(value) {
  const text = String(value || '').trim().replace(/-/g, '/');
  const match = text.match(/^(\d{4})\/(\d{1,2})\/(\d{1,2})$/);
  if (!match) throw new Error(`Invalid date "${value}", expected YYYY/MM/DD`);
  return `${match[1]}/${match[2].padStart(2, '0')}/${match[3].padStart(2, '0')}`;
}

async function listFields(tableId) {
  const out = runLark([
    'base',
    '+field-list',
    '--as',
    'bot',
    '--base-token',
    BASE_TOKEN,
    '--table-id',
    tableId,
    '--limit',
    '200',
  ]);
  return parseJsonOutput(out).data.fields || [];
}

function buildFieldMap(fields) {
  const map = new Map();
  for (const field of fields) map.set(field.name, field);
  return map;
}

function validateFields(sourceFieldMap, targetFieldMaps) {
  const requiredSource = [
    FIELD_NAMES.source.building,
    FIELD_NAMES.source.room,
    FIELD_NAMES.source.line,
    FIELD_NAMES.source.pdu,
    FIELD_NAMES.source.branchNo,
    ...HOURS.map(FIELD_NAMES.source.power),
  ];
  for (const name of requiredSource) requireField(sourceFieldMap, TABLES.source.name, name);
  for (const [kind, names] of Object.entries({
    branch: FIELD_NAMES.branch,
    cabinet: FIELD_NAMES.cabinet,
    lineHead: FIELD_NAMES.lineHead,
    rowLine: FIELD_NAMES.rowLine,
  })) {
    for (const name of names) requireField(targetFieldMaps[kind], TABLES[kind].name, name);
  }
}

function requireField(fieldMap, tableName, name) {
  if (!fieldMap.has(name)) throw new Error(`${tableName} 缺少字段：${name}`);
}

async function readSourceRows(sourceFieldMap) {
  const selectedNames = [
    FIELD_NAMES.source.building,
    FIELD_NAMES.source.room,
    FIELD_NAMES.source.line,
    FIELD_NAMES.source.pdu,
    FIELD_NAMES.source.branchNo,
    ...HOURS.map(FIELD_NAMES.source.power),
  ];
  const selectedFields = selectedNames.map(name => sourceFieldMap.get(name).id);
  const records = await listRecords(TABLES.source.tableId, {
    viewId: TABLES.source.viewId,
    fieldIds: selectedFields,
  });

  return records
    .map(record => {
      const row = {};
      selectedNames.forEach((name, index) => {
        row[name] = record.values[index];
      });
      return normalizeSourceRow(row);
    })
    .filter(row => row.room && row.lineRaw && row.pdu && row.branchNo && row.line && row.pduInfo);
}

async function listRecords(tableId, { viewId, fieldIds }) {
  const all = [];
  let offset = 0;
  const limit = 200;

  while (true) {
    const args = [
      'base',
      '+record-list',
      '--as',
      'bot',
      '--base-token',
      BASE_TOKEN,
      '--table-id',
      tableId,
      '--offset',
      String(offset),
      '--limit',
      String(limit),
      '--format',
      'json',
    ];
    if (viewId) args.push('--view-id', viewId);
    for (const fieldId of fieldIds) args.push('--field-id', fieldId);

    const data = parseJsonOutput(runLark(args)).data;
    const ids = data.record_id_list || [];
    const rows = data.data || [];
    ids.forEach((recordId, index) => {
      all.push({ recordId, values: rows[index] || [] });
    });

    if (!data.has_more || ids.length === 0) break;
    offset += ids.length;
    console.log(`[read] table=${tableId} rows=${all.length}`);
  }

  return all;
}

function normalizeSourceRow(raw) {
  const building = firstSelect(raw[FIELD_NAMES.source.building]);
  const room = String(raw[FIELD_NAMES.source.room] || '').trim();
  const lineRaw = String(raw[FIELD_NAMES.source.line] || '').trim();
  const pdu = String(raw[FIELD_NAMES.source.pdu] || '').trim();
  const branchNo = String(raw[FIELD_NAMES.source.branchNo] || '').trim();
  const line = parseLine(lineRaw);
  const pduInfo = parsePdu(pdu);
  const powers = HOURS.map(hour => numberOrZero(raw[FIELD_NAMES.source.power(hour)]));

  return {
    building,
    buildingLetter: building.replace(/楼$/, ''),
    room,
    roomShort: room.replace(/包间$/, ''),
    lineRaw,
    line,
    pdu,
    pduInfo,
    branchNo,
    powers,
  };
}

function generateAllTargets(sourceRows) {
  return {
    branch: generateBranchRows(sourceRows),
    cabinet: generateCabinetRows(sourceRows),
    lineHead: generateLineHeadRows(sourceRows),
    rowLine: generateRowLineRows(sourceRows),
  };
}

function generateBranchRows(sourceRows) {
  const index = buildBranchIndex(sourceRows);
  const result = [];

  for (const row of sourceRows) {
    const stats = thresholdStats(row.powers, TABLES.branch.threshold);
    if (!stats.overCount) continue;

    const opposite = findOppositeBranch(row, index);
    const oppositePower =
      opposite && Number.isFinite(opposite.powers[stats.maxHour])
        ? fmtTrim(opposite.powers[stats.maxHour], 3)
        : null;

    result.push({
      '序号': result.length + 1,
      '数据时间': reportDate,
      '机房': row.lineRaw,
      '楼栋': row.building,
      '房间': row.room,
      'PDU编号': row.pdu,
      '支路号': row.branchNo,
      '支路编号': makeBranchCode(row.pdu, row.branchNo),
      '支路功率': fmtTrim(stats.maxValue, 3),
      '对侧PDU编号': opposite ? opposite.pdu : null,
      '对侧支路功率': oppositePower,
      '采集时间点': `${stats.maxHour}:00`,
      '时长': `${stats.overCount}h`,
      '备注': null,
    });
  }

  return result;
}

function buildBranchIndex(sourceRows) {
  const index = new Map();
  for (const row of sourceRows) {
    const key = branchKey(row.room, row.pdu, row.branchNo);
    if (!index.has(key)) index.set(key, row);
  }
  return index;
}

function findOppositeBranch(row, index) {
  const info = row.pduInfo;
  if (!info) return null;

  const oppositeSide = info.side === 'A' ? 'B' : 'A';
  const exactPdu = `${info.col}${info.numPad2}-${oppositeSide}${info.feed}`;
  const exact = index.get(branchKey(row.room, exactPdu, row.branchNo));
  if (exact) return exact;

  const complement = branchComplement(`${info.side}${info.feed}|${row.branchNo}`);
  if (!complement) return null;

  const complementPdu = `${info.col}${info.numPad2}-${complement.side}${complement.feed}`;
  return index.get(branchKey(row.room, complementPdu, complement.branchNo)) || null;
}

function branchComplement(key) {
  const map = {
    'A2|38': { side: 'B', feed: '1', branchNo: '1' },
    'B1|1': { side: 'A', feed: '2', branchNo: '38' },
    'A1|37': { side: 'B', feed: '2', branchNo: '19' },
    'B2|19': { side: 'A', feed: '1', branchNo: '37' },
  };
  return map[key];
}

function branchKey(room, pdu, branchNo) {
  return `${room}||${pdu}||${branchNo}`;
}

function generateCabinetRows(sourceRows) {
  const groups = groupBy(sourceRows, row => {
    const p = row.pduInfo;
    if (!p) return null;
    return `${row.room}||${p.col}${p.numPad2}`;
  });

  const result = [];
  for (const group of groups.values()) {
    const stats = thresholdStats(sumByHour(group), TABLES.cabinet.threshold);
    if (!stats.overCount) continue;

    const first = group[0];
    const p = first.pduInfo;
    const sorted = [...group].sort(comparePduRows);

    for (const item of sorted) {
      result.push({
        '序号': String(result.length + 1),
        '数据时间': reportDate,
        '机房': `${first.roomShort}-${p.col}列`,
        '楼栋': first.building,
        '房间': first.room,
        '机柜号': `${p.col}列${p.col}${p.numPad2}`,
        '机柜功率': `${fmtTrim(stats.maxValue, 2)}kw`,
        'PDU编号': item.pdu,
        '电流值': Number(fmtTrim(item.powers[stats.maxHour], 3)),
        '是否负载不均匀': '均匀',
        '次数': stats.runs,
        '时长': `${stats.overCount}h`,
        '备注': null,
      });
    }
  }
  return result;
}

function generateLineHeadRows(sourceRows) {
  const groups = groupBy(sourceRows, row => row.lineRaw);
  const groupStats = new Map();
  for (const [key, group] of groups.entries()) {
    groupStats.set(key, {
      group,
      totals: sumByHour(group),
    });
  }

  const result = [];
  for (const [key, data] of groupStats.entries()) {
    const stats = thresholdStats(data.totals, TABLES.lineHead.threshold);
    if (!stats.overCount) continue;

    const first = data.group[0];
    const oppositeKey = oppositeLineRaw(first.lineRaw);
    const opposite = oppositeKey ? groupStats.get(oppositeKey) : null;
    const oppositeMax = opposite ? maxOf(opposite.totals) : null;

    result.push({
      '序号': result.length + 1,
      '数据时间': reportDate,
      '机房': DATA_CENTER_NAME,
      '楼栋': first.building,
      '房间': `${first.roomShort}.${DATA_CENTER_NAME}`,
      '机列': lineDisplay(first.line),
      '功率': `${fmtTrim(stats.maxValue, 3)}kw`,
      '对侧机列': opposite ? lineDisplay(opposite.group[0].line) : null,
      '对侧机列最大功率': opposite ? `${fmtTrim(oppositeMax, 3)}kw` : null,
      '次数': stats.runs,
      '时长': `${stats.overCount}h`,
      '备注': null,
    });
  }
  return result;
}

function generateRowLineRows(sourceRows) {
  const groups = groupBy(sourceRows, row => {
    if (!row.line) return null;
    return `${row.room}||${row.line.col}`;
  });

  const result = [];
  for (const group of groups.values()) {
    const stats = thresholdStats(sumByHour(group), TABLES.rowLine.threshold);
    if (!stats.overCount) continue;

    const first = group[0];
    result.push({
      '序号': result.length + 1,
      '数据时间': reportDate,
      '机房': DATA_CENTER_NAME,
      '楼栋': first.building,
      '房间': `${first.roomShort}.${DATA_CENTER_NAME}`,
      '机列': `${first.line.col}列`,
      '功率': `${fmtTrim(stats.maxValue, 3)}KW`,
      '次数': stats.runs,
      '时长': `${stats.overCount}h`,
      '备注': null,
    });
  }
  return result;
}

async function replaceRowsForDate(table, fieldMap, fieldNames, rows) {
  const dateField = fieldMap.get('数据时间');
  if (!dateField) throw new Error(`${table.name} 缺少 数据时间 字段`);

  const existing = await listRecords(table.tableId, { fieldIds: [dateField.id] });
  const toDelete = existing.filter(record => normalizeLooseDate(record.values[0]) === reportDate);
  console.log(
    `[target] ${table.name}: generated=${rows.length}, sameDateExisting=${toDelete.length}, action=${
      dryRun ? 'preview' : 'replace'
    }`,
  );

  if (dryRun) {
    console.log(`[target] ${table.name}: sample=${JSON.stringify(rows.slice(0, 3), null, 2)}`);
    return;
  }

  for (const record of toDelete) {
    runLark([
      'base',
      '+record-delete',
      '--as',
      'bot',
      '--base-token',
      BASE_TOKEN,
      '--table-id',
      table.tableId,
      '--record-id',
      record.recordId,
      '--yes',
    ]);
  }

  await batchCreate(table, fieldMap, fieldNames, rows);
}

async function batchCreate(table, fieldMap, fieldNames, rows) {
  const fieldIds = fieldNames.map(name => fieldMap.get(name).id);
  for (let offset = 0; offset < rows.length; offset += 200) {
    const chunk = rows.slice(offset, offset + 200);
    if (chunk.length === 0) continue;

    const body = {
      fields: fieldIds,
      rows: chunk.map(row => fieldNames.map(name => row[name] ?? null)),
    };
    const fileName = `${table.tableId}_${String(offset).padStart(5, '0')}.json`;
    const filePath = path.join(TMP_DIR, fileName);
    writeAsciiJson(filePath, body);

    runLark([
      'base',
      '+record-batch-create',
      '--as',
      'bot',
      '--base-token',
      BASE_TOKEN,
      '--table-id',
      table.tableId,
      '--json',
      `@${filePath.replace(/\\/g, '/')}`,
    ]);
  }
}

function printPlan(generated) {
  for (const [kind, rows] of Object.entries(generated)) {
    console.log(`[generate] ${TABLES[kind].name}: ${rows.length} rows`);
  }
}

function runLark(args) {
  try {
    return execFileSync(LARK_CLI, args, {
      encoding: 'utf8',
      shell: true,
      cwd: process.cwd(),
      stdio: ['ignore', 'pipe', 'pipe'],
      maxBuffer: 1024 * 1024 * 64,
    }).toString();
  } catch (error) {
    const stdout = error.stdout ? error.stdout.toString() : '';
    const stderr = error.stderr ? error.stderr.toString() : '';
    throw new Error(`lark-cli failed: ${args.join(' ')}\nSTDOUT:\n${stdout}\nSTDERR:\n${stderr}`);
  }
}

function parseJsonOutput(text) {
  const start = text.indexOf('{');
  const end = text.lastIndexOf('}');
  if (start < 0 || end < start) throw new Error(`No JSON object found in output:\n${text}`);
  return JSON.parse(text.slice(start, end + 1));
}

function writeAsciiJson(filePath, value) {
  const json = JSON.stringify(value).replace(/[^\x00-\x7F]/g, char => {
    return `\\u${char.charCodeAt(0).toString(16).padStart(4, '0')}`;
  });
  fs.writeFileSync(filePath, json, 'utf8');
}

function cleanupTmp() {
  if (!fs.existsSync(TMP_DIR)) return;
  fs.rmSync(TMP_DIR, { recursive: true, force: true });
}

function firstSelect(value) {
  if (Array.isArray(value)) return String(value[0] || '').trim();
  return String(value || '').trim();
}

function numberOrZero(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : 0;
}

function parseLine(value) {
  const match = String(value || '').trim().match(/^(.+)-([A-Z])列-(AC|DC)(\d+)$/i);
  if (!match) return null;
  return {
    roomShort: match[1],
    col: match[2].toUpperCase(),
    type: match[3].toUpperCase(),
    num: match[4].padStart(3, '0'),
  };
}

function parsePdu(value) {
  const match = String(value || '').trim().match(/^([A-Z])0*(\d+)-([AB])([12])$/i);
  if (!match) return null;
  return {
    col: match[1].toUpperCase(),
    num: Number(match[2]),
    numPad2: String(Number(match[2])).padStart(2, '0'),
    side: match[3].toUpperCase(),
    feed: match[4],
  };
}

function lineDisplay(line) {
  return `${line.col}列-${line.type}${line.num}`;
}

function oppositeLineRaw(lineRaw) {
  const line = parseLine(lineRaw);
  if (!line) return null;
  const oppositeType = line.type === 'AC' ? 'DC' : 'AC';
  return `${line.roomShort}-${line.col}列-${oppositeType}${line.num}`;
}

function makeBranchCode(pdu, branchNo) {
  const p = parsePdu(pdu);
  if (!p || !branchNo) return null;
  const type = p.side === 'A' ? 'AC' : 'DC';
  return `${p.col}列-${type}${String(p.num).padStart(3, '0')} #${String(branchNo).trim()}`;
}

function thresholdStats(values, threshold) {
  let overCount = 0;
  let runs = 0;
  let wasOver = false;
  let maxValue = -Infinity;
  let maxHour = 0;

  values.forEach((value, hour) => {
    const over = value > threshold;
    if (over) {
      overCount += 1;
      if (!wasOver) runs += 1;
      if (value >= maxValue) {
        maxValue = value;
        maxHour = hour;
      }
    }
    wasOver = over;
  });

  return {
    overCount,
    runs,
    maxValue: overCount ? maxValue : maxOf(values),
    maxHour,
  };
}

function sumByHour(rows) {
  return HOURS.map(hour => rows.reduce((sum, row) => sum + row.powers[hour], 0));
}

function maxOf(values) {
  return values.reduce((max, value) => (value > max ? value : max), -Infinity);
}

function groupBy(rows, keyFn) {
  const groups = new Map();
  for (const row of rows) {
    const key = keyFn(row);
    if (!key) continue;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }
  return groups;
}

function comparePduRows(a, b) {
  const aa = a.pduInfo;
  const bb = b.pduInfo;
  if (!aa || !bb) return a.pdu.localeCompare(b.pdu);
  if (aa.side !== bb.side) return aa.side.localeCompare(bb.side);
  return Number(aa.feed) - Number(bb.feed);
}

function fmtTrim(value, digits) {
  if (!Number.isFinite(value)) return '';
  return Number(value.toFixed(digits)).toString();
}

function normalizeLooseDate(value) {
  if (value === null || value === undefined || value === '') return '';
  try {
    return normalizeDate(String(value).slice(0, 10));
  } catch (_) {
    return String(value).trim();
  }
}
