import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createMaintenanceRecurringHelpers, maintenanceValuesFromRow} from '../web/frontend/src/handover_maintenance_recurring_helpers.js';
const ref = value => ({value});
const session = ref({session_id:'A|day'});
let records = [], writes = 0, fail = false, readFail = false, allow = true, confirmed = true;
const calls = [];
globalThis.window = {confirm: () => confirmed};
const h = createMaintenanceRecurringHelpers({
  ref, session, buildingCode:'a', reviewClientId:'test', ensureEditingLock:async()=>allow,
  statusText: ref(''), isBusy:()=>false,
  getApi:async()=>{if(readFail) throw Error('读取失败');return {records};},
  saveApi:async(_building,payload)=>{
    if(fail) throw Error('保存失败');
    calls.push(payload); writes++;
    const record = {id:payload.record_id || `id${writes}`,revision:writes,mode:payload.mode,values:payload.values};
    records=records.filter(r=>r.id!==record.id).concat(record);
    return {records,record};
  },
  deleteApi:async(_building,id)=>{records=records.filter(r=>r.id!==id);return {records};},
});
const section = {name:'维护管理',columns:[{key:'F',label:'完成情况'}]};
const one = {row_id:'one',cells:{B:'巡检',C:'自维',F:'已完成',H:'甲'}};
const two = {row_id:'two',cells:{B:'设备保养',C:'厂维',F:'跟进中',H:'乙'}};
assert.deepEqual(maintenanceValuesFromRow(section,one),{maintenance_item:'巡检',maintenance_party:'自维',completion:'已完成',executor:'甲'});
assert(h.isMaintenanceSection(section));
assert(!h.isMaintenanceSection({name:'变更管理'}));
await h.hydrateMaintenanceRecurring();
assert.equal(h.maintenanceRecurringRowMode(section,one),'none');
h.setMaintenanceRecurringRowMode(section,one,'day_only');
assert.equal(h.maintenanceRecurringRowMode(section,two),'none');
await h.saveMaintenanceRecurringRow(section,one);
assert.equal(writes,1);
assert.equal(h.maintenanceRecurringRowMode(section,one),'day_only');
h.setMaintenanceRecurringRowMode(section,two,'every_shift');
await h.saveMaintenanceRecurringRow(section,two);
assert.equal(records.length,2);
assert.equal(h.maintenanceRecurringRowMode(section,one),'day_only');
assert.equal(h.maintenanceRecurringRowMode(section,two),'every_shift');
one.cells.B='巡检已更新';
await h.saveMaintenanceRecurringRow(section,one);
assert.equal(calls.at(-1).record_id,'id1');
assert.equal(calls.at(-1).values.maintenance_item,'巡检已更新');
assert.equal(records.length,2);
fail=true;
h.setMaintenanceRecurringRowMode(section,one,'every_shift');
await h.saveMaintenanceRecurringRow(section,one);
assert.match(h.maintenanceRecurringRowError(section,one),/保存失败/);
assert.equal(h.maintenanceRecurringRowError(section,two),'');
assert.equal(h.maintenanceRecurringBusy.value,false);
fail=false;
await h.loadMaintenanceRecurring();
assert.equal(h.maintenanceRecurringRowError(section,one),'');
await h.saveMaintenanceRecurringRow(section,one);
assert.equal(h.maintenanceRecurringRowMode(section,one),'every_shift');
h.setMaintenanceRecurringRowMode(section,one,'none');
one.cells.B=two.cells.B;
confirmed=false;
await h.saveMaintenanceRecurringRow(section,one);
assert.equal(records.length,2);
assert.equal(h.maintenanceRecurringRowMode(section,one),'every_shift');
confirmed=true;
h.setMaintenanceRecurringRowMode(section,one,'none');
await h.saveMaintenanceRecurringRow(section,one);
assert.equal(records.length,1);
assert.equal(h.maintenanceRecurringRowMode(section,one),'none');
assert.equal(h.maintenanceRecurringRowMode(section,two),'every_shift');
await h.loadMaintenanceRecurring();
assert.equal(h.maintenanceRecurringRowMode(section,one),'none');
assert.equal(h.maintenanceRecurringCanSave(section,one),false);
assert.equal(one.cells.F,'已完成');
h.setMaintenanceRecurringRowMode(section,two,'day_only');
session.value={session_id:'A|night'};
await h.hydrateMaintenanceRecurring();
assert.equal(h.maintenanceRecurringRowMode(section,two),'every_shift');
allow=false;
const before=writes;
await h.saveMaintenanceRecurringRow(section,two);
assert.equal(writes,before);
assert.match(h.maintenanceRecurringRowError(section,two),/班次已切换|其他终端编辑/);
readFail=true;
await h.loadMaintenanceRecurring();
assert.match(h.maintenanceRecurringRowError(section,one),/读取失败/);
assert.equal(records.length,1);
const template=readFileSync(new URL('../web/frontend/src/handover_review_template.js',import.meta.url),'utf8');
assert(!template.includes('review-maintenance-recurring-form'));
assert(!template.includes('toggleMaintenanceRecurring'));
assert(template.indexOf('本条循环填写')>template.indexOf('v-for="(row, rowIndex) in section.rows"'));
assert.equal((template.match(/review-col-action-maintenance/g) || []).length,2);
const style=readFileSync(new URL('../web/frontend/src/style.css',import.meta.url),'utf8');
assert.match(style,/\.review-col-action-maintenance \.review-row-actions \{[^}]*display: flex;/);
assert.match(style,/\.review-row-actions \{[^}]*display: grid;/);
for (const directory of ['web/frontend/dist/assets','web_frontend/dist/assets']) {
  for (const file of ['handover_review_app.js','handover_review_template.js','handover_maintenance_recurring_helpers.js','style.css']) {
    assert.equal(readFileSync(new URL(`../${directory}/${file}`,import.meta.url),'utf8'),
      readFileSync(new URL(`../web/frontend/src/${file}`,import.meta.url),'utf8'));
  }
}
console.log('per-row maintenance recurring UI checks passed');
