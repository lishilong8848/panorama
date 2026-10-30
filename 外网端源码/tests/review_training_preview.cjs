// Isolated review UI fixture: no production service or Feishu requests.
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const root = path.resolve(__dirname, '../web/frontend/dist/assets');
const columns = [{key:'B',label:'培训项目'},{key:'D',label:'培训时间段'},{key:'E',label:'培训方式'},{key:'F',label:'培训讲师'},{key:'G',label:'培训参与人'}];
const document = {
  fixed_blocks: [
    {id:'people',title:'班次人员',fields:[{cell:'C3',label:'值班人员',value:'测试甲、测试乙'},{cell:'G3',label:'接班人员',value:'测试丙、测试丁'},{cell:'B4',label:'长白岗',value:'测试主管'},{cell:'F4',label:'备注',value:''}]},
    {id:'metrics_summary',title:'运行参数',fields:['干球温度','湿球温度','蓄水池后备时间','总机柜数量','上电机柜总量','柴油后备时间'].map((label,i)=>({cell:['B7','D7','D8','B13','D13','F15'][i],label,value:String(20+i)}))},
  ],
  sections: [{name:'培训管理',columns,rows:[{row_id:'manual-1',cells:{B:'已保存的手工培训',D:'17：30-18：00',E:'理论',F:'测试讲师',G:'测试甲、测试乙'}}]}],
  capacity_room_inputs:{title:'机柜数量与空调启动台数',rows:['M1','M2','M3','M4','M5','M6','203','303','403'].map((room,i)=>({room,label:room.startsWith('M')?room:`挖潜机房${room}`,row:69+i*10,total_cell:`Z${69+i*10}`,powered_cell:`AA${69+i*10}`,aircon_cell:`AC${69+i*10}`,total_cabinets:'120',powered_cabinets:'84',aircon_started:'6'}))},
  footer_blocks:[],cooling_pump_pressures:{rows:[],tanks:[],primary_rows:[],secondary_rows:[]},
};
const server = http.createServer(async (req,res)=>{
  const url = new URL(req.url,'http://localhost');
  if (url.pathname.startsWith('/assets/')) {
    const file = path.resolve(root,url.pathname.slice(8));
    if (!file.startsWith(root+path.sep)||!fs.existsSync(file)) {res.writeHead(404);return res.end();}
    const type={'.js':'text/javascript','.css':'text/css','.png':'image/png','.svg':'image/svg+xml'}[path.extname(file)]||'application/octet-stream';
    res.writeHead(200,{'Content-Type':type});return fs.createReadStream(file).pipe(res);
  }
  if (!url.pathname.startsWith('/api/')) {
    res.writeHead(200,{'Content-Type':'text/html; charset=utf-8'});
    return res.end('<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>审核页隔离预览</title><link rel="stylesheet" href="/assets/style.css"><div id="app"></div><script src="/assets/vue.global.prod.js"></script><script type="module">import{mountHandoverReviewApp}from"/assets/handover_review_app.js";mountHandoverReviewApp(window.Vue)</script></html>');
  }
  let body='';for await(const chunk of req) body+=chunk;
  let input={};try{input=JSON.parse(body||'{}')}catch{}
  if (input.document) Object.assign(document, input.document);
  const dutyDate=url.searchParams.get('duty_date')||input.duty_date||'2026-10-27';
  const shift=url.searchParams.get('duty_shift')||input.duty_shift||'day';
  const code=url.pathname.includes('/b/')?'B':'A';
  const id=`${code}楼|${dutyDate}|${shift}`;
  const session={session_id:id,building:`${code}楼`,duty_date:dutyDate,duty_shift:shift,batch_key:`${dutyDate}|${shift}`,revision:1,output_file:'fixture.xlsx'};
  let result={ok:true,building:`${code}楼`,session,document,review_ui:{poll_interval_sec:60},document_revision:1,
    concurrency:{client_holds_lock:true},history:{latest_session_id:'A楼|2026-10-27|day',selected_session_id:id,selected_is_latest:dutyDate==='2026-10-27',sessions:[]}};
  if(url.pathname.includes('events/refresh'))result={ok:true,session_id:input.session_id,sections:{'培训管理':[{row_id:'training:fixture',cells:{B:'新增培训',D:'17：30-18：00',E:'理论',F:'接口讲师',G:'测试甲、测试乙、测试丙、测试丁'}}]}};
  if(url.pathname.includes('station-h')) result={ok:true,historical:dutyDate!=='2026-10-27',saved:true,batch:{duty_date:dutyDate,duty_shift:shift,batch_key:`${dutyDate}|${shift}`},selection:{current_people_text:'测试甲、测试乙',next_people_text:'测试丙、测试丁',long_day_people_text:'测试主管'},options:{},training_rows:document.sections[0].rows,duty_focus:{rows:[],checks:{},signatures:{},signature_directory:{people:[]}}};
  res.writeHead(200,{'Content-Type':'application/json; charset=utf-8'});res.end(JSON.stringify(result));
});
server.listen(0,'127.0.0.1',()=>console.log(`PREVIEW_URL=http://127.0.0.1:${server.address().port}/handover/review/a`));
