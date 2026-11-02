// Local-only fixture. No production configuration, credentials, downloads or cloud writes.
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const root = path.resolve(__dirname, '../web/frontend/dist/assets');
const columns = [{key:'B',label:'维护总项'},{key:'C',label:'维护执行方'},{key:'D',label:'完成情况'},{key:'H',label:'执行人'}];
const document = {
  fixed_blocks:[], footer_blocks:[],
  sections:[{name:'维护管理', columns, rows:[
    {row_id:'one', cells:{B:'每日机房设备巡检',C:'自维',D:'已完成',H:'测试甲、测试乙'}},
    {row_id:'two', cells:{B:'配电系统运行检查及异常跟踪',C:'自维',D:'已完成',H:'测试丙'}},
  ]},{name:'变更管理',columns,rows:[]}],
};
let nextRecord = 2;
let records = [{id:'fixture-1',revision:1,mode:'day_only',values:{maintenance_item:'每日机房设备巡检',maintenance_party:'自维',completion:'已完成',executor:'测试甲、测试乙'}}];
const server = http.createServer(async(req,res)=>{
  const url = new URL(req.url,'http://localhost');
  if(url.pathname.startsWith('/assets/')) {
    const file=path.resolve(root,url.pathname.slice(8));
    if(!file.startsWith(root+path.sep)||!fs.existsSync(file)||!fs.statSync(file).isFile()){res.writeHead(404);return res.end();}
    res.writeHead(200,{'Content-Type':{'.js':'text/javascript','.css':'text/css','.png':'image/png','.svg':'image/svg+xml'}[path.extname(file)]||'application/octet-stream'});
    return fs.createReadStream(file).pipe(res);
  }
  if(!url.pathname.startsWith('/api/')) {
    res.writeHead(200,{'Content-Type':'text/html; charset=utf-8'});
    return res.end(`<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>维护循环填写隔离预览</title><link rel="stylesheet" href="/assets/style.css"><div id="app"></div><script src="/assets/vue.global.prod.js"></script><script type="module">
      import{mountHandoverReviewApp}from"/assets/handover_review_app.js";mountHandoverReviewApp(window.Vue);
      if(location.search.includes('focus=1')) {
        let tries=0;
        const timer=setInterval(()=>{
          const control=document.querySelector('.review-maintenance-row-repeat');
          if(control) {
            clearInterval(timer);
            const height=document.querySelector('.review-header-sticky')?.getBoundingClientRect().height||0;
            control.style.scrollMarginTop=(height+24)+'px';
            control.scrollIntoView({block:'start',inline:'end'});
          }
          else if(++tries>50) clearInterval(timer);
        },100);
      }
    </script></html>`);
  }
  let raw=''; for await(const chunk of req) raw+=chunk;
  const body=JSON.parse(raw||'{}');
  if(body.document) Object.assign(document,body.document);
  const session={session_id:'A楼|2026-11-02|day',building:'A楼',duty_date:'2026-11-02',duty_shift:'day',batch_key:'2026-11-02|day',revision:1,output_file:'fixture.xlsx'};
  let result={ok:true,building:'A楼',session,document,document_revision:1,review_ui:{poll_interval_sec:60},
    concurrency:{client_holds_lock:true},history:{latest_session_id:session.session_id,selected_session_id:session.session_id,selected_is_latest:true,sessions:[]}};
  if(url.pathname.includes('maintenance-recurring')) {
    if(req.method==='POST') {
      const id=body.record_id||`fixture-${nextRecord++}`;
      const record={id,revision:1,mode:body.mode,values:body.values};
      records=records.filter(r=>r.id!==id).concat(record);
    }
    if(req.method==='DELETE') records=records.filter(r=>r.id!==url.pathname.split('/').pop());
    result={ok:true,records};
  }
  res.writeHead(200,{'Content-Type':'application/json; charset=utf-8'});
  res.end(JSON.stringify(result));
});
server.listen(0,'127.0.0.1',()=>console.log(`PREVIEW_URL=http://127.0.0.1:${server.address().port}/handover/review/a`));
