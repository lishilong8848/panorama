# 交接班日志模块（handover_log_module）

## 说明
该模块用于在内网下载“交接班日志”报表，并将数据填充到模板文件，输出本地 xlsx。  
模块与月报主流程解耦，可单独调用。

## 默认下载策略
1. 时间窗口：当前时间前 10 分钟到当前时间。  
2. 查询刻度：`5分钟`。  
3. 导出方式：查询后执行`原样导出`。  
4. 稳定性策略：
- 登录态自适应（已登录时跳过账号密码填充）
- 每任务重建 iframe 链路
- 查询/导出超时后刷新页面并重试

## 主要入口
- `handover_log_module.api.facade.run_from_existing_file(...)`
- `handover_log_module.api.facade.run_from_download(...)`

## 配置来源
优先读取主配置中的 `handover_log`，缺省时使用：
- `handover_log_module/config/handover_default.json`

## 本地调试
```powershell
python handover_log_module/scripts/run_handover_local.py --mode from-download --config 表格计算配置.json --buildings C楼
```

