# 天气及室外干湿球温度查询接口

## 1. 地址与用途

请求方式：`GET`，响应格式：`application/json`。

```text
http://192.168.224.157:18765/api/handover/review/environment
```

主机和端口应替换为实际外网端服务地址。需要更新、重启外网端并进入外网端角色；内网端不需要为此更新。

此接口只查询数据，不触发内网补采，不扫描共享目录，不生成文件、不上传飞书、不发送消息、不修改审核内容。
接口不包含人员、签名、配置凭证或文件路径。

当前复用审核页访问规则，不需要飞书 Token、App Secret、client_id 或 session_id。它不是带鉴权的公网 API，只应通过受信任的局域网/VPN访问；对公网开放前应在网关增加鉴权、HTTPS及限流。
建议由其他程序的后端调用。不同域名的浏览器前端直接调用涉及 CORS，本次未新增跨域放行。

## 2. 查询参数

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| duty_date | string | `YYYY-MM-DD`，班次开始日期，必须是有效日期且可计算次日 |
| duty_shift | string | `day` 白班，`night` 夜班 |

两项参数同时填写，或同时省略。只填写一项返回 HTTP 400。无需填写楼栋，接口读取该批次 A-E 楼共用的室外温度。

同时省略时，按服务端本地时间确定当前班次，服务器时区应设置为中国标准时间：

| 请求时间 | 归属日期 | 班次 |
| --- | --- | --- |
| 00:00至09:00之前 | 前一天 | night |
| 09:00至18:00之前 | 当天 | day |
| 18:00及之后 | 当天 | night |

夜班日期属于开始夜班的那一天。例如9月4日02:00仍属于9月3日夜班，不应传9月4日夜班。

查询2026年9月3日白班，窗口为9月3日09:00至18:00：

```text
http://192.168.224.157:18765/api/handover/review/environment?duty_date=2026-09-03&duty_shift=day
```

查询2026年9月3日夜班，窗口为9月3日18:00至9月4日09:00：

```text
http://192.168.224.157:18765/api/handover/review/environment?duty_date=2026-09-03&duty_shift=night
```

## 3. 返回示例

以下是结构示例，不是实际查询结果。

```json
{
  "ok": true,
  "status": "success",
  "duty_date": "2026-09-03",
  "duty_shift": "day",
  "batch_key": "2026-09-03|day",
  "timezone": "Asia/Shanghai",
  "data": {
    "weather": "多云",
    "dry_bulb_temperature": 27.7,
    "wet_bulb_temperature": 25.8,
    "relative_humidity": 85.9
  },
  "units": {
    "dry_bulb_temperature": "℃",
    "wet_bulb_temperature": "℃",
    "relative_humidity": "%"
  },
  "sources": {
    "temperature": {
      "type": "handover_shared_block",
      "updated_at": "2026-09-03 12:00:00",
      "revision": 1
    },
    "weather": {
      "type": "open_meteo_hourly_shift_summary",
      "fetched_at": "2026-09-03 12:10:00",
      "window_start": "2026-09-03 09:00:00",
      "window_end": "2026-09-03 18:00:00"
    },
    "humidity": {
      "type": "calculated_from_dry_wet",
      "pressure_hpa": 1013.25
    }
  },
  "warnings": []
}
```

| 数据字段 | 类型 | 含义 |
| --- | --- | --- |
| data.weather | string/null | 对应班次的天气概况，如晴、多云、阴、小雨 |
| data.dry_bulb_temperature | number/null | 已保存的室外干球温度，摄氏度 |
| data.wet_bulb_temperature | number/null | 已保存的室外湿球温度，摄氏度 |
| data.relative_humidity | number/null | 通过同组干湿球计算的相对湿度，0至100，保留两位小数；85.9代表85.9% |

数值字段没有单位后缀，单位单独放在 `units`。小数末尾的0可能被JSON省略。

## 4. 数据口径与缺值

- 干湿球温度从本地SQLite中该日期、班次的 `outdoor_temperature` 共享块读取，分别对应B7/D7。它是交接班保存值，不是请求时重新采集的传感器值。
- 审核页保存共享温度后，下次查询直接使用新值，不需要确认上传云文档。温度不使用天气缓存。
- 若该批次从未保存共享温度，返回 `null`，不借用其他班次或其他日期的数据，也不回读Excel。旧历史批次没有共享块时同样如此。
- `sources.temperature.updated_at` 是共享温度保存时间，不是传感器采样时间；`revision` 是共享块版本，不是整个审核页版本。
- 湿度沿用容量表的干湿球计算公式，按标准气压1013.25hPa估算，不是湿度传感器实测值。湿球大于干球或无法计算时，湿度返回 `null`。
- 天气复用项目Open-Meteo逐小时取数与天气代码规则、暖通天气位置配置；未配置位置时使用项目默认经纬度31.94/120.98。
- 天气是对应班次逐小时数据的概况，不是某一秒的实时天气，也不保证与过去已生成的容量表天气文字完全一致。天气窗口左闭右开，白班09:00至17:00共9个小时点，夜班18:00至次日08:00共15个小时点。
- 天气仅使用上游最近2天及2天预报范围内可覆盖该班次的小时数据。完整窗口不可用时返回 `null`，不使用今天或相邻日期天气代替历史数据。本接口不提供长期历史天气归档。
- `sources.weather.fetched_at` 是天气数据请求完成时间，不是观测时间；即使窗口不匹配，它仍可能非空，是否可用应以 `data.weather` 为准。

## 5. 状态与错误处理

HTTP 200只代表请求处理完成，接收端仍需检查 `status` 和各数据字段：

| status | 含义 |
| --- | --- |
| success | 四项数据均可用 |
| partial | 部分数据可用，其余为null |
| unavailable | 四项数据均不可用 |

以上状态的 `ok` 都是 `true`。`ok` 不代表所有数据齐全。缺值不能当0处理，不能把旧值冒充新值。

| warnings中的代码 | 含义 |
| --- | --- |
| temperature_missing_or_invalid | 该批次干球或湿球缺失、不是有效数字 |
| temperature_store_unavailable | 本地温度状态库暂时不可读 |
| wet_bulb_exceeds_dry_bulb | 湿球大于干球，保留原温度供排查，不计算湿度 |
| humidity_calculation_failed | 干湿球无法代入现有公式计算有效湿度 |
| weather_disabled | 项目暖通天气查询配置未启用 |
| weather_refreshing | 另一请求正在刷新天气，本次不等待，不重复请求上游 |
| weather_unavailable | 上游天气超时、失败或无数据 |
| weather_window_unavailable | 上游天气不能完整覆盖目标班次 |

HTTP 400表示参数错误，响应例如 `{"detail":"请同时提供 duty_date 和 duty_shift，班次仅支持 day/night"}`。
HTTP 409通常表示未进入外网端角色或当前运行在内网角色；查看 `detail`，先修正部署状态。
HTTP 404可能表示尚未更新/重启到包含本接口的版本，或请求地址错误。
HTTP 503及连接超时可短暂等待后有限重试；错误响应可能不是JSON，应先判断HTTP状态。

## 6. 缓存与调用建议

天气成功请求缓存5分钟，失败缓存60秒。同一进程最多执行一个天气刷新，其他同时到达的请求会返回已有可用天气缓存，或返回 `weather_refreshing`，不会排队等待另一个天气请求。
上游天气请求的连接/读取超时参数设为5秒；这不是整个接口严格5秒的完成保证。
温度每次读取已保存状态。响应带 `Cache-Control: no-store`，防止HTTP缓存遮蔽用户刚保存的温度。

建议按需调用或间隔1至5分钟轮询；连接超时设为5秒、读取超时设为15秒。网络异常可间隔30秒、60秒有限重试。
`weather_refreshing` 可60秒后再查。参数错误、长期历史天气缺失不要无限重试。

Python调用示例（调用方需已安装requests）：

```python
import requests

response = requests.get(
    "http://192.168.224.157:18765/api/handover/review/environment",
    params={"duty_date": "2026-09-03", "duty_shift": "night"},
    timeout=(5, 15),
)
response.raise_for_status()
result = response.json()
data = result["data"]
print(result["status"], result["warnings"])
print(data["weather"], data["dry_bulb_temperature"],
      data["wet_bulb_temperature"], data["relative_humidity"])
# 使用前分别判断 is not None；业务要求四项齐全时检查status == "success"。
```

## 7. 验证范围

通过隔离的FastAPI HTTP测试与临时SQLite验证白/夜班、默认班次边界、非法参数、温度保存后立即可见、天气缓存与超时、并发刷新、历史不串值、非有限数字及缺值。
测试使用mock天气客户端，不触发生产采集、不修改生产审核数据、不发送飞书消息。部署后需在目标服务器确认网络可达性与上游天气可用性。
