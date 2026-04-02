import requests
import json

# 配置信息
APP_ID = "cli_a9d1c9fe9d381cee"
APP_SECRET = "PCZSOMI9ITqFWUkqT5TtQcVljP0Sx48y"
APP_TOKEN = "D01TwFPyXiJBY6kCBDZcMCGLnSe"
TABLE_ID = "tblpaHktT0mn0hwg"
RECORD_ID = "recve6bRxanpLf"

def get_tenant_access_token():
    """获取飞书租户访问令牌"""
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {
        "app_id": APP_ID,
        "app_secret": APP_SECRET
    }
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        return response.json().get("tenant_access_token")
    else:
        raise Exception(f"获取 token 失败: {response.text}")

def get_record_field(token, field_name):
    """查询指定记录的特定字段内容"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/{RECORD_ID}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        if data.get("code") == 0:
            # 飞书 API 返回的数据结构中，字段信息在 data.record.fields 下
            fields = data.get("data", {}).get("record", {}).get("fields", {})
            return fields.get(field_name)
        else:
            raise Exception(f"查询记录失败: {data.get('msg')} (code: {data.get('code')})")
    else:
        raise Exception(f"网络请求失败: {response.status_code}, {response.text}")

if __name__ == "__main__":
    try:
        print(f"正在查询记录 {RECORD_ID} ...")
        token = get_tenant_access_token()
        
        description = get_record_field(token, "告警描述")
        
        print("-" * 30)
        print(f"查询成功！")
        print(f"告警描述: {description}")
        print("-" * 30)
    except Exception as e:
        print(f"执行出错: {e}")
