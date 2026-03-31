import json

import lark_oapi as lark
from lark_oapi.api.corehr.v2 import *


# SDK 使用说明: https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/server-side-sdk/python--sdk/preparations-before-development
# 以下示例代码默认根据文档示例值填充，如果存在代码问题，请在 API 调试台填上相关必要参数后再复制代码使用
# 复制该 Demo 后, 需要将 "YOUR_APP_ID", "YOUR_APP_SECRET" 替换为自己应用的 APP_ID, APP_SECRET.
def main():
    # 创建client
    client = lark.Client.builder() \
        .app_id("cli_a75263d211f2d00e") \
        .app_secret("H5yhfZJd1QbqDumbYFKHfcO8EI7Iecsc") \
        .log_level(lark.LogLevel.DEBUG) \
        .build()

    # 构造请求对象
    request: SearchEmployeeRequest = SearchEmployeeRequest.builder() \
        .page_size(100) \
        .user_id_type("user_id") \
        .department_id_type("open_department_id") \
        .request_body(SearchEmployeeRequestBody.builder()
            .fields(["person_info.phone_number"])
            .employment_id_list(["7140964208476371111"])
            .employee_number_list(["100001"])
            .work_email("13312345678@qq.com")
            .phone_number("16760342300")
            .key_word("张三")
            .employment_status("hired")
            .employee_type_id("6971090097697521314")
            .department_id_list(["7140964208476371111"])
            .direct_manager_id_list(["7027024823985117820"])
            .dotted_line_manager_id_list(["7027024823985117820"])
            .regular_employee_start_date_start("2020-01-01")
            .regular_employee_start_date_end("2020-01-01")
            .effective_time_start("2020-01-01")
            .effective_time_end("2020-01-01")
            .work_location_id_list_include_sub(["7140964208476371111"])
            .preferred_english_full_name_list(["Sandy"])
            .preferred_local_full_name_list(["小明"])
            .national_id_number_list(["110100xxxxxxxxxxxx"])
            .phone_number_list(["16760342300"])
            .email_address_list(["xxx@xxx.com"])
            .department_id_list_include_sub(["7140964208476371111"])
            .additional_national_id_number_list(["7140964208476371111"])
            .citizenship_status_list(["公民（中国大陆）"])
            .cost_center_id_list(["7140964208476371111"])
            .service_company_list(["7140964208476371111"])
            .service_company_list_include_sub(["7140964208476371111"])
            .job_family_id_list(["7140964208476371111"])
            .job_family_id_list_include_sub(["7140964208476371111"])
            .job_level_id_list(["7140964208476371111"])
            .job_grade_id_list(["7140964208476371111"])
            .job_id_list(["7140964208476371111"])
            .position_id_list(["7140964208476371111"])
            .position_id_list_include_sub(["7140964208476371111"])
            .working_hours_type_id_list(["7140964208476371111"])
            .nationality_id_list(["7140964208476371111"])
            .pay_group_id_list(["7140964208476371111"])
            .assignment_pay_group_id_list(["7140964208476371111"])
            .contract_type_list(["7140964208476371111"])
            .archive_cpst_plan_id_list(["7140964208476371111"])
            .build()) \
        .build()

    # 发起请求
    response: SearchEmployeeResponse = client.corehr.v2.employee.search(request)

    # 处理失败返回
    if not response.success():
        lark.logger.error(
            f"client.corehr.v2.employee.search failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}")
        return

    # 处理业务结果
    lark.logger.info(lark.JSON.marshal(response.data, indent=4))


if __name__ == "__main__":
    main()