from handover_log_module.service.handover_capacity_report_service import _build_fixed_header_cells


def test_build_fixed_header_cells_for_a_building() -> None:
    cells = _build_fixed_header_cells("A楼")

    assert cells["A1"] == "世纪互联南通数据中心A栋FM运维交接班重要事项"
    assert cells["E5"] == "A楼"
    assert cells["G16"] == "A楼"
    assert cells["G17"] == "A楼"
    assert cells["G18"] == "A楼"
    assert cells["S15"] == "A楼"
    assert cells["S16"] == "A楼"
    assert cells["S17"] == "A楼"
    assert cells["S18"] == "A楼"
    assert cells["A20"] == "A楼"
    assert cells["A65"] == "A楼容量一览表"
    assert cells["O55"] == "A楼能耗一览"
