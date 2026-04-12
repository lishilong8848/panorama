from handover_log_module.core.building_title_rules import (
    HANDOVER_TITLE_CELL,
    build_handover_building_title,
    canonical_handover_building_title_map,
)


def test_build_handover_building_title_uses_building_code() -> None:
    assert HANDOVER_TITLE_CELL == "A1"
    assert build_handover_building_title("A楼") == "EA118机房A栋数据中心交接班日志"
    assert build_handover_building_title("B栋") == "EA118机房B栋数据中心交接班日志"
    assert build_handover_building_title("EA118机房C栋") == "EA118机房C栋数据中心交接班日志"


def test_canonical_handover_building_title_map_is_fixed() -> None:
    assert canonical_handover_building_title_map() == {
        "A楼": "EA118机房A栋数据中心交接班日志",
        "B楼": "EA118机房B栋数据中心交接班日志",
        "C楼": "EA118机房C栋数据中心交接班日志",
        "D楼": "EA118机房D栋数据中心交接班日志",
        "E楼": "EA118机房E栋数据中心交接班日志",
    }
