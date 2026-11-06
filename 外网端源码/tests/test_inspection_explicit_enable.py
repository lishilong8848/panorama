from unittest.mock import Mock

import pytest

from app.modules.inspection_data.service import point_config_bitable as cloud
from app.modules.inspection_data.service import point_settings


def test_explicit_enable_is_separate_from_query_rules():
    group = {"baseline": {"7_0_1": {}}, "points": {}, "remove": [], "explicitly_enabled": ["7_0_1"]}
    settings = {"D楼": {"electrical_inspection_family": group}}
    assert point_settings.query_rules(settings)["D楼"]["electrical_inspection_family"] == {
        "baseline": ["7_0_1"], "add": [], "remove": []
    }
    row = cloud._expected_rows(settings["D楼"], include_explicit_enable=True)[("电气", "7_0_1")]
    assert row["基线测点"] is True
    assert row["启用"] is True
    assert row[cloud.EXPLICIT_ENABLE_FIELD] is True
    assert cloud.EXPLICIT_ENABLE_FIELD not in cloud._expected_rows(settings["D楼"])[("电气", "7_0_1")]


def test_missing_optional_field_does_not_write_cloud():
    client = Mock()
    client.list_fields.return_value = [
        {"field_name": "名称", "type": 1, "is_primary": True},
        *({"field_name": name, "type": kind} for name, kind in cloud._FIELD_TYPES.items()),
    ]
    settings = {"D楼": {"electrical_inspection_family": {
        "baseline": {"7_0_1": {}}, "points": {}, "remove": [], "explicitly_enabled": ["7_0_1"]
    }}}
    with pytest.raises(ValueError, match="用户主动启用"):
        cloud.sync_point_config_bitable({}, settings, buildings=("D楼",), initialize=True, client=client)
    client._post_json.assert_not_called()
    client.batch_create_records.assert_not_called()
    client.batch_update_records.assert_not_called()


def test_baseline_restore_and_explicit_enable_are_independent():
    group = {"baseline": {"7_0_1": {}}, "points": {}, "remove": []}
    row = cloud._expected_rows({"electrical_inspection_family": group}, include_explicit_enable=True)[("电气", "7_0_1")]
    assert row["启用"] is True
    assert cloud.EXPLICIT_ENABLE_FIELD not in row
    group["explicitly_disabled"] = ["7_0_1"]
    row = cloud._expected_rows({"electrical_inspection_family": group}, include_explicit_enable=True)[("电气", "7_0_1")]
    assert row[cloud.EXPLICIT_ENABLE_FIELD] is False


def test_restore_explicit_enable_from_cloud_without_enabling_removed_points():
    client = Mock()
    def records(_table, **_kwargs):
        return [
            {"record_id": "a", "fields": {"专业": "电气", "测点ID": "7_0_1", "基线测点": True,
                                           "启用": True, cloud.EXPLICIT_ENABLE_FIELD: True}},
            {"record_id": "b", "fields": {"专业": "电气", "测点ID": "7_0_2", "基线测点": True,
                                           "启用": False, cloud.EXPLICIT_ENABLE_FIELD: True}},
            {"record_id": "c", "fields": {"专业": "暖通", "测点ID": "8_0_1", "基线测点": True,
                                           "启用": True}},
        ]
    client.list_records.side_effect = records
    settings = cloud.read_point_config_bitable({}, client=client)
    for building in cloud.POINT_CONFIG_TABLES:
        group = settings[building]["electrical_inspection_family"]
        assert group["explicitly_enabled"] == ["7_0_1"]
        assert group["remove"] == ["7_0_2"]
        assert "explicitly_enabled" not in settings[building]["hvac_inspection_family"]


def test_old_local_config_does_not_overwrite_other_cloud_flags():
    group = {"baseline": {"7_0_1": {}, "7_0_2": {}}, "points": {}, "remove": [],
             "explicitly_enabled": ["7_0_1"]}
    rows = cloud._expected_rows({"electrical_inspection_family": group}, include_explicit_enable=True)
    assert rows[("电气", "7_0_1")][cloud.EXPLICIT_ENABLE_FIELD] is True
    assert cloud.EXPLICIT_ENABLE_FIELD not in rows[("电气", "7_0_2")]
