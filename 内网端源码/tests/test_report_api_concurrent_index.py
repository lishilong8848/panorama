import json
from concurrent.futures import ThreadPoolExecutor

from handover_log_module.service.handover_source_file_cache_service import HandoverSourceFileCacheService


def test_concurrent_api_sources_keep_all_index_entries(tmp_path):
    service = HandoverSourceFileCacheService({}, business_root_override=tmp_path / "cache")
    def write(index):
        source = tmp_path / f"source-{index}.json"
        source.write_text(json.dumps({
            "schema_version": 1, "source_family": "handover_log_family", "building": "A楼",
            "business_date": "", "query_start": "2026-11-04 08:00:00", "query_end": "2026-11-04 08:05:00",
            "sheets": [{"name": "sheet1", "rows": [[], [None, None, None, None, "2026-11-04 08:00:00"],
                                                        [], [None, "房间", "设备", "温度", index]]}],
        }, ensure_ascii=False), encoding="utf-8")
        identity = service.build_download_identity(building="A楼", template_name=f"report-{index}")
        service.register_downloaded_source(identity=identity, file_path=str(source), emit_log=lambda _: None)
        return identity
    with ThreadPoolExecutor(max_workers=12) as executor:
        identities = list(executor.map(write, range(20)))
    index = service._load_download_index()
    assert set(index) == set(identities)
    assert all(service.lookup_downloaded_source(identity=identity) for identity in identities)
