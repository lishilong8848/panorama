from unittest.mock import Mock
import copy
from datetime import datetime, timedelta
import json
from pathlib import Path

import pytest
from openpyxl import Workbook

import app.modules.inspection_data.service.inspection_data_upload_service as module
from app.modules.inspection_data.service.inspection_data_upload_service import (
    InspectionDataUploadService, load_catalog, field_definitions,
)


def source(path, rows):
    wb = Workbook()
    for row in rows:
        wb.active.append([None, *row])
    wb.save(path)
    wb.close()
    return path


@pytest.fixture(autouse=True)
def forbid_live_clients(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('Tests must use mock clients, not live bridge or Feishu access')
    monkeypatch.setattr(module.InternalBridgeHttpClient, 'from_runtime_config', forbidden)
    monkeypatch.setattr(module, 'require_feishu_auth_settings', forbidden)
    monkeypatch.setattr(module, 'FeishuBitableClient', forbidden)


def test_catalog_covers_all_forms_and_e_specific_points():
    catalog = load_catalog()
    assert len({d['workbook'] for d in catalog}) == 4
    assert sum(d['building'] == 'D楼' for d in catalog) == 458
    assert sum(d['building'] == 'E楼' for d in catalog) == 521
    assert sum(len(d['points']) for d in catalog) == 2151
    assert len({(d['building'], d['position'], d['device']) for d in catalog}) == len(catalog)
    e = [d for d in catalog if d['building'] == 'E楼']
    assert any(d['device'] == '14#柴发' for d in e)
    assert any(d['device'] == 'E-279直流屏' for d in e)
    assert not any(d['device'] == '14#柴发' for d in catalog if d['building'] == 'D楼')
    assert {p['field'] for d in e for p in d['points']} <= {f['field_name'] for f in field_definitions()}


def test_merged_rows_zero_and_missing_are_distinct(tmp_path):
    p = source(tmp_path/'d.xlsx', [
        ['电气系统西区','D-144-TR-101','L1温度(℃)',0],
        [None,None,'L2温度(℃)',35],
        [None,None,'L3温度(℃)',36],
        [None,None,'告警状态','正常'],
        [None,None,'启停状态',1],
    ])
    result = InspectionDataUploadService.parse_workbook(p, building='D楼')
    row = next(r for r in result['rows'] if r['设备编号']=='D-144-TR-101')
    assert row['L1温度(℃)'] == 0
    assert row['启停状态'] == '开启'
    assert row['数据完整性'] == '完整'
    missing = next(r for r in result['rows'] if r['设备编号']=='D-144-TR-102')
    assert 'L1温度(℃)' not in missing
    assert missing['数据完整性'] == '缺少源数据'


def test_equal_crah_names_need_room_context(tmp_path):
    p = source(tmp_path/'d.xlsx', [
        ['M1包间精密空调/北侧（240）','CRAH01','回风温度',26],
        ['M1包间精密空调/南侧（241）','CRAH01','回风温度',27],
        ['不明确位置','CRAH01','回风温度',99],
    ])
    result = InspectionDataUploadService.parse_workbook(p, building='D楼')
    assert result['matched_devices'] == 2
    assert len(result['diagnostics']) == 1
    values = [r['回风温度(℃)'] for r in result['rows'] if '回风温度(℃)' in r]
    assert sorted(values) == [26,27]


def test_crah_full_source_name_uses_printed_room(tmp_path):
    p = source(tmp_path/'d.xlsx', [['240','D-240-CRAH-01','回风温度(℃)',25]])
    result = InspectionDataUploadService.parse_workbook(p, building='D楼')
    assert result['matched_devices'] == 1
    assert next(r for r in result['rows'] if '回风温度(℃)' in r)['位置'].endswith('北侧（240）')


def test_generic_phase_temperature_is_not_guessed(tmp_path):
    p=source(tmp_path/'d.xlsx', [['西区','D-144-TR-101','温度',55]])
    with pytest.raises(ValueError, match='没有可识别'):
        InspectionDataUploadService.parse_workbook(p,building='D楼')


def test_real_electrical_names_match_leading_device_only(tmp_path):
    p = source(tmp_path / "electrical.xlsx", [
        ["变电所P1 D-144", "D-144-AH-102_D-144-TR-101变压器馈线柜_电量仪", "平均_电流_A", 12],
        [None, None, "B相温度", 99],
        [None, "D-144-TR-101_变压器温控器", "B相温度", 35],
        [None, "D-144-AD-001_直流屏", "合母电压", 220],
    ])
    rows = InspectionDataUploadService.parse_workbook(p, building="D楼")["rows"]
    assert next(row for row in rows if row["设备编号"] == "144-AH-102")["电流(A)"] == 12
    assert next(row for row in rows if row["设备编号"] == "D-144-TR-101")["L2温度(℃)"] == 35
    assert next(row for row in rows if row["设备编号"] == "直流屏" and row["位置"] == "电气系统西区")["合母电压(V)"] == 220


@pytest.mark.parametrize('value',['nan','inf','-inf','2A'])
def test_invalid_temperature_not_converted_to_zero(tmp_path,value):
    p=source(tmp_path/'d.xlsx', [['西区','D-144-TR-101','L1温度(℃)',value]])
    with pytest.raises(ValueError,match='没有可识别'):
        InspectionDataUploadService.parse_workbook(p,building='D楼')


def test_wrong_building_and_conflicting_duplicate_rejected(tmp_path):
    p=source(tmp_path/'d.xlsx', [['E楼','E-144-TR-101','L1温度(℃)',35]])
    with pytest.raises(ValueError,match='其他楼栋'):
        InspectionDataUploadService.parse_workbook(p,building='D楼')
    p=source(p,[['西区','D-144-TR-101','L1温度(℃)',35],[None,None,'L1温度(℃)',36]])
    with pytest.raises(ValueError,match='重复点位'):
        InspectionDataUploadService.parse_workbook(p,building='D楼')


def test_alarm_health_does_not_imply_running(tmp_path):
    p=source(tmp_path/'d.xlsx', [['西区','D-144-TR-101','告警状态','正常']])
    row=next(r for r in InspectionDataUploadService.parse_workbook(p,building='D楼')['rows'] if r['设备编号']=='D-144-TR-101')
    assert '启停状态' not in row


@pytest.mark.parametrize(('building', 'source_name'), [
    ('D楼', 'D-178-柴发并机系统'), ('E楼', '柴发并机系统'),
])
def test_parallel_diesel_points_match_exact_generator_and_unit(building, source_name):
    readings = [
        [building, source_name, '1#_电池电压_V', '26.7'],
        [None, None, '1#_冷却液温度_℃', '90.0'],
        [None, None, '10#_电池电压_V', '27.1'],
        [None, None, '11#_冷却液温度_℃', '3276.40'],
        [None, None, '99#_电池电压_V', '28'],
        [None, '其他系统', '1#油箱体积', '554.29'],
        [None, '其他系统', '2#_电池电压_V', '26.8'],
    ]
    matched_rows = {}
    result = InspectionDataUploadService.parse_samples(readings, building=building, value_time='', matched_rows=matched_rows)
    rows = {r['设备编号']: r for r in result['rows'] if r['巡检表单'].strip() == '柴发巡检表'}
    assert result['valid_points'] == 3
    assert rows['1#柴发']['蓄电池电压(V)'] == 26.7
    assert rows['1#柴发']['冷却液水温(℃)'] == 90
    assert rows['10#柴发']['蓄电池电压(V)'] == 27.1
    assert '冷却液水温(℃)' not in rows['11#柴发']
    diesel_11 = next(i for i, row in enumerate(result['rows'])
                    if row['设备编号'] == '11#柴发' and row['巡检表单'].strip() == '柴发巡检表')
    assert matched_rows[diesel_11, '冷却液水温(℃)'] == [3]
    assert '柴油液位(cm)' not in rows['1#柴发']
    assert '蓄电池电压(V)' not in rows['2#柴发']
    assert any('3276.40' in text for text in result['diagnostics'])
    with pytest.raises(ValueError, match='重复点位数值冲突'):
        InspectionDataUploadService.parse_samples(
            [*readings, [building, source_name, '1#_电池电压_V', '27.0']], building=building, value_time='',
        )


@pytest.mark.parametrize(('building', 'source_name'), [
    ('D楼', 'D-178-燃油自控系统'), ('E楼', '燃油自控系统'),
])
def test_diesel_liquid_level_scales_volume_and_rounds_all_measurements(building, source_name):
    result = InspectionDataUploadService.parse_samples([
        [building, source_name, '1#油箱体积', '554.29'],
        [None, None, '2#油箱体积', '565.55'],
        [None, None, '10#油箱体积', '0'],
        [None, None, '11#油箱体积', '—'],
        [None, None, '99#油箱体积', '700'],
        [None, '柴发并机系统', '1#_电池电压_V', '26.675'],
        [None, None, '1#_冷却液温度_℃', '28'],
        [None, None, '3#油箱体积', '400'],
    ], building=building, value_time='')
    rows = {r['设备编号']: r for r in result['rows'] if r['巡检表单'].strip() == '柴发巡检表'}
    assert result['valid_points'] == 5
    assert rows['1#柴发']['柴油液位(cm)'] == 55.43
    assert rows['2#柴发']['柴油液位(cm)'] == 56.56
    assert rows['10#柴发']['柴油液位(cm)'] == 0
    assert '柴油液位(cm)' not in rows['11#柴发'] and '柴油液位(cm)' not in rows['3#柴发']
    assert rows['1#柴发']['蓄电池电压(V)'] == 26.68
    assert rows['1#柴发']['冷却液水温(℃)'] == 28


class Cloud:
    def __init__(self):
        self.rows={t: [{'record_id':'old'+t,'fields':{'设备编号':'old'}}] for t in module.DEFAULT_CONFIG['tables'].values()}
        self.events=[]
        self.fail=False
    def list_fields(self,table):
        return field_definitions()
    def list_records(self,table,**kwargs):
        assert kwargs['max_records']==0
        return self.rows[table]
    def batch_delete_records(self,table,ids,**kwargs):
        self.events.append(('delete',table))
        self.rows[table]=[r for r in self.rows[table] if r['record_id'] not in ids]
        return len(ids)
    def batch_create_records(self,table,rows,**kwargs):
        self.events.append(('create',table))
        if self.fail:
            raise RuntimeError('network timeout')
        created=[{'record_id':f'{table}-{len(self.events)}-{i}','fields':r} for i,r in enumerate(rows)]
        self.rows[table].extend(created)
        return [{'data':{'records':created}}]
    def update_record(self, table, record_id, fields):
        self.events.append(('update', table))
        next(row for row in self.rows[table] if row['record_id'] == record_id)['fields'].update(fields)
        return {'data': {}}


def inspection_datasets(tmp_path, invalid_source=None):
    datasets = {}
    catalog = load_catalog()
    for building in ['D楼', 'E楼']:
        datasets[building] = {}
        for index, (family, contract) in enumerate(module.SOURCE_FAMILIES.items()):
            device = next(d for d in catalog if d['building'] == building and d['discipline'] == contract['discipline'])
            point = device['points'][0]
            value = '36' if point['type'] == 2 else '原始状态'
            if invalid_source == (building, family):
                value = None
            datasets[building][family] = {
                'schema_version': 1, 'building': building, 'source_family': family,
                'report_name': contract['report_name'], 'bucket_key': '2026-10-27 10',
                'query_start': f'2026-10-27 09:{40+index*5}:00',
                'query_end': f'2026-10-27 10:0{index*5}:00',
                'value_time': f'2026-10-27 09:{40+index*5}:00',
                'rows': [[device['position'], device['device'], point['field'], value]],
            }
    return datasets


def prepared(tmp_path,monkeypatch,invalid_e=False,invalid_source=None):
    if invalid_e:
        invalid_source = ('E楼', 'electrical_inspection_family')
    files = inspection_datasets(tmp_path, invalid_source)
    cloud=Cloud()
    service=InspectionDataUploadService({},bitable_client=cloud)
    service.test_datasets = files
    monkeypatch.setattr(service,'_sources',lambda *_:files)
    monkeypatch.setattr(module, 'load_workbook', Mock(side_effect=AssertionError('upload must not open Excel')))
    monkeypatch.setattr(module,'BACKUP_ROOT',tmp_path/'backups')
    backups=[]
    monkeypatch.setattr(module,'atomic_write_text',lambda path,text:backups.append((path,text)))
    return service,cloud,backups


def test_preflight_all_sources_before_clearing(tmp_path,monkeypatch):
    service,cloud,_=prepared(tmp_path,monkeypatch,invalid_e=True)
    with pytest.raises(ValueError,match='没有可识别'):
        service.run()
    assert cloud.events==[]


def test_replace_both_tables_and_retry_does_not_append(tmp_path,monkeypatch):
    service,cloud,backups=prepared(tmp_path,monkeypatch)
    for _ in range(2):
        result=service.run()
        assert result['uploaded_count']==983
        assert len(cloud.rows[module.DEFAULT_CONFIG['tables']['D楼']])==460
        assert len(cloud.rows[module.DEFAULT_CONFIG['tables']['E楼']])==523
    assert [e[0] for e in cloud.events]==['create','create','delete','update','delete','update']*2
    assert len(backups)==8
    row=cloud.rows[module.DEFAULT_CONFIG['tables']['D楼']][0]['fields']
    assert isinstance(row['上传日期'],int) and row['源文件时间']>0
    assert all(field['type'] != 17 for field in field_definitions())


def test_dry_run_and_cancellation_never_delete(tmp_path,monkeypatch):
    service,cloud,backups=prepared(tmp_path,monkeypatch)
    assert service.run(dry_run=True)['dry_run'] is True
    assert cloud.events==[] and backups==[]
    def cancel():
        raise RuntimeError('cancelled')
    with pytest.raises(RuntimeError,match='cancelled'):
        service.run(check_cancel=cancel)
    assert cloud.events==[]


def test_create_failure_is_not_success_and_has_backup(tmp_path,monkeypatch):
    service,cloud,backups=prepared(tmp_path,monkeypatch)
    cloud.fail=True
    with pytest.raises(RuntimeError,match='未删除旧记录'):
        service.run()
    assert len(backups)==2
    assert len(cloud.rows[module.DEFAULT_CONFIG['tables']['E楼']])==1


def test_missing_api_dataset_does_not_accept_file_paths(tmp_path):
    class Internal:
        def get_inspection_snapshot(self, *, source_date, bucket_key):
            return {'ok': True, 'bucket_key': bucket_key, 'datasets': [], 'entries': [{'file_path': str(tmp_path/'legacy.xlsx')}]}
    cloud=Cloud()
    service=InspectionDataUploadService({'inspection_data_upload':{'wait_source_timeout_sec':0}},internal_client=Internal(),bitable_client=cloud)
    with pytest.raises(RuntimeError,match='未清空多维表'):
        service.run()
    assert cloud.events==[]


@pytest.mark.parametrize('building',['D楼','E楼'])
@pytest.mark.parametrize('discipline',['电气','暖通'])
def test_every_catalog_point_round_trips_without_missing_or_cross_device_values(tmp_path,building,discipline):
    catalog=[d for d in load_catalog() if d['building']==building and d['discipline']==discipline]
    inputs=[]
    for i,d in enumerate(catalog):
        for p in d['points']:
            inputs.append([d['position'],d['device'],p['field'],i+0.25 if p['type']==2 else '原始状态'])
        inputs.append([d['position'],d['device'],'启停状态',i%2])
    result=InspectionDataUploadService.parse_workbook(source(tmp_path/'full.xlsx',inputs),building=building,catalog=catalog)
    assert result['diagnostics']==[]
    assert result['missing_devices']==0
    assert result['matched_devices']==len(catalog)
    for i,(d,r) in enumerate(zip(catalog,result['rows'])):
        assert r['缺少源数据']==''
        assert r['启停状态']==('开启' if i%2 else '关闭')
        for p in d['points']:
            assert r[p['field']]==(i+0.25 if p['type']==2 else '原始状态')


def test_field_conflict_in_e_prevents_d_clear(tmp_path,monkeypatch):
    service,cloud,_=prepared(tmp_path,monkeypatch)
    original=cloud.list_fields
    def fields(table):
        definitions=original(table)
        if table==module.DEFAULT_CONFIG['tables']['E楼']:
            definitions[0]['type']=2
        return definitions
    cloud.list_fields=fields
    with pytest.raises(ValueError,match='类型不符'):
        service.run()
    assert cloud.events==[]


def test_same_room_changed_device_blank_does_not_reuse_previous_room(tmp_path):
    p=source(tmp_path/'d.xlsx', [['西区','D-144-TR-101','L1温度(℃)',35],['其他位置',None,'L2温度(℃)',99]])
    row=next(r for r in InspectionDataUploadService.parse_workbook(p,building='D楼')['rows'] if r['设备编号']=='D-144-TR-101')
    assert 'L2温度(℃)' not in row


def test_retry_preserves_pre_failure_snapshot(tmp_path,monkeypatch):
    original_write=module.atomic_write_text
    service,cloud,_=prepared(tmp_path,monkeypatch)
    monkeypatch.setattr(module,'atomic_write_text',original_write)
    cloud.fail=True
    for _ in range(2):
        with pytest.raises(RuntimeError):
            service.run()
    backup=tmp_path/'backups'/f"{module.DEFAULT_CONFIG['tables']['D楼']}.json"
    snapshot=json.loads(backup.read_text(encoding='utf-8'))
    assert snapshot['status']=='pending'
    assert snapshot['records'][0]['fields']['设备编号']=='old'
    cloud.fail=False
    service.run()
    assert json.loads(backup.read_text(encoding='utf-8'))['status']=='completed'


def test_second_building_partial_create_does_not_delete_either_old_batch(tmp_path, monkeypatch):
    service, cloud, _ = prepared(tmp_path, monkeypatch)
    originals = {table: list(rows) for table, rows in cloud.rows.items()}
    create = cloud.batch_create_records
    e_table = module.DEFAULT_CONFIG['tables']['E楼']

    def partial(table, rows, **kwargs):
        if table == e_table:
            create(table, rows[:1], **kwargs)
            raise RuntimeError('connection lost after partial commit')
        return create(table, rows, **kwargs)

    monkeypatch.setattr(cloud, 'batch_create_records', partial)
    with pytest.raises(RuntimeError, match='未删除旧记录'):
        service.run()
    assert cloud.rows == originals


def test_cleanup_failure_keeps_both_verified_new_batches(tmp_path, monkeypatch):
    service, cloud, _ = prepared(tmp_path, monkeypatch)
    delete = cloud.batch_delete_records
    e_table = module.DEFAULT_CONFIG['tables']['E楼']

    def partial(table, ids, **kwargs):
        if table == e_table:
            raise RuntimeError('cleanup timeout')
        return delete(table, ids, **kwargs)

    monkeypatch.setattr(cloud, 'batch_delete_records', partial)
    with pytest.raises(RuntimeError, match='新批次已核验并保留'):
        service.run()
    assert all(any(row['fields'].get('上传日期') for row in rows) for rows in cloud.rows.values())
    assert any(row['record_id'] == 'old' + e_table for row in cloud.rows[e_table])


def test_fixed_first_timestamp_column_ignores_summary_and_later_values(tmp_path):
    wb=Workbook()
    s=wb.active
    s.append([None,'位置','设备编号','指标','最大值','2026-10-27 09:00:00','2026-10-27 09:05:00'])
    s.append([None,'西区','D-144-TR-101','L1温度(℃)',99,35,36])
    s.append([None,None,None,'L2温度(℃)',99,None,37])
    p=tmp_path/'times.xlsx';wb.save(p);wb.close()
    parsed=InspectionDataUploadService.parse_workbook(p,building='D楼')
    row=next(r for r in parsed['rows'] if r['设备编号']=='D-144-TR-101')
    assert row['L1温度(℃)']==35
    assert 'L2温度(℃)' not in row
    assert parsed['value_column']==6 and parsed['value_time']=='2026-10-27 09:00:00'


def test_multi_column_without_time_headers_fails_before_upload(tmp_path):
    wb=Workbook();wb.active.append([None,'西区','D-144-TR-101','L1温度(℃)',35,99]);p=tmp_path/'unknown.xlsx';wb.save(p);wb.close()
    with pytest.raises(ValueError,match='时间表头'):
        InspectionDataUploadService.parse_workbook(p,building='D楼')


SOURCE_PAIRS = [(b, family) for b in ['D楼', 'E楼'] for family in module.SOURCE_FAMILIES]


@pytest.fixture(autouse=True)
def isolate_point_state(tmp_path, monkeypatch):
    from app.modules.inspection_data.service import point_settings

    original = point_settings._path
    monkeypatch.setattr(point_settings, '_path', lambda config: original(config)
                        if config.get('paths', {}).get('runtime_state_root') else tmp_path / 'points.json')
    monkeypatch.setattr('app.modules.feishu.service.feishu_auth_resolver.resolve_feishu_auth_settings', lambda *_: {})


def set_dataset_end(entry, end):
    entry.update(
        bucket_key=module.inspection_hour_bucket(end),
        query_start=(end - timedelta(minutes=20)).strftime('%Y-%m-%d %H:%M:%S'),
        query_end=end.strftime('%Y-%m-%d %H:%M:%S'),
        value_time=end.strftime('%Y-%m-%d %H:%M:%S'),
    )


class SnapshotSources:
    def __init__(self, datasets):
        self.queries = []
        self.refreshes = []
        self.entries = {(building, family): entry for building, docs in datasets.items() for family, entry in docs.items()}
        for entry in self.entries.values():
            set_dataset_end(entry, module.datetime.now().replace(microsecond=0))

    def get_inspection_snapshot(self, *, source_date, bucket_key):
        self.queries.append((source_date, bucket_key))
        bucket = bucket_key or f'{source_date} 10'
        return {'ok': True, 'bucket_key': bucket,
                'datasets': [entry for entry in self.entries.values() if entry['bucket_key'] == bucket]}

    def refresh_latest_source_cache(self, **kwargs):
        self.refreshes.append(kwargs)


def test_baseline_is_persistent_and_only_the_effective_ids_can_upload(tmp_path, monkeypatch):
    from app.modules.inspection_data.service import point_config_bitable, point_settings

    config = {"paths": {"runtime_state_root": str(tmp_path)}, "inspection_data_upload": {"wait_source_timeout_sec": 0}}
    internal = SnapshotSources(inspection_datasets(tmp_path))
    internal.put_inspection_point_rules = Mock(side_effect=lambda rules, revision: {"ok": True, "point_overrides": rules, "revision": revision})
    monkeypatch.setattr(point_settings.InternalBridgeHttpClient, "from_runtime_config", lambda _: internal)
    cloud_config = Mock()
    monkeypatch.setattr(point_config_bitable, 'sync_point_config_bitable', cloud_config)
    for doc in internal.entries.values():
        doc.update(point_ids=['7_0_1'], requested_point_ids=['7_0_1'])

    docs = list(internal.entries.values())
    with pytest.raises(ValueError, match='四组数据'):
        point_settings.initialize_point_baseline(config, docs[:-1])
    assert point_settings.load_point_settings(config) is None
    settings = point_settings.initialize_point_baseline(config, docs)
    assert settings == point_settings.load_point_settings(config)
    b, f = 'D楼', 'electrical_inspection_family'
    group = settings[b][f]
    assert list(group['baseline']) == ['7_0_1']
    assert group['baseline']['7_0_1']['sampleTime'] == docs[0]['value_time']
    configured = {b: {f: {**group, 'points': {'7_0_1': '只修改备注'}}}}
    assert point_settings.query_rules(configured)[b][f] == {'add': [], 'remove': [], 'baseline': ['7_0_1']}
    with pytest.raises(ValueError, match='必须保留'):
        point_settings.save_point_group(config, building=b, family=f, points={}, remove=['7_0_1'])
    saved = point_settings.save_point_group(config, building=b, family=f, points={'7_0_2': '改点'},
                                            remove=['7_0_1'], expected_group=group)['settings']
    assert saved[b][f]['baseline'] == group['baseline']
    cloud_config.assert_called_once_with(config, saved, buildings=(b,), initialize=True)
    rules = point_settings.query_rules(saved)
    for (building, family), doc in internal.entries.items():
        doc['point_overrides'] = rules[building][family]
    doc = internal.entries[b, f]
    doc.update(point_ids=['7_0_3'], requested_point_ids=['7_0_3'])
    assert point_settings.initialize_point_baseline(config, docs) == saved
    service = InspectionDataUploadService(config, internal_client=internal, bitable_client=Cloud())
    with pytest.raises(ValueError, match='与已保存清单不符'):
        service.run(dry_run=True)
    doc.update(point_ids=['7_0_2'], requested_point_ids=['7_0_2'])
    assert service.run(dry_run=True)['status'] == 'success'
    cloud_config.side_effect = RuntimeError('timeout')
    failed = point_settings.save_point_group(config, building=b, family=f, points={'7_0_2': '新备注'}, remove=['7_0_1'])
    assert failed['cloud_synced'] is False and failed['synced'] is True
    assert point_settings._pending_buildings(config) == {b}
    cloud_config.side_effect = None
    point_settings.retry_pending_point_config(config)
    assert not point_settings._pending_buildings(config)


def test_feishu_point_config_upsert_and_recovery():
    from app.modules.inspection_data.service import point_config_bitable as cfg

    cloud = Mock()
    tables = {table: [{'record_id': 'blank', 'fields': {}}] for table in cfg.POINT_CONFIG_TABLES.values()}
    cloud.list_fields.return_value = [{'field_name': '文本', 'type': 1, 'is_primary': True},
                                      *({'field_name': k, 'type': t} for k, t in cfg._FIELD_TYPES.items())]
    cloud.list_records.side_effect = lambda table, **_: tables[table]

    def create(table, rows):
        for fields in rows:
            tables[table].append({'record_id': f'rec{len(tables[table])}', 'fields': fields})

    def update(table, rows):
        for row in rows:
            next(r for r in tables[table] if r['record_id'] == row['record_id'])['fields'].update(row['fields'])

    cloud.batch_create_records.side_effect = create
    cloud.batch_update_records.side_effect = update
    groups = {b: {f: {'baseline': {'7_0_1': {'name': '采样点', 'path': '路径'}}, 'points': {}, 'remove': []}
                  for f in module.SOURCE_FAMILIES} for b in cfg.POINT_CONFIG_TABLES}
    result = cfg.sync_point_config_bitable({}, groups, client=cloud)
    assert all(v['count'] == 2 and v['created'] == v['updated'] == 1 for v in result.values())
    assert cfg.read_point_config_bitable({}, client=cloud) == groups
    assert all(v['created'] == v['updated'] == 0 for v in cfg.sync_point_config_bitable({}, groups, client=cloud).values())
    group = groups['D楼']['electrical_inspection_family']
    group.update(points={'7_0_2': '新点'}, remove=['7_0_1'],
                 targets={'7_0_2': {'workbook': '电气巡检表', 'sheet': '柴油', 'cell': 'C6'}})
    result = cfg.sync_point_config_bitable({}, groups, buildings=('D楼',), client=cloud)
    assert result['D楼']['created'] == result['D楼']['updated'] == 1
    assert cfg.read_point_config_bitable({}, client=cloud) == groups
    cloud.batch_delete_records.assert_not_called()


def test_custom_point_settings_sync_and_upload_point_remark(tmp_path, monkeypatch):
    from app.modules.inspection_data.service import point_settings

    config = {"paths": {"runtime_state_root": str(tmp_path)}, "inspection_data_upload": {"wait_source_timeout_sec": 0}}
    internal = SnapshotSources(inspection_datasets(tmp_path))
    internal.put_inspection_point_rules = Mock(side_effect=lambda rules, revision: {"ok": True, "point_overrides": rules, "revision": revision})
    monkeypatch.setattr(point_settings.InternalBridgeHttpClient, "from_runtime_config", lambda _: internal)
    family = 'electrical_inspection_family'
    point_id, excluded = '7_0_387_1_606_0', '7_0_387_1_614_0'
    book = next(name for name, sheets in point_settings.inspection_target_options('D楼').items() if '柴发巡检表' in sheets)
    target = {'workbook': book, 'sheet': '柴发巡检表', 'cell': 'b6'}
    saved = point_settings.save_point_group(config, building='D楼', family=family, points={point_id: '复测点'},
                                            remove=[excluded], targets={point_id: target})
    assert saved['synced'] is True
    assert point_settings.load_point_settings(config) == saved['settings']

    assert internal.put_inspection_point_rules.call_args.args[0] == {
        'D楼': {family: {'add': [point_id], 'remove': [excluded]}},
    }
    cloud = Cloud()
    service = InspectionDataUploadService(config, internal_client=internal, bitable_client=cloud)
    monkeypatch.setattr(module, 'BACKUP_ROOT', tmp_path/'backups')
    with pytest.raises(RuntimeError, match='未清空多维表'):
        service.run()
    assert not cloud.events

    entry = internal.entries['D楼', family]
    entry.update(point_overrides={'add': [point_id], 'remove': [excluded]}, point_ids=[point_id], requested_point_ids=[point_id])
    entry['rows'] = [['D楼 变电间', '新增设备', '待归类电流', '12.345']]
    assert service.run()['results']['D楼']['point_count'] == 2
    detail = next(row['fields'] for row in cloud.rows[module.DEFAULT_CONFIG['tables']['D楼']]
                  if row['fields'].get('测点ID') == point_id)
    assert detail['记录类型'] == '测点明细'
    assert detail['测点值'] == '12.35' and detail['测点备注'] == '复测点'
    assert (detail['目标巡检表'], detail['目标工作表'], detail['目标单元格']) == (book, '柴发巡检表', 'B6')
    assert detail['测点路径'] == 'D楼 变电间 / 新增设备 / 待归类电流'

    with pytest.raises(ValueError, match='同时'):
        point_settings.save_point_group(config, building='D楼', family=family, points={point_id: ''}, remove=[point_id])
    with pytest.raises(ValueError, match='有效工作表和单元格'):
        point_settings.save_point_group(config, building='D楼', family=family, points={point_id: ''}, remove=[],
                                        targets={point_id: {**target, 'cell': 'XFE1'}})
    with pytest.raises(ValueError, match='已分配'):
        point_settings.save_point_group(config, building='D楼', family='hvac_inspection_family', points={excluded: ''}, remove=[],
                                        targets={excluded: target})
    assert point_settings.load_point_settings(config) == saved['settings']


def test_review_point_routes_are_building_scoped_and_reject_stale_edits(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from app.modules.scheduler.api.inspection_data_upload_routes import router

    config = {'paths': {'runtime_state_root': str(tmp_path)}}
    internal = SnapshotSources(inspection_datasets(tmp_path))
    family = 'electrical_inspection_family'
    doc = internal.entries['D楼', family]
    doc['point_ids'] = ['7_0_1']
    internal.get_latest_inspection_points = Mock(return_value={'ok': True, 'datasets': [doc]})
    internal.put_inspection_point_rules = Mock(side_effect=lambda rules, revision: {'ok': True, 'point_overrides': rules, 'revision': revision})
    monkeypatch.setattr(module.InternalBridgeHttpClient, 'from_runtime_config', lambda _: internal)
    app = FastAPI()
    app.include_router(router)
    app.state.container = SimpleNamespace(runtime_config=config, _configured_deployment_snapshot=lambda: {'role_mode': 'external'})
    base = '/api/scheduler/inspection-data-upload/points'
    with TestClient(app) as client:
        change = {'family': family, 'points': {'7_0_1': '复测'}, 'remove': [], 'expected_group': {'points': {}, 'remove': []}}
        assert client.put(f'{base}/d', json=change).status_code == 200
        assert client.put(f'{base}/d', json=change).status_code == 409
        assert client.put(f'{base}/d', json={**change, 'building': 'E楼'}).status_code == 400
        assert client.get(f'{base}/e').json()['settings'] == {}
        assert client.get(f'{base}/a').status_code == 404
        latest = client.get(f'{base}/d/latest').json()['datasets'][family]
        assert latest['sample_time'] == doc['value_time']
        assert latest['points'][0]['id'] == '7_0_1'
        assert client.get(f'{base}/e/latest').status_code == 503


def test_public_review_only_exposes_inspection_capability_not_management():
    from fastapi import HTTPException
    from starlette.requests import Request
    from app.bootstrap.app_factory import _is_externally_allowed_path
    from app.modules.handover_review.api.routes import (
        handover_review_inspection_access, handover_review_inspection_points_page,
    )
    import asyncio

    def request(peer, host):
        return Request({'type': 'http', 'path': '/handover/review/d/inspection-points',
                        'headers': [(b'host', host.encode())], 'client': (peer, 80)})

    public = request('8.8.8.8', 'public.example')
    lan = request('192.168.10.12', '192.168.10.14')
    assert json.loads(handover_review_inspection_access('d', public).body) == {'allowed': False}
    assert json.loads(handover_review_inspection_access('e', lan).body) == {'allowed': True}
    assert json.loads(handover_review_inspection_access('a', lan).body) == {'allowed': False}
    assert _is_externally_allowed_path('/api/handover/review/d/inspection-access')
    assert not _is_externally_allowed_path('/api/scheduler/inspection-data-upload/points/d')
    with pytest.raises(HTTPException) as error:
        asyncio.run(handover_review_inspection_points_page('d', public))
    assert error.value.status_code == 403


@pytest.mark.parametrize('pair', SOURCE_PAIRS)
def test_each_of_four_sources_must_parse_before_any_clear(tmp_path, monkeypatch, pair):
    service, cloud, backups = prepared(tmp_path, monkeypatch, invalid_source=pair)
    with pytest.raises(ValueError, match='没有可识别'):
        service.run()
    assert cloud.events == [] and backups == []


@pytest.mark.parametrize('pair', SOURCE_PAIRS)
def test_each_missing_source_is_named_and_blocks_clear(tmp_path, pair):
    internal = SnapshotSources(inspection_datasets(tmp_path))
    del internal.entries[pair]
    cloud = Cloud()
    service = InspectionDataUploadService({'inspection_data_upload': {'wait_source_timeout_sec': 0}},
                                         internal_client=internal, bitable_client=cloud)
    with pytest.raises(RuntimeError, match=f'{pair[0]}/{module.SOURCE_FAMILIES[pair[1]]["label"]}'):
        service.run()
    assert cloud.events == [] and internal.refreshes == []
    assert internal.queries[0] == (module.datetime.now().date().isoformat(), module.inspection_hour_bucket(module.datetime.now()))


@pytest.mark.parametrize('patch', [
    {'source_family': 'inspection_data_family'},
    {'source_family': 'electrical_inspection_family'},
    {'building': 'D楼'},
    {'schema_version': 2},
    {'report_name': '电气巡检表'},
    {'query_end': 'invalid'},
    {'value_time': '2026-01-01 00:00:00'},
    {'rows': [['位置', '设备', '测点']]},
])
def test_api_requires_explicit_matching_contract(tmp_path, patch):
    internal = SnapshotSources(inspection_datasets(tmp_path))
    internal.entries[('E楼', 'hvac_inspection_family')].update(patch)
    cloud = Cloud()
    service = InspectionDataUploadService({'inspection_data_upload': {'wait_source_timeout_sec': 0}},
                                         internal_client=internal, bitable_client=cloud)
    with pytest.raises(ValueError, match='未清空多维表'):
        service.run()
    assert cloud.events == []


@pytest.mark.parametrize('pair', SOURCE_PAIRS)
def test_valid_points_from_other_discipline_cannot_satisfy_preflight(tmp_path, monkeypatch, pair):
    service, cloud, backups = prepared(tmp_path, monkeypatch)
    building, family = pair
    other = next(d for d in load_catalog() if d['building'] == building and d['discipline'] != module.SOURCE_FAMILIES[family]['discipline'])
    point = other['points'][0]
    service.test_datasets[building][family]['rows'] = [[other['position'], other['device'], point['field'], '55']]
    with pytest.raises(ValueError, match='没有可识别'):
        service.run()
    assert cloud.events == [] and backups == []


@pytest.mark.parametrize('dry_run', [False, True])
def test_split_result_reports_four_files_and_both_timestamps(tmp_path, monkeypatch, dry_run):
    service, cloud, _ = prepared(tmp_path, monkeypatch)
    result = service.run(dry_run=dry_run)
    assert result['source_file_count'] == 4
    assert result['source_families'] == {'电气': 'electrical_inspection_family', '暖通': 'hvac_inspection_family'}
    assert 'source_family' not in result
    assert result['uploaded_count'] == (0 if dry_run else 983)
    for building, details in result['results'].items():
        assert set(details['sources']) == set(module.SOURCE_FAMILIES)
        assert 'source_time' not in details and 'value_time' not in details
        for index, (family, contract) in enumerate(module.SOURCE_FAMILIES.items()):
            metadata = details['sources'][family]
            assert metadata['report_name'] == contract['report_name']
            assert metadata['label'] == contract['label']
            assert metadata['source_family'] == family
            assert metadata['source_file'] == f"巡检接口/{contract['report_name']}"
            assert metadata['source_time'] == f'2026-10-27 10:0{index*5}:00'
            assert metadata['value_time'] == f'2026-10-27 09:{40+index*5}:00'
            assert metadata['matched_devices'] == 1
            if not dry_run:
                rows = [r['fields'] for r in cloud.rows[module.DEFAULT_CONFIG['tables'][building]]]
                rows = [r for r in rows if r['专业'] == contract['discipline']]
                assert rows and all(r['源文件'] == metadata['source_file'] for r in rows)
                expected_time = int(datetime.strptime(metadata['source_time'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=module.LOCAL_TZ).timestamp()*1000)
                assert all(r['源文件时间'] == expected_time for r in rows)
    if dry_run:
        assert cloud.events == []


def test_fixed_family_mapping_ignores_legacy_and_crossmapped_config(tmp_path):
    internal = SnapshotSources(inspection_datasets(tmp_path))
    service = InspectionDataUploadService({'inspection_data_upload': {
        'source_family': 'inspection_data_family',
        'source_families': {'电气': 'hvac_inspection_family', '暖通': 'electrical_inspection_family'},
    }}, internal_client=internal, bitable_client=Cloud())
    result = service.run(dry_run=True)
    assert 'source_family' not in service.cfg
    assert result['source_families'] == module.DEFAULT_CONFIG['source_families']
    assert set(internal.entries) == set(SOURCE_PAIRS)


def test_other_discipline_readings_do_not_fill_missing_devices(tmp_path, monkeypatch):
    service, cloud, _ = prepared(tmp_path, monkeypatch)
    catalog = load_catalog()
    for building, family in SOURCE_PAIRS:
        discipline = module.SOURCE_FAMILIES[family]['discipline']
        own = next(d for d in catalog if d['building'] == building and d['discipline'] == discipline)
        other = [d for d in catalog if d['building'] == building and d['discipline'] != discipline][1]
        service.test_datasets[building][family]['rows'] = [
            [own['position'], own['device'], own['points'][0]['field'], '36'],
            [other['position'], other['device'], other['points'][0]['field'], '99'],
        ]
    service.run()
    for building in ['D楼', 'E楼']:
        rows = [r['fields'] for r in cloud.rows[module.DEFAULT_CONFIG['tables'][building]]]
        for discipline in ['电气', '暖通']:
            device = [d for d in catalog if d['building'] == building and d['discipline'] == discipline][1]
            row = next(r for r in rows if r['专业'] == discipline and r['设备编号'] == device['device'] and r['位置'] == device['position'])
            assert all(p['field'] not in row for p in device['points'])
            assert '启停状态' not in row
            assert row['数据完整性'] == '缺少源数据'


def test_e_old_record_preflight_failure_prevents_d_clear(tmp_path, monkeypatch):
    service, cloud, _ = prepared(tmp_path, monkeypatch)
    cloud.rows[module.DEFAULT_CONFIG['tables']['E楼']] = [{'fields': {'设备编号': 'old'}}]
    with pytest.raises(RuntimeError, match='旧记录ID不完整'):
        service.run()
    assert cloud.events == []


def test_e_backup_failure_prevents_d_clear(tmp_path, monkeypatch):
    service, cloud, _ = prepared(tmp_path, monkeypatch)
    original = module.atomic_write_text
    def write(path, content):
        if path.stem == module.DEFAULT_CONFIG['tables']['E楼']:
            raise OSError('disk full')
        original(path, content)
    monkeypatch.setattr(module, 'atomic_write_text', write)
    with pytest.raises(OSError, match='disk full'):
        service.run()
    assert cloud.events == []


@pytest.fixture
def polling_clock(monkeypatch):
    class Clock:
        elapsed = 0.0
        def monotonic(self):
            return self.elapsed
        def sleep(self, seconds):
            self.elapsed += seconds
    clock = Clock()
    class WallClock(datetime):
        @classmethod
        def now(cls, tz=None):
            value = cls(2026, 10, 30, 10, 0, 0) + timedelta(seconds=clock.elapsed)
            return value.replace(tzinfo=tz) if tz else value
    monkeypatch.setattr(module, 'time', clock)
    monkeypatch.setattr(module, 'datetime', WallClock)
    return clock


def test_refresh_retries_only_missing_family_and_building(tmp_path, polling_clock):
    internal = SnapshotSources(inspection_datasets(tmp_path))
    missing = [('D楼', 'electrical_inspection_family'), ('E楼', 'hvac_inspection_family')]
    saved = {pair: internal.entries.pop(pair) for pair in missing}
    def refresh(**kwargs):
        internal.refreshes.append(kwargs)
        family = kwargs['source_family']
        if family == 'hvac_inspection_family' and sum(r['source_family'] == family for r in internal.refreshes) == 1:
            raise RuntimeError('connection refused')
        for building in kwargs['buildings']:
            internal.entries[(building, family)] = saved[(building, family)]
    internal.refresh_latest_source_cache = refresh
    service = InspectionDataUploadService({'inspection_data_upload': {'wait_source_timeout_sec': 5, 'wait_source_poll_sec': 1}},
                                         internal_client=internal, bitable_client=Cloud())
    result = service.run(dry_run=True)
    assert result['source_file_count'] == 4
    assert [(r['source_family'], r['buildings']) for r in internal.refreshes] == [
        ('electrical_inspection_family', ['D楼']), ('hvac_inspection_family', ['E楼']), ('hvac_inspection_family', ['E楼']),
    ]
    assert all(r['target_bucket_key'] == '2026-10-30 10' for r in internal.refreshes)


def test_accepted_refresh_is_not_repeated_while_waiting(tmp_path, polling_clock):
    internal = SnapshotSources(inspection_datasets(tmp_path))
    internal.entries.pop(('E楼', 'hvac_inspection_family'))
    service = InspectionDataUploadService({'inspection_data_upload': {'wait_source_timeout_sec': 3, 'wait_source_poll_sec': 1}},
                                         internal_client=internal, bitable_client=Cloud())
    with pytest.raises(RuntimeError, match='E楼/暖通巡检源文件'):
        service.run()
    assert internal.refreshes == [{'source_family': 'hvac_inspection_family', 'buildings': ['E楼'], 'target_bucket_key': '2026-10-30 10'}]


def test_scheduled_upload_waits_for_all_files_from_same_bucket(tmp_path, polling_clock):
    internal = SnapshotSources(inspection_datasets(tmp_path))
    old = internal.entries[('E楼', 'hvac_inspection_family')]
    set_dataset_end(old, datetime(2026, 10, 30, 8, 5))
    cloud = Cloud()
    service = InspectionDataUploadService(
        {'inspection_data_upload': {'wait_source_timeout_sec': 3, 'wait_source_poll_sec': 1}},
        internal_client=internal, bitable_client=cloud,
    )
    with pytest.raises(RuntimeError, match='E楼/暖通巡检源文件'):
        service.run(source_date='2026-10-30', source_bucket_key='2026-10-30 10')
    assert not cloud.events
    assert internal.refreshes == [
        {'source_family': 'hvac_inspection_family', 'buildings': ['E楼'], 'target_bucket_key': '2026-10-30 10'},
    ]
    set_dataset_end(old, datetime(2026, 10, 30, 10, 0, 3))
    assert service.run(source_date='2026-10-30', source_bucket_key='2026-10-30 10', dry_run=True)['source_file_count'] == 4


def test_upload_starts_only_after_last_current_bucket_source_arrives(tmp_path, polling_clock, monkeypatch):
    internal = SnapshotSources(inspection_datasets(tmp_path))
    last = internal.entries.pop(('E楼', 'hvac_inspection_family'))
    cloud = Cloud()
    original_snapshot = internal.get_inspection_snapshot

    def snapshot(**kwargs):
        if polling_clock.elapsed < 4:
            assert not cloud.events
        else:
            internal.entries[('E楼', 'hvac_inspection_family')] = last
        return original_snapshot(**kwargs)

    internal.get_inspection_snapshot = snapshot
    monkeypatch.setattr(module, 'BACKUP_ROOT', tmp_path / 'backups')
    service = InspectionDataUploadService(
        {'inspection_data_upload': {'wait_source_timeout_sec': 10, 'wait_source_poll_sec': 1}},
        internal_client=internal, bitable_client=cloud,
    )
    result = service.run(source_date='2026-10-30', source_bucket_key='2026-10-30 10')
    assert polling_clock.elapsed >= 4
    assert result['status'] == 'success'
    assert [op for op, _ in cloud.events] == ['create', 'create', 'delete', 'update', 'delete', 'update']
    assert internal.refreshes == [
        {'source_family': 'hvac_inspection_family', 'buildings': ['E楼'], 'target_bucket_key': '2026-10-30 10'},
    ]


def test_expired_scheduled_slot_does_not_refresh_or_clear(tmp_path, polling_clock):
    internal = SnapshotSources(inspection_datasets(tmp_path))
    cloud = Cloud()
    service = InspectionDataUploadService({}, internal_client=internal, bitable_client=cloud)
    with pytest.raises(RuntimeError, match='巡检时段已结束'):
        service.run(source_date='2026-10-30', source_bucket_key='2026-10-30 08')
    assert not internal.refreshes
    assert not cloud.events


def test_round_targets_validate_positions_and_preserve_legacy():
    from app.modules.inspection_data.service import point_settings

    book = next(name for name, sheets in point_settings.inspection_target_options('D楼').items()
                if '柴发巡检表' in sheets)
    keys = ('day_1', 'day_2', 'night_1', 'night_2')
    positions = dict(zip(keys, ('b23', 'b24', 'b25', 'b26')))
    target = {'workbook': book, 'sheet': '柴发巡检表', 'cell': '', 'round_cells': positions}
    normalized = point_settings._normalize_targets('D楼', {'7_0_1': ''}, {'7_0_1': target})
    assert list(normalized['7_0_1']['round_cells'].values()) == ['B23', 'B24', 'B25', 'B26']
    legacy = {'workbook': book, 'sheet': '柴发巡检表', 'cell': 'B6'}
    assert point_settings._normalize_targets('D楼', {'7_0_2': ''}, {'7_0_2': legacy})['7_0_2'] == legacy
    with pytest.raises(ValueError, match='不能重复'):
        point_settings._normalize_targets('D楼', {'7_0_1': ''}, {'7_0_1': {
            **target, 'round_cells': dict.fromkeys(keys, 'B23'),
        }})
    with pytest.raises(ValueError, match='已分配'):
        point_settings._validate_target_collisions({
            'electrical': {'targets': normalized},
            'hvac': {'targets': {'8_0_1': {**target, 'round_cells': dict.fromkeys(keys, 'B23')}}},
        })


def test_cold_room_accepts_one_fixed_cell_only():
    from app.modules.inspection_data.service import point_settings

    sheet = '暖通冷机系统巡检表'
    book = next(name for name, sheets in point_settings.inspection_target_options('D楼').items() if sheet in sheets)
    empty = {'day_1': 'C8', 'day_2': '', 'night_1': '', 'night_2': ''}
    target = {'workbook': book, 'sheet': sheet, 'cell': '', 'round_cells': empty}
    saved = point_settings._normalize_targets('D楼', {'7_0_1': ''}, {'7_0_1': target})['7_0_1']
    assert set(saved['round_cells'].values()) == {'C8'}
    with pytest.raises(ValueError, match='只能配置一个'):
        point_settings._normalize_targets('D楼', {'7_0_1': ''}, {'7_0_1': {
            **target, 'round_cells': {**empty, 'night_1': 'C9'},
        }})


def test_round_targets_sync_without_promoting_or_clearing_legacy():
    from app.modules.inspection_data.service import point_config_bitable

    family = 'electrical_inspection_family'
    rounds = {'day_1': 'B23', 'day_2': 'B24', 'night_1': 'B25', 'night_2': 'B26'}
    groups = {family: {'baseline': {'7_0_1': {}, '7_0_2': {}}, 'points': {'7_0_1': '', '7_0_2': ''},
                       'remove': [], 'targets': {
                           '7_0_1': {'workbook': 'book', 'sheet': 'sheet', 'cell': '', 'round_cells': rounds},
                           '7_0_2': {'workbook': 'book', 'sheet': 'sheet', 'cell': 'B6'},
                       }}}
    desired = point_config_bitable._expected_rows(groups)
    configured, legacy = desired[('电气', '7_0_1')], desired[('电气', '7_0_2')]
    assert [configured[name] for name in ('白1目标单元格', '白2目标单元格', '夜1目标单元格', '夜2目标单元格')] == list(rounds.values())
    assert legacy['目标单元格'] == 'B6'
    assert all(name not in legacy for name in ('白1目标单元格', '白2目标单元格', '夜1目标单元格', '夜2目标单元格'))


@pytest.mark.parametrize('disabled', [False, True])
def test_explicitly_removed_target_clears_round_fields(disabled):
    from app.modules.inspection_data.service import point_config_bitable

    pid = '7_0_1'
    group = {'baseline': {pid: {}}, 'points': {} if disabled else {pid: ''},
             'remove': [pid] if disabled else []}
    row = point_config_bitable._expected_rows({'electrical_inspection_family': group})[('电气', pid)]
    assert all(row[name] == '' for name in ('白1目标单元格', '白2目标单元格', '夜1目标单元格', '夜2目标单元格'))
    assert row['启用'] is not disabled


def test_round_targets_roundtrip_and_leave_other_remote_rounds_unchanged():
    from app.modules.inspection_data.service import point_config_bitable as cfg

    cloud = Mock()
    cloud.list_fields.return_value = [{'field_name': '文本', 'type': 1, 'is_primary': True},
                                      *({'field_name': k, 'type': t} for k, t in cfg._FIELD_TYPES.items())]
    tables = {table: [] for table in cfg.POINT_CONFIG_TABLES.values()}
    cloud.list_records.side_effect = lambda table, **_: copy.deepcopy(tables[table])
    def create(table, rows):
        tables[table].extend({'record_id': f'{table}-{i}', 'fields': dict(row)} for i, row in enumerate(rows))
    def update(table, rows):
        for row in rows:
            next(item for item in tables[table] if item['record_id'] == row['record_id'])['fields'].update(row['fields'])
    cloud.batch_create_records.side_effect = create
    cloud.batch_update_records.side_effect = update
    groups = {b: {f: {'baseline': {'7_0_1': {'name': '点', 'path': '路径'}}, 'points': {}, 'remove': []}
                  for f in module.SOURCE_FAMILIES} for b in cfg.POINT_CONFIG_TABLES}
    cfg.sync_point_config_bitable({}, groups, client=cloud)
    table = cfg.POINT_CONFIG_TABLES['D楼']
    untouched = next(r for r in tables[table] if r['fields']['专业'] == '暖通')
    untouched['fields']['白1目标单元格'] = 'Z99'
    group = groups['D楼']['electrical_inspection_family']
    rounds = {'day_1': 'B23', 'day_2': 'B24', 'night_1': 'B25', 'night_2': 'B26'}
    group.update(points={'7_0_1': ''}, targets={'7_0_1': {
        'workbook': 'book', 'sheet': 'sheet', 'cell': '', 'round_cells': rounds,
    }})
    cfg.sync_point_config_bitable({}, groups, buildings=('D楼',), client=cloud)
    assert untouched['fields']['白1目标单元格'] == 'Z99'
    restored = cfg.read_point_config_bitable({}, client=cloud)
    assert restored['D楼']['electrical_inspection_family']['targets']['7_0_1']['round_cells'] == rounds
    group['targets'] = {}
    cfg.sync_point_config_bitable({}, groups, buildings=('D楼',), client=cloud)
    removed = next(r['fields'] for r in tables[table] if r['fields']['专业'] == '电气')
    assert all(removed[name] == '' for name in ('白1目标单元格', '白2目标单元格', '夜1目标单元格', '夜2目标单元格'))
    assert untouched['fields']['白1目标单元格'] == 'Z99'


def test_point_detail_includes_only_explicit_round_targets(tmp_path):
    doc = inspection_datasets(tmp_path)['D楼']['electrical_inspection_family']
    doc['point_ids'] = ['7_0_1']
    rounds = {'day_1': 'B23', 'day_2': 'B24', 'night_1': 'B25', 'night_2': 'B26'}
    row = InspectionDataUploadService._point_detail_rows(doc, '电气', {}, {
        '7_0_1': {'workbook': 'book', 'sheet': 'sheet', 'cell': '', 'round_cells': rounds},
    })[0]
    assert [row[name] for name in ('白1目标单元格', '白2目标单元格', '夜1目标单元格', '夜2目标单元格')] == list(rounds.values())


def test_rich_text_record_readback_keeps_create_then_cleanup_order(tmp_path, monkeypatch):
    service, cloud, _ = prepared(tmp_path, monkeypatch)
    list_records = cloud.list_records

    def read(table, **kwargs):
        rows = list_records(table, **kwargs)
        return [{
            **row,
            "fields": {**row["fields"], "设备编号": [{"text": row["fields"]["设备编号"], "type": "text"}]},
        } for row in rows]

    monkeypatch.setattr(cloud, "list_records", read)
    assert service.run()["uploaded_count"] == 983
    assert [op for op, _ in cloud.events] == ["create", "create", "delete", "update", "delete", "update"]


def test_numeric_field_decimal_comparison_preserves_strict_text_and_precision():
    numeric = {'电压(kV)', '电流(A)'}
    fields = ['设备编号', '电压(kV)', '电流(A)']
    expected = {'设备编号': '001', '电压(kV)': 10.31, '电流(A)': 119.68}
    compare = module._comparison_record_identity
    assert compare(expected, fields, numeric) == compare(
        {'设备编号': '001', '电压(kV)': '10.310', '电流(A)': [{'text': '119.68'}]}, fields, numeric)
    assert compare(expected, fields, numeric) != compare(
        {'设备编号': '1', '电压(kV)': '10.31', '电流(A)': '119.68'}, fields, numeric)
    assert compare(expected, fields, numeric) != compare(
        {'设备编号': 1, '电压(kV)': '10.31', '电流(A)': '119.68'}, fields, numeric)
    assert compare(expected, fields, numeric) != compare(
        {'设备编号': '001', '电压(kV)': '10.311', '电流(A)': '119.68'}, fields, numeric)
    assert compare({'电压(kV)': 1}, ['电压(kV)'], numeric) != compare({'电压(kV)': True}, ['电压(kV)'], numeric)
    for invalid in ('NaN', 'Infinity', '1_0', 'not-a-number'):
        assert compare({'电压(kV)': 10}, ['电压(kV)'], numeric) != compare({'电压(kV)': invalid}, ['电压(kV)'], numeric)
    assert module._record_identity({'设备编号': '001'}, ['设备编号']) != module._record_identity({'设备编号': 1}, ['设备编号'])


def test_numeric_fields_read_back_as_text_during_both_checks(tmp_path, monkeypatch):
    service, cloud, _ = prepared(tmp_path, monkeypatch)
    for building in ('D楼', 'E楼'):
        entry = service.test_datasets[building]['electrical_inspection_family']
        entry['rows'][0][3] = '10.31'
        entry['rows'].append([None, None, '电流(A)', '119.68'])
    numeric = {item['field_name'] for item in field_definitions() if item['type'] == 2}
    assert {'电压(kV)', '电流(A)'} <= numeric and '设备编号' not in numeric and '测点值' not in numeric
    read = cloud.list_records
    def as_text(table, **kwargs):
        rows = copy.deepcopy(read(table, **kwargs))
        for record in rows:
            for field in numeric & record['fields'].keys():
                if isinstance(record['fields'][field], (int, float)):
                    record['fields'][field] = str(record['fields'][field])
        return rows
    monkeypatch.setattr(cloud, 'list_records', as_text)
    result = service.run()
    assert result['status'] == 'success'
    for building in ('D楼', 'E楼'):
        values = [row['fields'] for row in cloud.rows[module.DEFAULT_CONFIG['tables'][building]]]
        assert any(row.get('电压(kV)') == 10.31 and row.get('电流(A)') == 119.68 for row in values)
        assert service.read_completed_batch(building=building, upload_time=result['results'][building]['upload_time'])


def test_readback_mismatch_reports_only_three_safe_field_formats_and_rolls_back(tmp_path, monkeypatch):
    service, cloud, _ = prepared(tmp_path, monkeypatch)
    original = copy.deepcopy(cloud.rows)
    read = cloud.list_records
    secret = 'credential-should-never-appear'
    def mismatched(table, **kwargs):
        rows = copy.deepcopy(read(table, **kwargs))
        if not kwargs.get('field_names'):
            for record in rows:
                if record['record_id'].startswith('old'):
                    continue
                record['fields'].update({'专业': 42, '位置': secret, '巡检表单': [secret], '设备编号': {'token': secret}})
                break
        return rows
    monkeypatch.setattr(cloud, 'list_records', mismatched)
    logs = []
    with pytest.raises(RuntimeError) as error:
        service.run(log=logs.append)
    message = str(error.value)
    assert '未删除旧记录' in message and '上传后回查设备、位置或上传批次不一致' in message
    assert message.count('期望格式=') == 3 and message.count('实际格式=') == 3
    assert '字段值已隐藏' in message and secret not in message
    diagnostic_logs = [line for line in logs if '上传回查字段差异' in line]
    assert len(diagnostic_logs) == 1 and diagnostic_logs[0].count('期望格式=') == 3
    assert secret not in diagnostic_logs[0]
    assert cloud.rows == original
    assert all(record_id.startswith('old') for rows in cloud.rows.values() for record_id in (row['record_id'] for row in rows))


def test_mismatch_diagnostic_does_not_pair_unordered_rows():
    expected = [{'设备编号': 'a', '位置': 'A'}, {'设备编号': 'b', '位置': 'B'}]
    observed = [{'设备编号': 'a', '位置': 'B'}, {'设备编号': 'b', '位置': 'A'}]
    assert module._record_mismatch_diagnostic(expected, observed, ['位置', '设备编号'], set()) == '字段组合不一致，单字段分布相同'


def test_completion_marker_requires_exact_current_batch_and_counts(tmp_path, monkeypatch):
    service, cloud, _ = prepared(tmp_path, monkeypatch)
    result = service.run()
    for building in ('D楼', 'E楼'):
        info = result['results'][building]
        table = info['table_id']
        expected = info['batch_completion']
        upload_time = info['upload_time']
        assert service.read_completed_batch(building=building, upload_time=upload_time) == expected
        assert expected['device_count'] + expected['point_count'] == info['uploaded_count']
        assert expected['point_count'] == info['point_count']
        assert service.read_completed_batch(building=building, upload_time=upload_time + 1) is None
        original = copy.deepcopy(cloud.rows[table])
        for mutation in ('missing', 'duplicate', 'replaced_id', 'foreign', 'wrong_type', 'wrong_building', 'second_marker', 'wrong_count'):
            rows = copy.deepcopy(original)
            marked = next(r for r in rows if str(r['fields'].get('备注', '')).startswith(module.INSPECTION_BATCH_PREFIX))
            if mutation == 'missing':
                rows.pop()
            elif mutation == 'duplicate':
                rows.append(copy.deepcopy(rows[-1]))
            elif mutation == 'replaced_id':
                rows[-1]['record_id'] = 'another-record'
            elif mutation == 'foreign':
                rows.append({'record_id': 'old', 'fields': {'上传日期': upload_time - 1}})
            elif mutation == 'wrong_type':
                rows[-1]['fields']['记录类型'] = '其他'
            elif mutation == 'wrong_building':
                rows[-1]['fields']['楼栋'] = 'A楼'
            elif mutation == 'second_marker':
                next(r for r in rows if r is not marked and r['fields']['记录类型'] == '设备巡检')['fields']['备注'] = marked['fields']['备注']
            else:
                marker = json.loads(marked['fields']['备注'][len(module.INSPECTION_BATCH_PREFIX):])
                marker['point_count'] += 1
                marked['fields']['备注'] = module.INSPECTION_BATCH_PREFIX + json.dumps(marker)
            cloud.rows[table] = rows
            assert service.read_completed_batch(building=building, upload_time=upload_time) is None, mutation
        cloud.rows[table] = original


def test_completion_reader_rejects_concurrent_snapshot_change(tmp_path, monkeypatch):
    service, cloud, _ = prepared(tmp_path, monkeypatch)
    result = service.run()
    info = result['results']['D楼']
    original = cloud.list_records
    reads = 0
    def changing(table, **kwargs):
        nonlocal reads
        reads += 1
        rows = copy.deepcopy(original(table, **kwargs))
        if reads == 2:
            rows[0]['fields']['位置'] = 'changed'
        return rows
    monkeypatch.setattr(cloud, 'list_records', changing)
    assert service.read_completed_batch(building='D楼', upload_time=info['upload_time']) is None


def test_failed_delete_never_publishes_affected_building_marker(tmp_path, monkeypatch):
    service, cloud, _ = prepared(tmp_path, monkeypatch)
    delete = cloud.batch_delete_records
    e_table = module.DEFAULT_CONFIG['tables']['E楼']
    def fail_e(table, ids, **kwargs):
        if table == e_table:
            raise RuntimeError('delete failed')
        return delete(table, ids, **kwargs)
    monkeypatch.setattr(cloud, 'batch_delete_records', fail_e)
    with pytest.raises(RuntimeError):
        service.run()
    assert not any(str(row['fields'].get('备注', '')).startswith(module.INSPECTION_BATCH_PREFIX) for row in cloud.rows[e_table])
    assert ('update', e_table) not in cloud.events


def test_marker_readback_failure_clears_marker_and_retry_uses_new_batch(tmp_path, monkeypatch):
    service, cloud, _ = prepared(tmp_path, monkeypatch)
    original = service.read_completed_batch
    monkeypatch.setattr(service, 'read_completed_batch', lambda **kwargs: None)
    with pytest.raises(RuntimeError, match='完成标记回查不一致'):
        service.run()
    for rows in cloud.rows.values():
        assert not any(str(row['fields'].get('备注', '')).startswith(module.INSPECTION_BATCH_PREFIX) for row in rows)
    old_time = next(row['fields']['上传日期'] for row in cloud.rows[module.DEFAULT_CONFIG['tables']['D楼']])
    monkeypatch.setattr(service, 'read_completed_batch', original)
    result = service.run()
    info = result['results']['D楼']
    assert info['upload_time'] != old_time
    assert service.read_completed_batch(building='D楼', upload_time=old_time) is None
    assert service.read_completed_batch(building='D楼', upload_time=info['upload_time']) == info['batch_completion']


def test_marker_update_timeout_after_commit_attempts_invalidation(tmp_path, monkeypatch):
    service, cloud, _ = prepared(tmp_path, monkeypatch)
    update = cloud.update_record
    def timeout_after_commit(table, record_id, fields):
        result = update(table, record_id, fields)
        if fields.get('备注', '').startswith(module.INSPECTION_BATCH_PREFIX):
            raise RuntimeError('response lost')
        return result
    monkeypatch.setattr(cloud, 'update_record', timeout_after_commit)
    with pytest.raises(RuntimeError, match='response lost'):
        service.run()
    assert not any(str(row['fields'].get('备注', '')).startswith(module.INSPECTION_BATCH_PREFIX)
                   for rows in cloud.rows.values() for row in rows)


def test_completion_marker_rich_text_is_readable(tmp_path, monkeypatch):
    service, cloud, _ = prepared(tmp_path, monkeypatch)
    info = service.run()['results']['D楼']
    original = cloud.list_records
    def rich_text(table, **kwargs):
        rows = copy.deepcopy(original(table, **kwargs))
        for row in rows:
            text = row['fields'].get('备注')
            if text:
                row['fields']['备注'] = [{'text': text, 'type': 'text'}]
        return rows
    monkeypatch.setattr(cloud, 'list_records', rich_text)
    assert service.read_completed_batch(building='D楼', upload_time=info['upload_time']) == info['batch_completion']


@pytest.mark.parametrize('all_occupied', [False, True])
def test_completion_marker_only_uses_empty_device_remark(tmp_path, monkeypatch, all_occupied):
    service, cloud, _ = prepared(tmp_path, monkeypatch)
    create = cloud.batch_create_records
    d_table = module.DEFAULT_CONFIG['tables']['D楼']
    untouched_ids = []
    def with_business_notes(table, rows, **kwargs):
        result = create(table, rows, **kwargs)
        if table == d_table:
            devices = sorted((row for row in cloud.rows[table]
                              if row['fields'].get('记录类型') == '设备巡检'), key=lambda row: row['record_id'])
            occupied = devices if all_occupied else devices[:-1]
            for row in occupied:
                row['fields'] = {**row['fields'], '备注': '业务备注'}
                untouched_ids.append(row['record_id'])
        return result
    monkeypatch.setattr(cloud, 'batch_create_records', with_business_notes)
    if all_occupied:
        with pytest.raises(RuntimeError, match='备注均非空'):
            service.run()
        assert ('update', d_table) not in cloud.events
        assert len(cloud.rows[d_table]) == 460
        assert all(row['fields'].get('备注') == '业务备注' for row in cloud.rows[d_table]
                   if row['fields'].get('记录类型') == '设备巡检')
    else:
        result = service.run()
        assert service.read_completed_batch(building='D楼', upload_time=result['results']['D楼']['upload_time'])
        for row in cloud.rows[d_table]:
            if row['record_id'] in untouched_ids:
                assert row['fields']['备注'] == '业务备注'
        markers = [row for row in cloud.rows[d_table]
                   if str(row['fields'].get('备注', '')).startswith(module.INSPECTION_BATCH_PREFIX)]
        assert len(markers) == 1 and markers[0]['record_id'] not in untouched_ids


def test_marker_rollback_does_not_clear_changed_business_remark(tmp_path, monkeypatch):
    service, cloud, _ = prepared(tmp_path, monkeypatch)
    d_table = module.DEFAULT_CONFIG['tables']['D楼']
    def changed_after_write(**kwargs):
        marker = next(row for row in cloud.rows[d_table]
                      if str(row['fields'].get('备注', '')).startswith(module.INSPECTION_BATCH_PREFIX))
        marker['fields']['备注'] = '人工更新的业务备注'
        return None
    monkeypatch.setattr(service, 'read_completed_batch', changed_after_write)
    with pytest.raises(RuntimeError, match='完成标记回查不一致'):
        service.run()
    assert any(row['fields'].get('备注') == '人工更新的业务备注' for row in cloud.rows[d_table])
    assert cloud.events.count(('update', d_table)) == 1


def test_marker_write_rechecks_carrier_remark(tmp_path, monkeypatch):
    service, cloud, _ = prepared(tmp_path, monkeypatch)
    d_table = module.DEFAULT_CONFIG['tables']['D楼']
    read = cloud.list_records
    after_delete_reads = 0
    def concurrently_changed(table, **kwargs):
        nonlocal after_delete_reads
        rows = read(table, **kwargs)
        if table == d_table and rows and not any(row['record_id'].startswith('old') for row in rows):
            after_delete_reads += 1
            if after_delete_reads == 2:
                carrier = min((row for row in rows if row['fields'].get('记录类型') == '设备巡检'),
                              key=lambda row: row['record_id'])
                carrier['fields']['备注'] = '并发写入的业务备注'
        return rows
    monkeypatch.setattr(cloud, 'list_records', concurrently_changed)
    with pytest.raises(RuntimeError, match='设备备注已变化'):
        service.run()
    assert ('update', d_table) not in cloud.events
    assert any(row['fields'].get('备注') == '并发写入的业务备注' for row in cloud.rows[d_table])


def test_historical_missing_source_does_not_request_current_hour(tmp_path, polling_clock):
    internal = SnapshotSources(inspection_datasets(tmp_path))
    for entry in internal.entries.values():
        set_dataset_end(entry, datetime(2026, 10, 29, 10))
    internal.entries.pop(('E楼', 'hvac_inspection_family'))
    service = InspectionDataUploadService({}, internal_client=internal, bitable_client=Cloud())
    with pytest.raises(RuntimeError, match='E楼/暖通巡检源文件'):
        service.run(source_date='2026-10-29')
    assert internal.refreshes == []


def test_stale_source_refreshes_only_its_pair(tmp_path, polling_clock):
    internal = SnapshotSources(inspection_datasets(tmp_path))
    entry = internal.entries[('E楼', 'hvac_inspection_family')]
    set_dataset_end(entry, datetime(2026, 10, 30, 7))
    def refresh(**kwargs):
        internal.refreshes.append(kwargs)
        set_dataset_end(entry, datetime(2026, 10, 30, 10))
    internal.refresh_latest_source_cache = refresh
    service = InspectionDataUploadService({}, internal_client=internal, bitable_client=Cloud())
    assert service.run(dry_run=True)['source_file_count'] == 4
    assert len(internal.refreshes) == 1
    assert internal.refreshes[0]['source_family'] == 'hvac_inspection_family'
    assert internal.refreshes[0]['buildings'] == ['E楼']


def test_wait_crossing_midnight_never_refreshes_wrong_date(tmp_path, polling_clock):
    polling_clock.elapsed = 14*60*60 - 1
    internal = SnapshotSources(inspection_datasets(tmp_path))
    internal.entries.pop(('E楼', 'hvac_inspection_family'))
    service = InspectionDataUploadService({'inspection_data_upload': {'wait_source_timeout_sec': 3, 'wait_source_poll_sec': 1}},
                                         internal_client=internal, bitable_client=Cloud())
    with pytest.raises(RuntimeError, match='E楼/暖通巡检源文件'):
        service.run()
    assert internal.refreshes == [{'source_family': 'hvac_inspection_family', 'buildings': ['E楼'], 'target_bucket_key': '2026-10-30 22'}]


def test_lost_queued_refresh_is_retried_after_cooldown(tmp_path, polling_clock):
    internal = SnapshotSources(inspection_datasets(tmp_path))
    missing = ('E楼', 'hvac_inspection_family')
    entry = internal.entries.pop(missing)
    times = []

    def refresh(**kwargs):
        times.append(polling_clock.elapsed)
        assert kwargs['buildings'] == [missing[0]] and kwargs['source_family'] == missing[1]
        if len(times) == 2:
            internal.entries[missing] = entry
        return {'accepted': 1, 'results': [{'reason': 'queued'}]}

    internal.refresh_latest_source_cache = refresh
    service = InspectionDataUploadService({'inspection_data_upload': {'wait_source_timeout_sec': 70, 'wait_source_poll_sec': 1}},
                                         internal_client=internal, bitable_client=Cloud())
    assert service.run(dry_run=True)['source_file_count'] == 4
    assert times == [0, 60]


def test_stale_point_config_never_dispatches_from_external(tmp_path):
    from app.modules.inspection_data.service import point_settings

    config = {'paths': {'runtime_state_root': str(tmp_path)}}
    old = {'D楼': {'electrical_inspection_family': {'points': {'7_0_1': ''}, 'remove': []}}}
    new = {'D楼': {'electrical_inspection_family': {'points': {'7_0_2': ''}, 'remove': []}}}
    point_settings._save_settings(config, old)
    first = point_settings._settings_snapshot(config)[1]
    point_settings._save_settings(config, new)
    assert point_settings._settings_snapshot(config)[1] > first
    bridge = Mock()
    with pytest.raises(point_settings.PointSettingsConflict):
        point_settings.sync_point_settings(config, old, bridge=bridge)
    bridge.put_inspection_point_rules.assert_not_called()


def test_settings_write_lock_is_visible_to_independent_connection(tmp_path):
    import sqlite3
    from contextlib import closing
    from app.modules.inspection_data.service import point_settings

    config = {'paths': {'runtime_state_root': str(tmp_path)}}
    with closing(sqlite3.connect(tmp_path / 'inspection_point_settings.lock.db', timeout=0)) as worker:
        with point_settings._settings_write_lock(config):
            with pytest.raises(sqlite3.OperationalError, match='locked'):
                worker.execute('BEGIN IMMEDIATE')
        worker.execute('BEGIN IMMEDIATE')


@pytest.mark.parametrize('building', ['D楼', 'E楼'])
def test_explicit_own_building_label_is_accepted_and_other_building_rejected(tmp_path, building):
    path = source(tmp_path/'labeled.xlsx', [[f'{building} 西区', f'{building[0]}-144-TR-101', 'L1温度(℃)', 35]])
    result = InspectionDataUploadService.parse_workbook(path, building=building)
    assert result['matched_devices'] == 1
    other = 'D楼' if building == 'E楼' else 'E楼'
    with pytest.raises(ValueError, match='其他楼栋'):
        InspectionDataUploadService.parse_workbook(path, building=other)
