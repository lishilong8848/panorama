import asyncio
import concurrent.futures
import io
import os
import threading
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from PIL import Image

from app.modules.system_screenshot_capture.service import system_screenshot_capture_service as module


def config_for(tmp_path):
    return {"system_screenshot_capture": {"state_file": str(tmp_path / "capture.json"), "download_root": str(tmp_path)}}


def test_overlapping_force_request_joins_same_batch_and_cancel_is_scoped(tmp_path, monkeypatch):
    entered = threading.Event()
    completed = threading.Event()
    def capture(**kwargs):
        entered.set()
        assert kwargs["cancel_event"].wait(3)
        completed.set()
        raise asyncio.CancelledError()
    monkeypatch.setattr(module, "run_system_screenshot_capture", capture)
    container = SimpleNamespace(_system_screenshot_capture_run_lock=threading.Lock())
    first = module.submit_system_screenshot_capture(container=container, config=config_for(tmp_path), force=True)
    assert entered.wait(1)
    try:
        second = module.submit_system_screenshot_capture(container=container, config=config_for(tmp_path), force=True)
        assert second["status"] == "running"
        assert second["batch_id"] == first["batch_id"]
        assert second["started_at"] == first["started_at"]
        assert module.cancel_system_screenshot_capture(container, "other-batch")["status"] == "skipped"
    finally:
        module.cancel_system_screenshot_capture(container, first["batch_id"])
        assert completed.wait(1)
    assert container._system_screenshot_capture_run_lock.acquire(timeout=1)
    container._system_screenshot_capture_run_lock.release()


def test_retry_reuses_persisted_batch_identity(tmp_path, monkeypatch):
    captured_ids = []
    def capture(**kwargs):
        captured_ids.append(kwargs["batch_id"])
        return {"status": "success"}
    monkeypatch.setattr(module, "run_system_screenshot_capture", capture)
    container = SimpleNamespace(_system_screenshot_capture_run_lock=threading.Lock())
    first = module.submit_system_screenshot_capture(container=container, config=config_for(tmp_path), force=True, wait=True, request_id="request-1")
    container = SimpleNamespace(_system_screenshot_capture_run_lock=threading.Lock())
    second = module.submit_system_screenshot_capture(container=container, config=config_for(tmp_path), force=True, wait=True, request_id="request-1")
    assert first["batch_id"] == second["batch_id"]
    assert captured_ids[0] == captured_ids[1]
    third = module.submit_system_screenshot_capture(container=container, config=config_for(tmp_path), force=True, wait=True, request_id="request-2")
    assert third["batch_id"] != first["batch_id"]


def test_pool_submits_one_target_at_a_time(tmp_path, monkeypatch):
    targets = module._normalize_targets()[:3]
    lengths = []
    owners = []
    async def capture_targets(**kwargs):
        lengths.append(len(kwargs["building_targets"]))
        return [{"target_key": kwargs["building_targets"][0].key}]
    monkeypatch.setattr(module, "_capture_building_targets", capture_targets)
    async def run():
        tasks = []
        def submit(building, runner, *, owner):
            owners.append(owner)
            future = concurrent.futures.Future()
            async def complete():
                future.set_result(await runner(object()))
            tasks.append(asyncio.create_task(complete()))
            return future
        monkeypatch.setattr(module, "get_internal_download_browser_pool", lambda: SimpleNamespace(submit_building_job=submit, is_running=lambda: True))
        records = await module._capture_with_browser_pool(
            plan_by_building={"A楼": targets}, site_by_building={"A楼": object()},
            args=SimpleNamespace(cancel_event=threading.Event()),
            state_path=tmp_path / "state.json", download_root_path=tmp_path,
            output_dir=tmp_path, compact_date="20261006", hour_text="", emit_log=lambda _: None,
        )
        await asyncio.gather(*tasks)
        return records
    assert len(asyncio.run(run())) == 3
    assert lengths == [1, 1, 1]
    assert owners == ["system_screenshot_capture"] * 3


def test_busy_pool_retries_same_target_in_same_batch(tmp_path, monkeypatch):
    target = module._normalize_targets()[:1]
    submitted = []
    seen_batches = []

    async def capture_targets(**kwargs):
        seen_batches.append(kwargs["args"].batch_id)
        return [{"target_key": kwargs["building_targets"][0].key}]

    monkeypatch.setattr(module, "_capture_building_targets", capture_targets)

    async def run():
        tasks = []

        def submit(building, runner, *, owner):
            submitted.append((building, owner))
            future = concurrent.futures.Future()
            if len(submitted) == 1:
                future.set_exception(module.BuildingBrowserBusyError("页面繁忙"))
            else:
                async def complete():
                    future.set_result(await runner(object()))
                tasks.append(asyncio.create_task(complete()))
            return future

        monkeypatch.setattr(module, "get_internal_download_browser_pool", lambda: SimpleNamespace(submit_building_job=submit, is_running=lambda: True))
        result = await module._capture_with_browser_pool(
            plan_by_building={"A楼": target}, site_by_building={"A楼": object()},
            args=SimpleNamespace(cancel_event=threading.Event(), batch_id="batch-1"),
            state_path=tmp_path / "state.json", download_root_path=tmp_path,
            output_dir=tmp_path, compact_date="20261104", hour_text="", emit_log=lambda _: None,
        )
        await asyncio.gather(*tasks)
        return result

    assert len(asyncio.run(run())) == 1
    assert submitted == [("A楼", "system_screenshot_capture")] * 2
    assert seen_batches == ["batch-1"]


def test_failed_capture_releases_batch_lock(tmp_path, monkeypatch):
    monkeypatch.setattr(module, "run_system_screenshot_capture", Mock(side_effect=RuntimeError("capture failed")))
    container = SimpleNamespace(_system_screenshot_capture_run_lock=threading.Lock())
    with pytest.raises(RuntimeError, match="capture failed"):
        module.submit_system_screenshot_capture(container=container, config=config_for(tmp_path), wait=True)
    assert not container._system_screenshot_capture_run_lock.locked()
    assert module._load_state(tmp_path / "capture.json")["capture_batch"]["status"] == "failed"


def test_force_retry_only_captures_unfinished_targets(tmp_path, monkeypatch):
    targets = module._normalize_targets()[:2]
    path = tmp_path / "first.png"
    path.write_bytes(b"test-png")
    module._upsert_record(tmp_path / "capture.json", {
        "capture_date": "2026-10-06", "site_building": "A楼",
        "target_key": targets[0].key, "file_path": str(path),
        "status": "captured", "capture_batch_id": "batch-1",
    })
    monkeypatch.setattr(module, "_select_sites", lambda *_: [SimpleNamespace(building="A楼")])
    captured = []
    async def fill(**kwargs):
        for target in kwargs["plan_by_building"]["A楼"]:
            captured.append(target.key)
            module._upsert_record(tmp_path / "capture.json", {
                "capture_date": "2026-10-06", "site_building": "A楼",
                "target_key": target.key, "file_path": str(path),
                "status": "captured", "capture_batch_id": "batch-1",
            })
        return []
    monkeypatch.setattr(module, "_capture_with_browser_pool", fill)
    config = config_for(tmp_path)
    config["system_screenshot_capture"]["targets"] = [dict(key=item.key, label=item.label, span_id=item.span_id) for item in targets]
    result = asyncio.run(module.run_system_screenshot_capture_async(
        config=config, capture_date="2026-10-06", force=True, batch_id="batch-1",
    ))
    assert result["status"] == "success"
    assert captured == [targets[1].key]


def test_new_daily_capture_is_newer_than_old_hourly_record(tmp_path):
    path = tmp_path / "capture.png"
    path.write_bytes(b"test-png")
    modified = datetime(2026, 10, 6, 8, 0).timestamp()
    os.utime(path, (modified, modified))
    shared = {"capture_date": "2026-10-06", "site_building": "A楼", "target_key": "power_distribution", "file_path": str(path), "status": "captured"}
    module._upsert_record(tmp_path / "capture.json", {**shared, "capture_hour": "", "captured_at": "2026-10-06 10:00:00", "capture_batch_id": "new-batch"})
    module._upsert_record(tmp_path / "capture.json", {**shared, "capture_hour": "09", "captured_at": "2026-10-06 09:00:00", "capture_batch_id": "old-batch"})
    files = module.list_system_screenshot_files(config=config_for(tmp_path), capture_date="2026-10-06")["files"]
    assert len(files) == 1
    assert files[0]["capture_batch_id"] == "new-batch"


def test_state_write_failure_releases_capture_lock(tmp_path, monkeypatch):
    monkeypatch.setattr(module, "_save_capture_batch", Mock(side_effect=OSError("disk full")))
    container = SimpleNamespace(_system_screenshot_capture_run_lock=threading.Lock())
    with pytest.raises(OSError, match="disk full"):
        module.submit_system_screenshot_capture(container=container, config=config_for(tmp_path))
    assert not container._system_screenshot_capture_run_lock.locked()


def screenshot_png(*, blank=False):
    image = Image.new("RGB", (64, 64), "black")
    if not blank:
        image.putpixel((32, 32), (255, 255, 255))
    output = io.BytesIO()
    image.save(output, format="PNG")
    return output.getvalue()


class ScreenshotLocator:
    def __init__(self, page, frame):
        self.page = page
        self.frame = frame

    @property
    def first(self):
        return self

    async def is_visible(self, **kwargs):
        if self.page.mode == "missing_entry":
            return False
        if self.page.mode == "frame_replaced":
            if not self.page.frame_ready:
                self.page.frame_ready = True
                self.page.frames.append(ScreenshotFrame(self.page))
                raise RuntimeError("Frame was detached")
            return self.frame is not self.page.main_frame
        return True

    async def evaluate(self, *args, **kwargs):
        self.page.target_opened = True
        self.page.click_count += 1
        if self.page.mode == "error_page":
            self.page.url = "chrome-error://chromewebdata/"


class ScreenshotFrame:
    name = "main"
    url = "about:blank"

    def __init__(self, page):
        self.page = page

    def locator(self, selector):
        return ScreenshotLocator(self.page, self)


class ScreenshotPage:
    def __init__(self, mode="success", *, timeout_phase=""):
        self.mode = mode
        self.timeout_phase = timeout_phase
        self.main_frame = ScreenshotFrame(self)
        self.frames = [self.main_frame]
        self.url = "http://example.invalid/"
        self.target_opened = False
        self.click_count = 0
        self.frame_ready = False
        self.fonts_ready = False
        self.screenshot_options = []

    async def wait_for_load_state(self, *args, **kwargs):
        pass

    async def screenshot(self, *, path, **kwargs):
        self.screenshot_options.append(kwargs)
        if self.mode == "timeout" or (self.mode == "timeout_once" and len(self.screenshot_options) == 1):
            Path(path).write_bytes(b"partial screenshot")
            raise TimeoutError(
                "Page.screenshot: Timeout 30000ms exceeded.\n"
                f"Call log:\n  - taking page screenshot\n  - {self.timeout_phase}"
            )
        if self.mode == "delayed_ready":
            await asyncio.sleep(0)
            self.frame_ready = True
            await asyncio.sleep(0)
            self.fonts_ready = True
        data = screenshot_png(blank=self.mode == "blank")
        Path(path).write_bytes(data)
        return data


def run_mock_weak_current_capture(tmp_path, monkeypatch, building, page, target_key="weak_current"):
    async def no_navigation(*args, **kwargs):
        pass

    monkeypatch.setattr(module, "_goto_with_retries", no_navigation)
    monkeypatch.setattr(module, "_ensure_main_page_loaded", no_navigation)
    args = SimpleNamespace(
        capture_date="2026-11-04", batch_id="mock-batch", cancel_event=None,
        menu_timeout_ms=1000, action_timeout_ms=1000, network_idle_timeout_ms=1000,
        settle_sec=0, full_page=True,
    )
    site = SimpleNamespace(building=building, target_url="http://example.invalid/")
    target = next(item for item in module._normalize_targets() if item.key == target_key)
    return asyncio.run(module._capture_building_targets(
        page=page, site=site, building_targets=[target], args=args,
        state_path=tmp_path / "state.json", download_root_path=tmp_path,
        output_dir=tmp_path / "output", compact_date="20261104", hour_text="",
        emit_log=lambda _: None,
    ))


@pytest.mark.parametrize("building", ["C楼", "D楼", "E楼"])
def test_weak_current_waits_for_screenshot_with_delayed_frame_and_fonts(tmp_path, monkeypatch, building):
    page = ScreenshotPage("delayed_ready")
    records = run_mock_weak_current_capture(tmp_path, monkeypatch, building, page)
    assert page.target_opened and page.frame_ready and page.fonts_ready
    assert page.screenshot_options == [{"full_page": True}]
    assert len(records) == 1 and records[0]["status"] == "captured"
    assert Path(records[0]["file_path"]).read_bytes() == screenshot_png()


@pytest.mark.parametrize("building", ["C楼", "D楼", "E楼"])
@pytest.mark.parametrize("phase", ["waiting for fonts to load...", "frame preparation pending"])
def test_weak_current_screenshot_timeout_preserves_existing_source(tmp_path, monkeypatch, building, phase):
    output_dir = tmp_path / "output" / building
    output_dir.mkdir(parents=True)
    existing = output_dir / f"20261104--系统截图--{building}--弱电系统图.png"
    original = screenshot_png()
    existing.write_bytes(original)
    state_path = tmp_path / "state.json"
    module._upsert_record(state_path, {
        "capture_date": "2026-11-04", "site_building": building,
        "target_key": "weak_current", "file_path": str(existing), "status": "captured",
    })
    original_state = state_path.read_bytes()
    page = ScreenshotPage("timeout", timeout_phase=phase)

    with pytest.raises(TimeoutError, match="Page.screenshot: Timeout 30000ms exceeded"):
        run_mock_weak_current_capture(tmp_path, monkeypatch, building, page)

    assert page.target_opened
    assert page.screenshot_options == [
        {"full_page": True}, {"full_page": True, "animations": "disabled", "timeout": 90000}
    ]
    assert existing.read_bytes() == original
    assert state_path.read_bytes() == original_state
    assert not list(output_dir.glob("*.tmp.png"))


def test_missing_target_entry_does_not_capture_an_error_page(tmp_path, monkeypatch):
    page = ScreenshotPage("missing_entry")
    with pytest.raises(RuntimeError, match="未找到系统入口"):
        run_mock_weak_current_capture(tmp_path, monkeypatch, "C楼", page)
    assert not page.screenshot_options
    assert not (tmp_path / "state.json").exists()


def test_target_search_rechecks_frames_replaced_after_login(tmp_path, monkeypatch):
    page = ScreenshotPage("frame_replaced")
    records = run_mock_weak_current_capture(tmp_path, monkeypatch, "E楼", page)
    assert page.frame_ready and page.target_opened
    assert len(records) == 1 and records[0]["status"] == "captured"


@pytest.mark.parametrize(("target_key", "clicks"), [
    ("power_distribution", 1), ("hvac_a", 2), ("hvac_b", 2),
    ("fuel", 1), ("generator", 1),
])
def test_other_system_targets_keep_their_entry_flow(tmp_path, monkeypatch, target_key, clicks):
    page = ScreenshotPage()
    records = run_mock_weak_current_capture(tmp_path, monkeypatch, "C楼", page, target_key=target_key)
    assert page.click_count == clicks
    assert records[0]["target_key"] == target_key


def test_error_page_after_target_click_must_not_be_recorded_as_success(tmp_path, monkeypatch):
    page = ScreenshotPage("error_page")
    with pytest.raises(RuntimeError):
        run_mock_weak_current_capture(tmp_path, monkeypatch, "C楼", page)


def test_blank_png_must_not_be_recorded_as_success(tmp_path, monkeypatch):
    page = ScreenshotPage("blank")
    with pytest.raises(RuntimeError):
        run_mock_weak_current_capture(tmp_path, monkeypatch, "C楼", page)


@pytest.mark.parametrize("building", ["C楼", "D楼", "E楼"])
def test_weak_current_timeout_retries_same_page_without_changing_capture_range(tmp_path, monkeypatch, building):
    page = ScreenshotPage("timeout_once")
    records = run_mock_weak_current_capture(tmp_path, monkeypatch, building, page)
    assert page.target_opened
    assert page.screenshot_options == [
        {"full_page": True}, {"full_page": True, "animations": "disabled", "timeout": 90000}
    ]
    assert Path(records[0]["file_path"]).read_bytes() == screenshot_png()


def test_capture_completion_requires_existing_source_file(tmp_path, monkeypatch):
    target = next(item for item in module._normalize_targets() if item.key == "weak_current")
    monkeypatch.setattr(module, "_select_sites", lambda *_: [SimpleNamespace(building="C楼")])

    async def pretend_capture(**kwargs):
        return [{"target_key": target.key}]

    monkeypatch.setattr(module, "_capture_with_browser_pool", pretend_capture)
    config = config_for(tmp_path)
    config["system_screenshot_capture"]["targets"] = [
        {"key": target.key, "label": target.label, "span_id": target.span_id, "text": target.text}
    ]
    with pytest.raises(RuntimeError, match="系统截图采集后仍不完整"):
        asyncio.run(module.run_system_screenshot_capture_async(config=config, capture_date="2026-11-04"))
