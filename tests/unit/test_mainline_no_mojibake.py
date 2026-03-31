from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TARGETS = [
    PROJECT_ROOT / 'app',
    PROJECT_ROOT / 'web' / 'frontend' / 'src',
    PROJECT_ROOT / 'handover_log_module',
    PROJECT_ROOT / 'tests',
    PROJECT_ROOT / 'main.py',
]
EXCLUDED_PARTS = {
    'build_output',
    'runtime_state',
    '.runtime',
    'web_frontend',
    'dist',
}
EXCLUDED_SUFFIXES = {'.bak'}
BAD_SNIPPETS = [
    '鍚姩',
    '璋冨害',
    '鑷姩',
    '杩愯',
    '浠诲姟',
    '澶辫触',
    '婵犵數濮烽弫鍛婃叏',
    '椤圭嫭绔嬩笂浼',
    '婧愭暟鎹枃浠朵笉瀛樺湪',
    '\ufffd',
]


def _iter_target_files():
    for target in TARGETS:
        if target.is_file():
            if target.name == "test_mainline_no_mojibake.py":
                continue
            yield target
            continue
        for path in target.rglob('*'):
            if not path.is_file():
                continue
            if any(part in EXCLUDED_PARTS for part in path.parts):
                continue
            if path.suffix.lower() in EXCLUDED_SUFFIXES:
                continue
            if path.name == "test_mainline_no_mojibake.py":
                continue
            yield path


def test_mainline_source_has_no_known_mojibake_fragments():
    hits = []
    for path in _iter_target_files():
        try:
            text = path.read_text(encoding='utf-8')
        except UnicodeDecodeError:
            continue
        for snippet in BAD_SNIPPETS:
            if snippet in text:
                hits.append(f'{path.relative_to(PROJECT_ROOT)}::{snippet}')
                break
    assert not hits, '发现主线源码乱码片段:\n' + '\n'.join(hits)
