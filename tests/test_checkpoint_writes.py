from pathlib import Path

import pytest

from dividend_events.storage import read_json, write_json


@pytest.mark.parametrize("persistent", [False, True])
def test_windows_lock_preserves_checkpoint_until_atomic_replacement(tmp_path, monkeypatch, persistent):
    path = tmp_path / "checkpoint.json"
    write_json(path, {"version": 1})
    original = Path.replace
    calls = []

    def replace(source, target):
        calls.append(target)
        assert read_json(target) == {"version": 1}
        if persistent or len(calls) < 3:
            error = PermissionError("file locked")
            error.winerror = 5
            raise error
        return original(source, target)

    monkeypatch.setattr(Path, "replace", replace)
    monkeypatch.setattr("dividend_events.storage.time.sleep", lambda _: None)
    if persistent:
        with pytest.raises(PermissionError):
            write_json(path, {"version": 2})
        assert read_json(path) == {"version": 1} and len(calls) == 6
    else:
        write_json(path, {"version": 2})
        assert read_json(path) == {"version": 2} and len(calls) == 3
