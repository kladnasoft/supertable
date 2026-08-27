import shutil
from types import SimpleNamespace

import pytest

from supertable.engine.disk_admission import (
    DiskAdmissionUnavailable,
    reserve_disk,
)


def test_shared_disk_reservation_accounts_other_processes(tmp_path, monkeypatch):
    app_root = tmp_path / "app"
    monkeypatch.setattr(
        "supertable.engine.disk_admission.get_app_home",
        lambda: str(app_root),
    )
    monkeypatch.setattr(
        shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=150),
    )

    first = reserve_disk(tmp_path, 100)
    assert first is not None
    try:
        with pytest.raises(DiskAdmissionUnavailable):
            reserve_disk(tmp_path, 100)
    finally:
        first.release()

    second = reserve_disk(tmp_path, 100)
    assert second is not None
    second.release()
