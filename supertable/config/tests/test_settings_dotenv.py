import os
import subprocess
import sys


def test_settings_does_not_search_working_directory_for_dotenv(tmp_path):
    (tmp_path / ".env").write_text("SUPERTABLE_ORGANIZATION=poisoned\n")
    env = os.environ.copy()
    env.pop("SUPERTABLE_DOTENV_PATH", None)
    env.pop("SUPERTABLE_ORGANIZATION", None)
    result = subprocess.run(
        [sys.executable, "-c", "from supertable.config.settings import settings; print(settings.SUPERTABLE_ORGANIZATION)"],
        cwd=tmp_path, env=env, capture_output=True, text=True, check=True,
    )
    assert result.stdout.strip() == ""
