import os
import sys

from supertable.config.defaults import default, logger

# If this file is located in a subdirectory, adjust the path logic as needed.
# Currently appending ".." from __file__ to add the project root directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

app_home = os.getenv("SUPERTABLE_HOME", "~/supertable")

def change_to_app_home(home_dir: str) -> None:
    """
    Attempts to change the current working directory to `home_dir`.
    Prints the outcome. Logs (or prints) any error encountered.
    """
    expanded_dir = os.path.expanduser(home_dir)
    try:
        os.chdir(expanded_dir)
        logger.debug(f"Changed working directory to {expanded_dir}")
    except Exception as e:
        logger.error(f"Failed to change working directory to {expanded_dir}: {e}")

if app_home:
    change_to_app_home(app_home)
else:
    logger.error("SUPERTABLE_HOME environment variable is not set")


logger.info(f"Current working directory: {os.getcwd()}")


def get_app_home():
    return app_home