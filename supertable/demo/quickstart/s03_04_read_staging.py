"""List the files currently sitting in the staging area."""
from supertable.config.defaults import logger
from supertable.staging_area import Staging

from supertable.demo.quickstart.defaults import organization, super_name, staging_name, role_name


staging = Staging(
    organization=organization,
    super_name=super_name,
    staging_name=staging_name,
)

files = staging.list_files(role_name=role_name)
logger.info(f"staging files in {staging_name}: {files}")
