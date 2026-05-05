"""Create a SuperPipe that ingests files dropped into the staging area.

The pipe maps staging files to ``simple_name`` and dedups on
``overwrite_columns``. ``user_hash`` is recorded on the pipe definition for
audit/lineage purposes; we resolve it at runtime.
"""
from supertable.demo.quickstart.defaults import (
    organization, super_name, staging_name, simple_name, role_name,
)
from supertable.super_pipe import SuperPipe
from supertable.rbac.user_manager import UserManager


# Resolve user_hash dynamically rather than hard-coding it.
um = UserManager(super_name=super_name, organization=organization)
user_hash = um.get_or_create_default_user()

pipe = SuperPipe(
    organization=organization,
    super_name=super_name,
    staging_name=staging_name,
)

pipe_path = pipe.create(
    role_name=role_name,
    pipe_name="pipe_01",
    simple_name=simple_name,
    user_hash=user_hash,
    overwrite_columns=["day"],
)

print(f"created pipe at: {pipe_path}")
