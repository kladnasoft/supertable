"""Delete the pipe created by 2.3.2."""
from supertable.demo.quickstart.defaults import organization, super_name, staging_name, role_name
from supertable.super_pipe import SuperPipe


pipe = SuperPipe(
    organization=organization,
    super_name=super_name,
    staging_name=staging_name,
)

deleted = pipe.delete("pipe_01", role_name=role_name)
if deleted:
    print("deleted pipe 'pipe_01'")
else:
    print("pipe 'pipe_01' not found")
