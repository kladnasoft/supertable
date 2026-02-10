from examples.defaults import organization, super_name, staging_name
from supertable.super_pipe import SuperPipe

pipe_name = "pipe_01"

pipe = SuperPipe(
    organization=organization,
    super_name=super_name,
    staging_name=staging_name,
)

deleted = pipe.delete(pipe_name)

if deleted:
    print(f"Deleted pipe '{pipe_name}' from Redis.")
else:
    print(f"Pipe '{pipe_name}' was not found (nothing deleted).")
