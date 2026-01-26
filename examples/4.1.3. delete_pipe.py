from examples.defaults import organization, super_name, staging_name
from supertable.super_pipe import SuperPipe

pipe_name = "pipe_01"

pipe = SuperPipe(
    organization=organization,
    super_name=super_name,
    staging_name=staging_name,
)

# Minimal delete: removes the JSON definition from storage
pipe_path = pipe._pipe_path(pipe_name)  # noqa: SLF001
pipe.storage.delete(pipe_path)

print(pipe_path)
