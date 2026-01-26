from examples.defaults import organization, super_name, staging_name, user_hash, simple_name
from supertable.super_pipe import SuperPipe

# Same semantics as write_single_data.py:
# - empty list => append
# - otherwise overwrite on these columns
overwrite_columns = ["day"]

pipe_name = "pipe_01"

pipe = SuperPipe(
    organization=organization,
    super_name=super_name,
    staging_name=staging_name,
)

pipe_path = pipe.create(
    pipe_name=pipe_name,
    user_hash=user_hash,
    simple_name=simple_name,
    overwrite_columns=overwrite_columns,
)

print(pipe_path)
