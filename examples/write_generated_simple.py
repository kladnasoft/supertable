import json
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from supertable.data_writer import DataWriter
from supertable.utils.helper import format_size
from examples.defaults import organization, super_name, user_hash, overwrite_columns, generated_data_dir

# Initialize DataWriter (placeholder, this will be handled in the main loop)
data_writer = None


def read_file_as_table(file_path):
    if file_path.endswith(".csv"):
        df = pd.read_csv(file_path, low_memory=False)
        df = df.apply(lambda x: x.astype(str) if x.dtype == "object" else x)
        return pa.Table.from_pandas(df)
    elif file_path.endswith(".json"):
        try:
            df = pd.read_json(file_path, lines=True)
        except ValueError:
            with open(file_path, "r") as f:
                try:
                    data = json.load(f)
                    df = pd.DataFrame(data)
                except json.JSONDecodeError:
                    f.seek(0)
                    data = [json.loads(line) for line in f]
                    df = pd.DataFrame(data)
        df = df.apply(lambda x: x.astype(str) if x.dtype == "object" else x)
        return pa.Table.from_pandas(df)
    elif file_path.endswith(".parquet"):
        return pq.read_table(file_path)
    else:
        raise ValueError(f"Unsupported file type: {file_path}")


def get_data(generated_data_dir=generated_data_dir):
    directories = os.listdir(generated_data_dir)

    for directory in sorted(directories):
        dir_path = os.path.join(generated_data_dir, directory)
        if os.path.isdir(dir_path):
            print(f"Directory: {directory}")
            print("-" * 100)
            files = os.listdir(dir_path)
            for file in sorted(files):
                relative_path = os.path.join(dir_path, file)
                hyper_name = relative_path.split("/")[1]
                print(f"File: {relative_path}")
                table = read_file_as_table(relative_path)
                file_size = os.path.getsize(relative_path)

                print(
                    f"Rows: {table.num_rows}, Columns: {table.num_columns}, Size: {format_size(file_size)}"
                )

                yield hyper_name, table


# Main Execution
if __name__ == "__main__":

    # Initialize DataWriter with the super name
    data_writer = DataWriter(super_name, organization)

    for hyper_name, data in get_data(generated_data_dir):
        columns, rows, inserted, deleted = data_writer.write(
            user_hash=user_hash,
            simple_name=hyper_name,
            data=data,
            overwrite_columns=overwrite_columns,
        )
        print(
            f"Response: [columns: {columns}, rows: {rows}, inserted: {inserted}, deleted: {deleted}]"
        )
        print("-" * 100)
