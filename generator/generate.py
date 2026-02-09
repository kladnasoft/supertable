import os
import random
from datetime import datetime, timedelta
from faker import Faker
from sample_generator import generate_dummy_data
from sample_schemas import schema, get_random_schema
from multiprocessing import Pool, cpu_count
from enum import Enum, auto
from supertable.data_writer import DataWriter

class TimePrecision(Enum):
    DAY = auto()
    HOUR = auto()
    MINUTE = auto()

# Configuration
FILE_FORMAT = "parquet"  # Can be 'csv', 'parquet', or 'json'
START_DATE = "2025-01-01"
ITERATION = 1
MIN_FILES = 100
MAX_FILES = 200
MIN_ROWS = 100000
MAX_ROWS = 1000000
MIN_COLUMNS = 6
MAX_COLUMNS = 15
TIME_PRECISION = TimePrecision.MINUTE  # Change to DAY, HOUR, or MINUTE

# Initialize Faker
faker = Faker()

def get_timestamp_format(current_date, precision):
    if precision == TimePrecision.DAY:
        return current_date.strftime("%Y-%m-%d"), timedelta(days=1)
    elif precision == TimePrecision.HOUR:
        return current_date.strftime("%Y-%m-%d_%H"), timedelta(hours=1)
    elif precision == TimePrecision.MINUTE:
        return current_date.strftime("%Y-%m-%d_%H-%M"), timedelta(minutes=1)
    else:
        raise ValueError(f"Unknown time precision: {precision}")

def generate_files(partition_info):
    part, files_to_generate, partition_value, start_date, random_schema = partition_info

    base_dir = "generated_data"
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    partition_dir = os.path.join(base_dir, partition_value)
    if not os.path.exists(partition_dir):
        os.makedirs(partition_dir)

    current_date = start_date
    for i in range(files_to_generate):
        num_rows = random.randint(MIN_ROWS, MAX_ROWS)
        timestamp_str, time_increment = get_timestamp_format(current_date, TIME_PRECISION)
        current_date += time_increment

        print(
            f"partition: {part}, iteration: {i}/{files_to_generate}, "
            f"timestamp: {timestamp_str}, rows: {num_rows}, columns: {len(random_schema)}"
        )

        # Generate dummy data with updated function signature
        df = generate_dummy_data(
            schema=random_schema,
            num_rows=num_rows,
            _sys_table_name=partition_value,
            partition=timestamp_str
        )

        file_name = f"{timestamp_str}_dummy_data.{FILE_FORMAT}"
        file_path = os.path.join(partition_dir, file_name)

        if FILE_FORMAT == "csv":
            df.to_csv(file_path, index=False)
        elif FILE_FORMAT == "parquet":
            df.to_parquet(file_path, index=False)
        elif FILE_FORMAT == "json":
            df.to_json(file_path, orient="records", lines=True)
        else:
            raise ValueError(f"Unsupported file format: {FILE_FORMAT}")

        print(f"Generated: {file_path}")

def main():
    tasks = []
    for part in range(ITERATION):
        files_to_generate = random.randint(MIN_FILES, MAX_FILES)
        partition_value = faker.sentence().replace(" ", "_").lower().replace(".", "")
        start_date = datetime.strptime(START_DATE, "%Y-%m-%d")
        random_schema = get_random_schema(schema, MIN_COLUMNS, MAX_COLUMNS)

        print(f"Files to generate: {files_to_generate}")
        print(f"Columns: {len(random_schema)}")
        print(f"Time precision: {TIME_PRECISION.name}")

        tasks.append((
            part,
            files_to_generate,
            partition_value,
            start_date,
            random_schema,
        ))

    with Pool(cpu_count()) as pool:
        pool.map(generate_files, tasks)

if __name__ == "__main__":
    main()