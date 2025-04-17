import os
import random
from datetime import datetime, timedelta
from faker import Faker
from sample_generator import generate_dummy_data
from sample_schemas import schema, get_random_schema
from multiprocessing import Pool, cpu_count

from supertable.data_writer import DataWriter
print(os.getcwd())

FILE_FORMAT = "csv"  # Can be 'csv', 'parquet', or 'json'
START_DATE = "2024-01-01"
ITERATION = 1
MIN_FILES = 5
MAX_FILES = 500
MIN_ROWS = 100
MAX_ROWS = 10000
MIN_COLUMNS = 10
MAX_COLUMNS = 20


# Initialize Faker
faker = Faker()


# Function to generate files
def generate_files(partition_info):
    part, files_to_generate, partition_value, start_date, random_schema = partition_info

    # Create the base sample directory
    base_dir = "generated_data"
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    # Create directory for the partition value under the sample folder
    partition_dir = os.path.join(base_dir, partition_value)
    if not os.path.exists(partition_dir):
        os.makedirs(partition_dir)

    # Generate files
    for i in range(files_to_generate):
        num_rows = random.randint(MIN_ROWS, MAX_ROWS)
        # Calculate the date for the current iteration
        current_date = start_date + timedelta(days=i)
        day_str = current_date.strftime("%Y-%m-%d")
        print(
            f"partition: {part}, iteration: {i} out of {files_to_generate}, day_str: {day_str}, num_rows: {num_rows}, columns: {len(random_schema)}"
        )
        # Generate dummy data
        df = generate_dummy_data(random_schema, num_rows, partition_value, day_str)

        # Construct file name
        file_name = f"{day_str}_dummy_data.{FILE_FORMAT}"
        file_path = os.path.join(partition_dir, file_name)

        # Save to the specified format
        if FILE_FORMAT == "csv":
            df.to_csv(file_path, index=False)
        elif FILE_FORMAT == "parquet":
            df.to_parquet(file_path, index=False)
        elif FILE_FORMAT == "json":
            df.to_json(file_path, orient="records", lines=True)
        else:
            raise ValueError(f"Unsupported file format: {FILE_FORMAT}")

        print(f"Dummy {FILE_FORMAT.upper()} file generated successfully: {file_path}")

    print("All files processed.")


def main():
    tasks = []
    for part in range(ITERATION):
        # Parameters
        files_to_generate = random.randint(
            MIN_FILES, MAX_FILES
        )  # Number of files to generate
        partition_value = faker.sentence().replace(" ", "_").lower().replace(".", "")
        start_date = datetime.strptime(START_DATE, "%Y-%m-%d")
        random_schema = get_random_schema(schema, MIN_COLUMNS, MAX_COLUMNS)

        print(f"files_to_generate: {files_to_generate}")
        print(f"columns: {len(random_schema)}")

        # Prepare task information
        task_info = (
            part,
            files_to_generate,
            partition_value,
            start_date,
            random_schema,
        )
        tasks.append(task_info)

    # Use multiprocessing to generate files in parallel
    with Pool(cpu_count()) as pool:
        pool.map(generate_files, tasks)


if __name__ == "__main__":
    main()
