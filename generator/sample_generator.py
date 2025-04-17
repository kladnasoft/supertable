import pandas as pd
import numpy as np
from faker import Faker

faker = Faker()


def generate_dummy_data(schema, num_rows, partition_value, day):
    data = {}
    for column, col_type in schema.items():
        data["PartitionKey"] = [partition_value for _ in range(num_rows)]
        data["Day"] = [day for _ in range(num_rows)]
        if col_type == "name":
            data[column] = [faker.name() for _ in range(num_rows)]
        elif col_type == "address":
            data[column] = [faker.address() for _ in range(num_rows)]
        elif col_type == "email":
            data[column] = [faker.email() for _ in range(num_rows)]
        elif col_type == "phone_number":
            data[column] = [faker.phone_number() for _ in range(num_rows)]
        elif col_type == "date":
            data[column] = [faker.date() for _ in range(num_rows)]
        elif col_type.startswith("text"):
            data[column] = [faker.text(max_nb_chars=200) for _ in range(num_rows)]
        elif col_type.startswith("integer"):
            data[column] = np.random.randint(0, 100, size=num_rows).tolist()
        elif col_type.startswith("float"):
            data[column] = np.random.uniform(0, 100, size=num_rows).tolist()
        elif col_type == "word":
            data[column] = [faker.word() for _ in range(num_rows)]
        elif col_type == "sentence":
            data[column] = [faker.sentence() for _ in range(num_rows)]
        elif col_type == "paragraph":
            data[column] = [faker.paragraph() for _ in range(num_rows)]
        elif col_type == "boolean":
            data[column] = np.random.choice([True, False], size=num_rows).tolist()
        elif col_type == "country":
            data[column] = [faker.country() for _ in range(num_rows)]
        elif col_type == "city":
            data[column] = [faker.city() for _ in range(num_rows)]
        elif col_type == "state":
            data[column] = [faker.state() for _ in range(num_rows)]
        elif col_type == "zipcode":
            data[column] = [faker.zipcode() for _ in range(num_rows)]
        elif col_type == "street_name":
            data[column] = [faker.street_name() for _ in range(num_rows)]
        elif col_type == "company":
            data[column] = [faker.company() for _ in range(num_rows)]
        elif col_type == "job":
            data[column] = [faker.job() for _ in range(num_rows)]
        elif col_type == "credit_card_number":
            data[column] = [faker.credit_card_number() for _ in range(num_rows)]
        elif col_type == "ssn":
            data[column] = [faker.ssn() for _ in range(num_rows)]
        elif col_type == "url":
            data[column] = [faker.url() for _ in range(num_rows)]
        elif col_type == "ipv4":
            data[column] = [faker.ipv4() for _ in range(num_rows)]
        elif col_type == "uuid":
            data[column] = [faker.uuid4() for _ in range(num_rows)]
        elif col_type == "binary":
            data[column] = [faker.binary(length=10) for _ in range(num_rows)]
        elif col_type == "timezone":
            data[column] = [faker.timezone() for _ in range(num_rows)]
        elif col_type == "file_name":
            data[column] = [faker.file_name() for _ in range(num_rows)]
        elif col_type == "mime_type":
            data[column] = [faker.mime_type() for _ in range(num_rows)]
        elif col_type == "currency":
            data[column] = [faker.currency_name() for _ in range(num_rows)]
        elif col_type == "currency_code":
            data[column] = [faker.currency_code() for _ in range(num_rows)]
        elif col_type == "date_time":
            data[column] = [faker.date_time() for _ in range(num_rows)]
        elif col_type == "isbn13":
            data[column] = [faker.isbn13() for _ in range(num_rows)]
        elif col_type == "license_plate":
            data[column] = [faker.license_plate() for _ in range(num_rows)]
        elif col_type == "mac_address":
            data[column] = [faker.mac_address() for _ in range(num_rows)]
        elif col_type == "latitude":
            data[column] = np.random.uniform(-90, 90, size=num_rows).tolist()
        elif col_type == "longitude":
            data[column] = np.random.uniform(-180, 180, size=num_rows).tolist()
        elif col_type == "language_name":
            data[column] = [faker.language_name() for _ in range(num_rows)]
        elif col_type == "locale":
            data[column] = [faker.locale() for _ in range(num_rows)]
        elif col_type == "image_url":
            data[column] = [faker.image_url() for _ in range(num_rows)]
        elif col_type == "user_agent":
            data[column] = [faker.user_agent() for _ in range(num_rows)]
        elif col_type == "chrome":
            data[column] = [faker.chrome() for _ in range(num_rows)]
        elif col_type == "firefox":
            data[column] = [faker.firefox() for _ in range(num_rows)]
        elif col_type == "safari":
            data[column] = [faker.safari() for _ in range(num_rows)]
        elif col_type == "opera":
            data[column] = [faker.opera() for _ in range(num_rows)]
        elif col_type == "linux_platform_token":
            data[column] = [faker.linux_platform_token() for _ in range(num_rows)]
        elif col_type == "windows_platform_token":
            data[column] = [faker.windows_platform_token() for _ in range(num_rows)]
        elif col_type == "profile":
            data[column] = [faker.simple_profile() for _ in range(num_rows)]
        elif col_type == "unix_time":
            data[column] = [faker.unix_time() for _ in range(num_rows)]
        else:
            data[column] = [
                faker.word() for _ in range(num_rows)
            ]  # default to word if type is unknown

    return pd.DataFrame(data)
