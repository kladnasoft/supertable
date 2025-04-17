# Define the schema
schema = {
    "Name": "name",
    "Address": "address",
    "Email": "email",
    "Phone": "phone_number",
    "Birthdate": "date",
    "Notes": "text",
    "Score": "integer",
    "Rating": "float",
    "Company": "company",
    "Job": "job",
    "City": "city",
    "State": "state",
    "Country": "country",
    "ZipCode": "zipcode",
    "StreetName": "street_name",
    "CreditCard": "credit_card_number",
    "SSN": "ssn",
    "Website": "url",
    "IPv4": "ipv4",
    "UUID": "uuid",
    "BinaryData": "binary",
    "Timezone": "timezone",
    "Sentence": "sentence",
    "Paragraph": "paragraph",
    "Boolean": "boolean",
    "FileName": "file_name",
    "MimeType": "mime_type",
    "Currency": "currency",
    "CurrencyCode": "currency_code",
    "DateTime": "date_time",
    "ISBN13": "isbn13",
    "LicensePlate": "license_plate",
    "MacAddress": "mac_address",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "Language": "language_name",
    "Locale": "locale",
    "ImageUrl": "image_url",
    "UserAgent": "user_agent",
    "Chrome": "chrome",
    "Firefox": "firefox",
    "Safari": "safari",
    "Opera": "opera",
    "LinuxPlatform": "linux_platform_token",
    "WindowsPlatform": "windows_platform_token",
    "Profile": "profile",
    "UnixTime": "unix_time",
}

# Add 50 random 'text' columns
for i in range(1, 10):
    schema[f"Text_{i}"] = "text"

# Add 50 random 'integer' columns
for i in range(1, 51):
    schema[f"Integer_{i}"] = "integer"

# Add 50 random 'float' columns
for i in range(1, 51):
    schema[f"Float_{i}"] = "float"

# Add 10 random 'date' columns
for i in range(1, 50):
    schema[f"Date_{i}"] = "date"

import random


def get_random_schema(schema, min_cols=15, max_cols=200):
    total_columns = random.randint(min_cols, max_cols)
    selected_columns = random.sample(list(schema.keys()), total_columns)
    return {col: schema[col] for col in selected_columns}
