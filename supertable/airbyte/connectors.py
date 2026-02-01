import airbyte as ab
import json

# Initialize the source (this will install the connector if missing)
source = ab.get_source("source-mssql")

# The 'spec' contains the full JSON Schema for the configuration
config_spec = source.config_spec
# OR for the full technical schema:
#source.print_config_spec()
print(source.docs_url)