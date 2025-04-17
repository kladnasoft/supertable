from examples.defaults import super_name, user_hash, simple_name, organization
from supertable.simple_table import SimpleTable
from supertable.super_table import SuperTable

super_table = SuperTable(super_name=super_name, organization=organization)
simple_table = SimpleTable(super_table=super_table, simple_name=simple_name)
simple_table.delete(user_hash)

