from examples.defaults import super_name, user_hash, simple_name, organization
from supertable.super_table import SuperTable

super_table = SuperTable(super_name=super_name, organization=organization)
super_table.delete(user_hash)