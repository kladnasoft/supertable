from examples.defaults import super_name, organization
from supertable.super_table import SuperTable

super_table = SuperTable(super_name=super_name, organization=organization)
super_table.delete()