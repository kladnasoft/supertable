"""Destructive: drop the entire SuperTable. Disabled by default in the controller."""
from supertable.demo.quickstart.defaults import super_name, organization, role_name
from supertable.super_table import SuperTable


super_table = SuperTable(super_name=super_name, organization=organization)
super_table.delete(role_name=role_name)
