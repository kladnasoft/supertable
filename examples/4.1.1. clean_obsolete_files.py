from supertable.history_cleaner import HistoryCleaner
from examples.defaults import super_name, role_name, organization

history_cleaner = HistoryCleaner(super_name=super_name, organization=organization)
history_cleaner.clean(role_name=role_name)
