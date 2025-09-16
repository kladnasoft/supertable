from supertable.history_cleaner import HistoryCleaner
from examples.defaults import super_name, user_hash, organization

history_cleaner = HistoryCleaner(super_name=super_name, organization=organization)
history_cleaner.clean(user_hash=user_hash)
