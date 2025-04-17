import os
from examples.defaults import super_name, simple_name, user_hash
from supertable.locking import Locking

working_dir = os.getcwd()
resources_1 = ["table_1"]
resources_2 = ["table_1", "tables_2"]

lock1 = Locking(identity=simple_name, working_dir=working_dir)
print("lock1:", lock1.identity)
lock1.self_lock()
lock1.lock_resources(resources_1, 10, 3)
lock1.lock_resources(resources_2, 10, 3)

lock2 = Locking(identity=simple_name, working_dir=working_dir)
print("lock2:", lock2.identity)
lock2.lock_resources(resources_1)
