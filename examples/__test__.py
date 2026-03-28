from supertable.redis_connector import create_redis_client

r = create_redis_client()

# Wipe all RBAC keys
keys = list(r.scan_iter("supertable:kladna-soft:example:rbac:*"))
if keys:
    r.delete(*keys)
    print(f"Deleted {len(keys)} RBAC keys")
else:
    print("No RBAC keys found")

print(f"DB size after wipe: {r.dbsize()}")