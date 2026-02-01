import airbyte as ab

for name in ab.get_available_connectors():
    print(name)