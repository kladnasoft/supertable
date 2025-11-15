from sql_parser import SQLParser

def example_usage() -> None:
    query = """
        SELECT
            u.id,
            u.name,
            o.id AS order_id,
            o.amount,
            o.created_at
        FROM users u
        JOIN stock.orders o ON u.id = o.user_id
        LEFT JOIN accounting.payments p ON p.order_id = o.id
        WHERE u.country = 'US'
          AND o.status = 'completed'
        ORDER BY o.created_at DESC
    """

    try:
        parser = SQLParser(super_name="profile", query=query)
    except ValueError as exc:
        print(exc)
        return

    tuples = parser.get_table_tuples()
    for supertable, table, alias, columns in tuples:
        print(f"{supertable}.{table} [{alias}] -> {columns}")


if __name__ == "__main__":
    example_usage()