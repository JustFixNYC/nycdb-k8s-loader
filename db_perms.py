from typing import List


def exec_grant_sql(conn, sql: str):
    if not sql:
        return
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def ensure_table_exists(conn, table_name: str, schema: str):
    with conn.cursor() as cursor:
        cursor.execute(f"""
        SELECT EXISTS (
            SELECT 1
            FROM   information_schema.tables 
            WHERE  table_schema = '{schema}'
            AND    table_name = '{table_name}'
        );""")
        if cursor.fetchone()[0] is not True:
            raise ValueError(f'Table {schema}.{table_name} does not exist!')


def get_grant_sql(conn, table_name: str, schema: str='public') -> str:
    ensure_table_exists(conn, table_name, schema)
    # https://stackoverflow.com/a/46235386
    query = f"""
    SELECT 
        format (
        'GRANT %s ON TABLE %I.%I TO %I%s;',
        string_agg(tg.privilege_type, ', '),
        tg.table_schema,
        tg.table_name,
        tg.grantee,
        CASE
            WHEN tg.is_grantable = 'YES' 
            THEN ' WITH GRANT OPTION' 
            ELSE '' 
        END
        )
    FROM information_schema.role_table_grants tg
    JOIN pg_tables t ON t.schemaname = tg.table_schema AND t.tablename = tg.table_name
    WHERE
        tg.table_schema = '{schema}' AND
        tg.table_name = '{table_name}' AND
        t.tableowner <> tg.grantee
    GROUP BY tg.table_schema, tg.table_name, tg.grantee, tg.is_grantable;
    """

    with conn.cursor() as cur:
        cur.execute(query)
        return ''.join([r[0] for r in cur.fetchall()])


def get_current_database(conn) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT current_database()")
        return cur.fetchone()[0]


def create_readonly_user(conn, user: str, password: str):
    dbname = get_current_database(conn)
    query = f"""
    CREATE USER {user} WITH ENCRYPTED PASSWORD '{password}';
    GRANT CONNECT ON DATABASE {dbname} TO {user};
    GRANT USAGE ON SCHEMA public TO {user};
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO {user};
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
      GRANT SELECT ON TABLES TO {user};
    """

    with conn.cursor() as cur:
        cur.execute(query)
