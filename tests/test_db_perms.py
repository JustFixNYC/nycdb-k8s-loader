import pytest

from .conftest import DB_NAME
from lib import db_perms
import load_dataset


def test_get_current_database_works(conn):
    assert db_perms.get_current_database(conn) == DB_NAME


def test_exec_grant_sql_does_nothing_if_sql_is_empty():
    db_perms.exec_grant_sql(None, '')


def test_get_grant_sql_raises_error_on_invalid_table(conn):
    with pytest.raises(ValueError, match="Table public.blarg does not exist"):
        db_perms.get_grant_sql(conn, 'blarg')


def test_get_grant_sql_works(conn):
    with conn.cursor() as cur:
        cur.execute('DROP USER IF EXISTS boop')
        cur.execute("CREATE TABLE mytable (code char(5) PRIMARY KEY)")

    assert db_perms.get_grant_sql(conn, 'mytable') == ''

    db_perms.create_readonly_user(conn, 'boop', 'pass')

    grant_sql = 'GRANT SELECT ON TABLE public.mytable TO boop;'
    assert db_perms.get_grant_sql(conn, 'mytable') == grant_sql

    with load_dataset.create_and_enter_temporary_schema(conn, 'blarf'):
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE mytable (code char(5) PRIMARY KEY)")
        assert db_perms.get_grant_sql(conn, 'mytable', 'blarf') == ''
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE public.mytable")
            cur.execute(f"ALTER TABLE mytable SET SCHEMA public")

    assert db_perms.get_grant_sql(conn, 'mytable') == ''
    db_perms.exec_grant_sql(conn, grant_sql)
    assert db_perms.get_grant_sql(conn, 'mytable') == grant_sql
