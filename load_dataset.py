import os
import sys
import subprocess
import contextlib
import time
from pathlib import Path
from typing import NamedTuple, List
from types import SimpleNamespace
import urllib.parse
import psycopg2
import yaml
import nycdb.dataset
from nycdb.dataset import Dataset
from nycdb.utility import list_wrap

import slack
import db_perms


NYCDB_DIR = Path('/nyc-db/src')

DATABASE_URL = os.environ['DATABASE_URL']
USE_TEST_DATA = bool(os.environ.get('USE_TEST_DATA', ''))
DATASET = os.environ.get('DATASET', '')

TEST_DATA_DIR = NYCDB_DIR / 'tests' / 'integration' / 'data'
NYCDB_DATA_DIR = Path('/var/nycdb')

DB_INFO = urllib.parse.urlparse(DATABASE_URL)
DB_PORT = DB_INFO.port or 5432
DB_HOST = DB_INFO.hostname
DB_NAME = DB_INFO.path[1:]
DB_USER = DB_INFO.username
DB_PASSWORD = DB_INFO.password

NYCDB_ARGS = SimpleNamespace(
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    database=DB_NAME,
    port=str(DB_PORT),
    root_dir=str(TEST_DATA_DIR) if USE_TEST_DATA else str(NYCDB_DATA_DIR)
)


class TableInfo(NamedTuple):
    name: str
    dataset: str


def create_temp_schema_name(dataset: str) -> str:
    return f'{get_temp_schema_prefix(dataset)}{int(time.time())}'


def get_temp_schema_prefix(dataset: str) -> str:
    return f'temp_{dataset}_'


def get_friendly_temp_schema_creation_time(name: str) -> str:
    t = time.gmtime(int(name.split('_')[-1]))
    return time.strftime('%Y-%m-%d %H:%M:%S', t) + ' UTC'


def get_temp_schemas(conn, dataset: str) -> List[str]:
    prefix = get_temp_schema_prefix(dataset)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT schema_name from information_schema.schemata "
            f"WHERE schema_name LIKE '{prefix}%'"
        )
        return [row[0] for row in cur.fetchall()]


def get_dataset_tables() -> List[TableInfo]:
    result: List[TableInfo] = []
    for dataset_name, info in nycdb.dataset.datasets().items():
         for schema in list_wrap(info['schema']):
            result.append(TableInfo(name=schema['table_name'], dataset=dataset_name))
    return result


def get_tables_for_dataset(dataset: str) -> List[TableInfo]:
    tables = [
        table for table in get_dataset_tables()
        if table.dataset == dataset
    ]
    if not tables:
        print(f"'{dataset}' is not a valid dataset.")
        sys.exit(1)
    return tables


def get_urls_for_dataset(dataset: str) -> List[str]:
    return [
        fileinfo['url'] for fileinfo in nycdb.dataset.datasets()[dataset]['files']
    ]


def drop_tables_if_they_exist(conn, tables: List[TableInfo], schema: str):
    with conn.cursor() as cur:
        for table in tables:
            name = f"{schema}.{table.name}"
            print(f"Dropping table '{name}' if it exists.")
            cur.execute(f"DROP TABLE IF EXISTS {name}")
    conn.commit()


@contextlib.contextmanager
def save_and_reapply_permissions(conn, tables: List[TableInfo], schema: str):
    '''
    Holy hell this is annoying. See this issue for details:
    https://github.com/JustFixNYC/nycdb-k8s-loader/issues/5
    '''

    # Create blank placeholder tables if they don't already exist,
    # so that any users with default privileges in our schema
    # have expected permissions on them.
    create_blank_tables = ';'.join([
        f'CREATE TABLE IF NOT EXISTS {schema}.{table.name} ()'
        for table in tables
    ])
    with conn.cursor() as cur:
        cur.execute(create_blank_tables)
    conn.commit()

    # Now remember the permissions on the tables.
    grants = ''.join(db_perms.get_grant_sql(conn, table.name, schema)
                     for table in tables)

    # Let the code inside our "with" clause run. It will likely
    # drop the tables and replace them with new ones that have
    # the same name.
    yield

    # Now grant the same permissions to the new tables.
    db_perms.exec_grant_sql(conn, grants)


@contextlib.contextmanager
def create_and_enter_temporary_schema(conn, schema: str):
    print(f"Creating and entering temporary schema '{schema}'.")
    with conn.cursor() as cur:
         cur.execute('; '.join([
             f"DROP SCHEMA IF EXISTS {schema} CASCADE",
             f"CREATE SCHEMA {schema}",
             # Note that we still need public at the end of the search
             # path since we want functions like first(), which are
             # declared in the public schema, to work.
             f"SET search_path TO {schema}, public"
         ]))
    conn.commit()

    try:
        yield
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        print(f"Destroying temporary schema '{schema}'.")
        with conn.cursor() as cur:
            cur.execute('; '.join([
                f'DROP SCHEMA {schema} CASCADE',
                f'SET search_path TO public'
            ]))
        conn.commit()


def change_table_schemas(conn, tables: List[TableInfo], from_schema: str, to_schema: str):
    with conn.cursor() as cur:
        for table in tables:
            name = f"{from_schema}.{table.name}"
            print(f"Setting table '{name}' schema to '{to_schema}'.")
            cur.execute(f"ALTER TABLE {name} SET SCHEMA {to_schema}")
    conn.commit()


def sanity_check():
    assert TEST_DATA_DIR.exists()


def load_dataset(dataset: str):
    tables = get_tables_for_dataset(dataset)
    urls = get_urls_for_dataset(dataset)
    ds = Dataset(dataset, args=NYCDB_ARGS)

    slack.sendmsg(f'Downloading the dataset `{dataset}`...')
    ds.download_files()

    slack.sendmsg(f'Downloaded the dataset `{dataset}`. Loading it into the database...')
    ds.setup_db()
    conn = ds.db.conn
    temp_schema = create_temp_schema_name(dataset)
    with create_and_enter_temporary_schema(conn, temp_schema):
        ds.db_import()
        with save_and_reapply_permissions(conn, tables, 'public'):
            drop_tables_if_they_exist(conn, tables, 'public')
            change_table_schemas(conn, tables, temp_schema, 'public')
    slack.sendmsg(f'Finished loading the dataset `{dataset}` into the database.')
    print("Success!")


def main(argv: List[str]=sys.argv):
    sanity_check()

    NYCDB_DATA_DIR.mkdir(parents=True, exist_ok=True)

    tables = get_dataset_tables()

    dataset = DATASET

    if len(argv) > 1:
        dataset = argv[1]

    if not dataset:
        print(f"Usage: {argv[0]} <dataset>")
        print(f"Alternatively, set the DATASET environment variable.")
        sys.exit(1)

    try:
        load_dataset(dataset)
    except Exception as e:
        slack.sendmsg(f"Alas, an error occurred when loading the dataset `{dataset}`.")
        raise e


if __name__ == '__main__':
    main()
