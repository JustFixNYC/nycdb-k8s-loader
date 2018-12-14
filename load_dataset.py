import os
import sys
from pathlib import Path
from typing import NamedTuple, List
from types import SimpleNamespace
from copy import deepcopy
import urllib.parse
import nycdb.dataset
from nycdb.dataset import Dataset
from nycdb.utility import list_wrap


NYCDB_DIR = Path('/nyc-db/src')

DATABASE_URL = os.environ['DATABASE_URL']
USE_TEST_DATA = bool(os.environ.get('USE_TEST_DATA', ''))

TEST_DATA_DIR = NYCDB_DIR / 'tests' / 'integration' / 'data'
NYCDB_DATA_DIR = Path('/var/nycdb')
TEMP_SCHEMA = 'temp_schema'

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


def change_table_schemas(conn, tables: List[TableInfo], schema: str):
    with conn.cursor() as cur:
        for table in tables:
            print(f"Setting table '{table.name}' schema to '{schema}'.")
            cur.execute(f"ALTER TABLE {table.name} SET SCHEMA {schema}")


def alter_dataset_to_use_table_prefix(dataset: Dataset, prefix: str):
    schemas = deepcopy(dataset.schemas)
    for schema in schemas:
        name = schema['table_name']
        schema['table_name'] = f"{prefix}{name}"
    dataset.schemas = schemas


def sanity_check():
    assert TEST_DATA_DIR.exists()


def main():
    sanity_check()

    NYCDB_DATA_DIR.mkdir(parents=True, exist_ok=True)

    tables = get_dataset_tables()

    if len(sys.argv) <= 1:
        print(f"Usage: {sys.argv[0]} <dataset>")
        sys.exit(1)

    dataset_name = sys.argv[1]

    tables = get_tables_for_dataset(dataset_name)

    ds = Dataset(dataset_name, args=NYCDB_ARGS)
    ds.setup_db()
    conn = ds.db.conn

    ds.download_files()

    with conn.cursor() as cur:
        cur.execute('; '.join([
            f"DROP SCHEMA IF EXISTS {TEMP_SCHEMA} CASCADE",
            f"CREATE SCHEMA {TEMP_SCHEMA}",
            f"SET search_path TO {TEMP_SCHEMA}"
        ]))

    ds.db_import()

    change_table_schemas(conn, tables, 'public')

    print("Success!")


if __name__ == '__main__':
    main()
