import os
import sys
from pathlib import Path
from typing import NamedTuple, List
from types import SimpleNamespace
import urllib.parse
import psycopg2
from nycdb import dataset
from nycdb.utility import list_wrap


NYCDB_DIR = Path('/nyc-db/src')

DATABASE_URL = os.environ['DATABASE_URL']
USE_TEST_DATA = bool(os.environ.get('USE_TEST_DATA', ''))

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


def get_dataset_tables() -> List[TableInfo]:
    result: List[TableInfo] = []
    for dataset_name, info in dataset.datasets().items():
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


def drop_tables_if_they_exist(tables: List[TableInfo]):
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            for table in tables:
                print(f"Dropping table '{table.name}' if it exists.")
                cur.execute(f"DROP TABLE IF EXISTS {table.name}")


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
    ds = dataset.Dataset(dataset_name, args=NYCDB_ARGS)
    ds.download_files()
    drop_tables_if_they_exist(tables)
    ds.db_import()
    print("Success!")


if __name__ == '__main__':
    main()
