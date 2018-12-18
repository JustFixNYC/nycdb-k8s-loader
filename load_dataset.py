import os
import sys
import subprocess
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


def drop_tables_if_they_exist(conn, tables: List[TableInfo]):
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

    dataset = DATASET

    if not dataset:
        if len(sys.argv) <= 1:
            print(f"Usage: {sys.argv[0]} <dataset>")
            print(f"Alternatively, set the DATASET environment variable.")
            sys.exit(1)
        dataset = sys.argv[1]

    tables = get_tables_for_dataset(dataset)
    ds = Dataset(dataset, args=NYCDB_ARGS)
    ds.setup_db()
    conn = ds.db.conn

    slack.sendmsg(f'Downloading the dataset `{dataset}`...')
    ds.download_files()
    slack.sendmsg(f'Downloaded the dataset `{dataset}`. Loading it into the database...')
    drop_tables_if_they_exist(conn, tables)
    ds.db_import()
    slack.sendmsg(f'Finished loading the dataset `{dataset}` into the database.')
    print("Success!")


if __name__ == '__main__':
    main()
