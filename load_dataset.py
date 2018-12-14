import os
import sys
import subprocess
from pathlib import Path
from typing import NamedTuple, List
import urllib.parse
import psycopg2
import yaml


NYCDB_DIR = Path('/nyc-db/src')

DATABASE_URL = os.environ['DATABASE_URL']
USE_TEST_DATA = bool(os.environ.get('USE_TEST_DATA', ''))

DATASETS_YML = NYCDB_DIR / 'nycdb' / 'datasets.yml'
TEST_DATA_DIR = NYCDB_DIR / 'tests' / 'integration' / 'data'
NYCDB_DATA_DIR = Path('/var/nycdb')

DB_INFO = urllib.parse.urlparse(DATABASE_URL)
DB_PORT = DB_INFO.port or 5432
DB_HOST = DB_INFO.hostname
DB_NAME = DB_INFO.path[1:]
DB_USER = DB_INFO.username
DB_PASSWORD = DB_INFO.password

NYCDB_CMD = [
    'nycdb',
    '-D', DB_NAME,
    '-H', DB_HOST,
    '-U', DB_USER,
    '-P', DB_PASSWORD,
    '--port', str(DB_PORT),
    '--root-dir', str(TEST_DATA_DIR) if USE_TEST_DATA else str(NYCDB_DATA_DIR),
]


class TableInfo(NamedTuple):
    name: str
    dataset: str


def get_dataset_tables(yaml_file: Path=DATASETS_YML) -> List[TableInfo]:
    yml = yaml.load(yaml_file.read_text())
    result: List[TableInfo] = []
    for dataset_name, info in yml.items():
        schema = info['schema']
        if isinstance(schema, dict):
            schemas = [schema]
        else:
            schemas = schema
        for schema in schemas:
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


def call_nycdb(*args: str):
    subprocess.check_call(NYCDB_CMD + list(args))


def sanity_check():
    assert DATASETS_YML.exists()
    assert TEST_DATA_DIR.exists()


def main():
    sanity_check()

    NYCDB_DATA_DIR.mkdir(parents=True, exist_ok=True)

    tables = get_dataset_tables()

    if len(sys.argv) <= 1:
        print(f"Usage: {sys.argv[0]} <dataset>")
        sys.exit(1)
    dataset = sys.argv[1]
    tables = get_tables_for_dataset(dataset)
    call_nycdb('--download', dataset)
    drop_tables_if_they_exist(tables)
    call_nycdb('--load', dataset)
    print("Success!")


if __name__ == '__main__':
    main()
