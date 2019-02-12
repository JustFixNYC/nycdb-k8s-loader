"""\
Perform operations on the Who Owns What database tables.

Usage:
  wowutil.py build

Options:
  -h --help     Show this screen.

Environment variables:
  DATABASE_URL           The URL of the NYC-DB and WoW database.
"""

import sys
import os
from pathlib import Path
from typing import List
import docopt
import psycopg2
import yaml

from lib import slack
from lib.parse_created_tables import parse_created_tables_in_dir
from load_dataset import (
    create_temp_schema_name,
    create_and_enter_temporary_schema,
    save_and_reapply_permissions,
    ensure_schema_exists,
    drop_tables_if_they_exist,
    change_table_schemas,
    run_sql_if_nonempty,
    get_all_create_function_sql,
    TableInfo
)


WOW_SCHEMA = 'wow'

WOW_DIR = Path('/who-owns-what')

WOW_SQL_DIR = Path(WOW_DIR / 'sql')

WOW_YML = yaml.load((WOW_DIR / 'who-owns-what.yml').read_text())

WOW_SCRIPTS: List[str] = WOW_YML['sql']


def run_wow_sql(conn):
    with conn.cursor() as cur:
        for filename in WOW_SCRIPTS:
            print(f"Running {filename}...")
            sql = (WOW_SQL_DIR / filename).read_text()
            cur.execute(sql)
    conn.commit()


def install_db_extensions(conn):
    with conn.cursor() as cur:
        cur.execute('CREATE EXTENSION IF NOT EXISTS pg_trgm')
    conn.commit()


def drop_functions_if_they_exist(conn, schema):
    with conn.cursor() as cur:
        # TODO: It would be nice if we didn't have to hard-code this, but
        # apparently dropping all functions from a schema is non-trivial.
        cur.execute(f'DROP FUNCTION IF EXISTS wow.get_assoc_addrs_from_bbl(text)')
    conn.commit()


def build(db_url: str):
    slack.sendmsg('Rebuilding Who Owns What tables...')

    cosmetic_dataset_name = 'wow'

    tables = [
        TableInfo(name=name, dataset=cosmetic_dataset_name)
        for name in parse_created_tables_in_dir(WOW_SQL_DIR, WOW_SCRIPTS)
    ]

    with psycopg2.connect(db_url) as conn:
        install_db_extensions(conn)
        temp_schema = create_temp_schema_name(cosmetic_dataset_name)
        with create_and_enter_temporary_schema(conn, temp_schema):
            run_wow_sql(conn)
            ensure_schema_exists(conn, WOW_SCHEMA)
            with save_and_reapply_permissions(conn, tables, WOW_SCHEMA):
                drop_functions_if_they_exist(conn, WOW_SCHEMA)
                drop_tables_if_they_exist(conn, tables, WOW_SCHEMA)
                change_table_schemas(conn, tables, temp_schema, WOW_SCHEMA)

        # The WoW tables are now ready, but the functions defined by WoW were
        # in the temporary schema that just got destroyed. Let's re-run only
        # the function-creating SQL in the WoW schema now.
        #
        # Note this means that any client which uses the functions will need
        # to set their search_path to "{WOW_SCHEMA}, public" or else the function
        # may not be found or might even crash!
        print(f"Re-running CREATE FUNCTION statements in the {WOW_SCHEMA} schema...")
        sql = get_all_create_function_sql(WOW_SQL_DIR, WOW_SCRIPTS)
        run_sql_if_nonempty(conn, sql, initial_sql=f'SET search_path TO {WOW_SCHEMA}, public')

    slack.sendmsg('Finished rebuilding Who Owns What tables.')


def main(argv: List[str], db_url: str):
    args = docopt.docopt(__doc__, argv=argv)

    if args['build']:
        build(db_url)


if __name__ == '__main__':
    main(argv=sys.argv[1:], db_url=os.environ['DATABASE_URL'])
