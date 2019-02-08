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

from lib import slack
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

# TODO: This eventually should be in the WoW repo and we should import it.
WOW_NYCDB_DEPENDENCIES = [
    # 'pluto_17v1',        # TODO: I don't think we actually need this one, but make sure.
    'pluto_18v1',
    'rentstab_summary',
    'marshal_evictions_17',
    'hpd_registrations',
    'hpd_violations'
]

# TODO: This eventually should be in the WoW repo and we should import it.
WOW_TABLES = ['wow_bldgs']

# TODO: This eventually should be in the WoW repo and we should import it.
WOW_SCRIPTS = [
    ("Creating hpd_registrations_with_contacts...", 'registrations_with_contacts.sql'),
    ("Creating WoW buildings table...", "create_bldgs_table.sql"),
    ("Adding helper functions...", "helper_functions.sql"),
    ("Creating WoW search function...", "search_function.sql"),
    ("Creating WoW agg function...", "agg_function.sql"),
    ("Creating hpd landlord contact table...", "landlord_contact.sql"),
]


def run_wow_sql(conn):
    with conn.cursor() as cur:
        for msg, filename in WOW_SCRIPTS:
            print(msg)
            sql = (WOW_SQL_DIR / filename).read_text()
            cur.execute(sql)
    conn.commit()


def build(db_url: str):
    slack.sendmsg('Rebuilding Who Owns What tables...')

    cosmetic_dataset_name = 'wow'

    tables = [
        TableInfo(name=name, dataset=cosmetic_dataset_name)
        for name in WOW_TABLES
    ]

    with psycopg2.connect(db_url) as conn:
        temp_schema = create_temp_schema_name(cosmetic_dataset_name)
        with create_and_enter_temporary_schema(conn, temp_schema):
            run_wow_sql(conn)
            ensure_schema_exists(conn, WOW_SCHEMA)
            with save_and_reapply_permissions(conn, tables, WOW_SCHEMA):
                drop_tables_if_they_exist(conn, tables, WOW_SCHEMA)
                change_table_schemas(conn, tables, temp_schema, WOW_SCHEMA)

        # The WoW tables are now ready, but the functions defined by WoW were
        # in the temporary schema that just got destroyed. Let's re-run only
        # the function-creating SQL in the WoW schema now.
        #
        # Note this means that any client which uses the functions will need
        # to set their search_path to "{WOW_SCHEMA}, public" or else the function
        # may not be found or might even crash!
        sql = get_all_create_function_sql(WOW_SQL_DIR, [sqlf for _, sqlf in WOW_SCRIPTS])
        run_sql_if_nonempty(conn, sql, initial_sql=f'SET search_path TO {WOW_SCHEMA}, public')

    slack.sendmsg('Finished rebuilding Who Owns What tables.')


def main(argv: List[str], db_url: str):
    args = docopt.docopt(__doc__, argv=argv)

    if args['build']:
        build(db_url)


if __name__ == '__main__':
    main(argv=sys.argv[1:], db_url=os.environ['DATABASE_URL'])
