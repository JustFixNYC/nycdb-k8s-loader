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
    TableInfo
)


WOW_SCHEMA = 'wow'

WOW_DIR = Path('/who-owns-what')

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
            sql = (WOW_DIR / 'sql' / filename).read_text()
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

    slack.sendmsg('Finished rebuilding Who Owns What tables.')


def main(argv: List[str], db_url: str):
    args = docopt.docopt(__doc__, argv=argv)

    if args['build']:
        build(db_url)


if __name__ == '__main__':
    main(argv=sys.argv[1:], db_url=os.environ['DATABASE_URL'])
