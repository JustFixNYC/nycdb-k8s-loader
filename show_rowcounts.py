"""\
Show the row counts of datasets in the database.

Usage:
  show_rowcounts.py <dataset>...

Options:
  -h --help     Show this screen.

Environment variables:
  DATABASE_URL           The URL of the NYC-DB database.
"""

import os
import sys
from typing import List, Tuple, Iterator
import psycopg2
import docopt
import nycdb.datasets
from nycdb.utility import list_wrap


def get_tables_for_datasets(names: List[str]) -> List[str]:
    tables: List[str] = []

    for name in names:
        schema = list_wrap(nycdb.datasets.datasets()[name]['schema'])
        tables.extend([t['table_name'] for t in schema])

    return tables


def validate_and_get_dataset_names(dsnames: List[str]) -> List[str]:
    dataset_names: List[str] = []
    all_dataset_names = nycdb.datasets.datasets().keys()

    for name in dsnames:
        if name == 'all':
            dataset_names.extend(all_dataset_names)
        elif name not in all_dataset_names:
            print(f"ERROR: {name} is not a valid dataset. Please choose from:")
            print('\n'.join(all_dataset_names))
            print('all')
            sys.exit(1)
        else:
            dataset_names.append(name)

    return dataset_names


def get_rowcounts(url: str, table_names: List[str]) -> Iterator[Tuple[str, int]]:
    with psycopg2.connect(url) as conn:
        with conn.cursor() as cur:
            for table in table_names:
                cur.execute(f'SELECT COUNT(*) FROM {table}')
                count: int = cur.fetchone()[0]
                yield table, count


def main():
    args = docopt.docopt(__doc__)
    dataset_names = validate_and_get_dataset_names(args['<dataset>'])
    tables = get_tables_for_datasets(dataset_names)

    for table, count in get_rowcounts(os.environ['DATABASE_URL'], tables):
        print(f"{table} has {count} rows.")


if __name__ == '__main__':
    main()
