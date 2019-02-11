from typing import List
from pathlib import Path
from functools import lru_cache
import sqlparse
from sqlparse.sql import Identifier
import nycdb


NYCDB_SQL_DIR = Path(nycdb.__file__).parent.resolve() / 'sql'


def parse_created_tables(sql: str) -> List[str]:
    tables: List[str] = []

    for stmt in sqlparse.parse(sql):
        identifiers = [
            str(token.tokens[0]) for token in stmt.tokens
            if isinstance(token, Identifier)
        ]
        keywords = [
            str(token).upper() for token in stmt.tokens
            if token.is_keyword
        ]
        if keywords[:2] == ['CREATE', 'TABLE'] and identifiers:
            tables.append(identifiers[0])

    return tables


@lru_cache()
def _parse_sql_file(path: Path) -> List[str]:
    return parse_created_tables(path.read_text())


def parse_nycdb_created_tables(filenames: List[str]) -> List[str]:
    return parse_created_tables_in_dir(NYCDB_SQL_DIR, filenames)


def parse_created_tables_in_dir(root_dir: Path, filenames: List[str]) -> List[str]:
    result: List[str] = []
    for filename in filenames:
        result.extend(_parse_sql_file(root_dir / filename))
    return result
