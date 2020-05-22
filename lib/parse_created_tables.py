from typing import List
from pathlib import Path
from functools import lru_cache
import sqlparse
from sqlparse.sql import Identifier, IdentifierList
import nycdb


NYCDB_SQL_DIR = Path(nycdb.__file__).parent.resolve() / 'sql'


def get_identifiers(stmt) -> List[Identifier]:
    identifiers: List[Identifier] = []
    for token in stmt.tokens:
        if isinstance(token, IdentifierList):
            token = token.tokens[0]
        if isinstance(token, Identifier):
            identifiers.append(str(token.tokens[0]))
    return identifiers


def parse_created_tables(sql: str) -> List[str]:
    tables: List[str] = []

    for stmt in sqlparse.parse(sql):
        for token in stmt.tokens:
            print(repr(token), token.__class__.__name__)
        identifiers = get_identifiers(stmt)
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
