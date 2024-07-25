import abc
from typing import Optional, Iterable, Any, Dict
from sqlite3 import Connection, Cursor


class AbstractDbHash(abc.ABC):
    @abc.abstractmethod
    def get(self, key: str) -> Optional[str]:
        ...

    @abc.abstractmethod
    def __setitem__(self, key: str, value: str) -> None:
        ...

    @abc.abstractmethod
    def __delitem__(self, key: str) -> None:
        ...

    def __getitem__(self, key: str) -> str:
        item = self.get(key)
        if item is None:
            raise KeyError(key)
        return item

    def __contains__(self, key: str) -> bool:
        return self.get(key) is not None

    def set_or_delete(self, key: str, value: Optional[str]) -> None:
        if value is not None:
            self[key] = value
        elif key in self:
            del self[key]


class DictDbHash(AbstractDbHash):
    def __init__(self, d: Optional[Dict[str, str]] = None):
        if d is None:
            d = {}
        self.d = d

    def get(self, key: str) -> Optional[str]:
        return self.d.get(key)

    def __setitem__(self, key: str, value: str) -> None:
        self.d[key] = value

    def __delitem__(self, key: str) -> None:
        del self.d[key]


class SqlDbHash(AbstractDbHash):
    PARAM_SUBST_STRINGS: Dict[str, str] = {
        "sqlite3": r"?",
        "psycopg2.extensions": r"%s",
        "psycopg": r"%s",
    }

    def __init__(self, conn: Connection, table: str, autocommit: bool = True):
        self.table = table
        self.param_subst = self.__class__.PARAM_SUBST_STRINGS[conn.__class__.__module__]
        self.autocommit = autocommit
        self.conn = conn
        self._init_db()

    def _init_db(self) -> None:
        self._exec_sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                key text PRIMARY KEY NOT NULL,
                value text NOT NULL
            )
            """
        )

    def _exec_sql(self, sql: str, params: Iterable[Any] = tuple()) -> Cursor:
        sql = sql.replace("?", self.param_subst)
        cur = self.conn.cursor()
        cur.execute(sql, params) # type: ignore
        return cur

    def __setitem__(self, key: str, value: str) -> None:
        if self.get(key) is not None:
            self._exec_sql(
                f"UPDATE {self.table} SET value = ? WHERE key = ?", (value, key)
            )
        else:
            self._exec_sql(
                f"INSERT INTO {self.table} (key, value) VALUES (?, ?)", (key, value)
            )
        if self.autocommit:
            self.conn.commit()

    def __delitem__(self, key: str) -> None:
        if key not in self:
            raise KeyError(key)
        self._exec_sql(f"DELETE FROM {self.table} WHERE key = ?", (key,))
        if self.autocommit:
            self.conn.commit()

    def get(self, key: str) -> Optional[str]:
        cur = self._exec_sql(f"SELECT value FROM {self.table} WHERE key = ?", (key,))
        result = cur.fetchone()
        return None if result is None else result[0]
