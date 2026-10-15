from typing import Optional, NamedTuple, Dict, List, Mapping
import requests

from .dbhash import AbstractDbHash

HEAD_TIMEOUT = (30, 180)


class LastmodInfo(NamedTuple):
    url: str
    etag: Optional[str] = None
    last_modified: Optional[str] = None

    @staticmethod
    def read_from_dbhash(url: str, dbhash: AbstractDbHash) -> "LastmodInfo":
        return LastmodInfo(
            url=url,
            etag=dbhash.get(f"etag:{url}"),
            last_modified=dbhash.get(f"last_modified:{url}"),
        )

    def write_to_dbhash(self, dbhash: AbstractDbHash) -> None:
        dbhash.set_or_delete(f"last_modified:{self.url}", self.last_modified)
        dbhash.set_or_delete(f"etag:{self.url}", self.etag)

    @staticmethod
    def from_response_headers(url: str, headers: Mapping[str, str]) -> "LastmodInfo":
        return LastmodInfo(
            url=url,
            etag=headers.get("ETag"),
            last_modified=headers.get("Last-Modified"),
        )

    def to_request_headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        if self.etag:
            headers["If-None-Match"] = self.etag
        if self.last_modified:
            headers["If-Modified-Since"] = self.last_modified
        return headers

    def metadata_matches(self, other: "LastmodInfo") -> bool:
        if not (self.etag or self.last_modified):
            return False
        if self.etag and other.etag and self.etag == other.etag:
            return True
        if (
            self.last_modified
            and other.last_modified
            and self.last_modified == other.last_modified
        ):
            return True
        return False


class UrlModTracker:
    updated_lastmods: List[LastmodInfo]

    def __init__(self, urls: List[str], dbhash: AbstractDbHash):
        self.urls = urls
        self.dbhash = dbhash
        self.updated_lastmods = []

    def did_any_urls_change(self) -> bool:
        self.updated_lastmods = []
        for url in self.urls:
            print(f"Checking {url}...")
            stored = LastmodInfo.read_from_dbhash(url, self.dbhash)
            res = requests.head(
                url, headers=stored.to_request_headers(), timeout=HEAD_TIMEOUT
            )
            if res.status_code in (405, 501):
                raise requests.HTTPError(
                    f"HEAD not supported for change detection at {url} "
                    f"(HTTP {res.status_code})",
                    response=res,
                )
            if res.status_code == 304:
                continue
            res.raise_for_status()
            current = LastmodInfo.from_response_headers(url, res.headers)
            if stored.metadata_matches(current):
                continue
            self.updated_lastmods.append(current)
        return len(self.updated_lastmods) > 0

    def update_lastmods(self) -> None:
        for lminfo in self.updated_lastmods:
            lminfo.write_to_dbhash(self.dbhash)
