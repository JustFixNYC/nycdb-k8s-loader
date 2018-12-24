from typing import Optional, NamedTuple, TypeVar, Type, Dict, List
import requests

from dbhash import AbstractDbHash


T = TypeVar('T', bound='LastmodInfo')


class LastmodInfo(NamedTuple):
    url: str
    etag: Optional[str] = None
    last_modified: Optional[str] = None

    @classmethod
    def read_from_dbhash(cls: Type[T], url: str, dbhash: AbstractDbHash) -> T:
        return cls(
            url=url,
            etag=dbhash.get(f'etag:{url}'),
            last_modified=dbhash.get(f'last_modified:{url}')
        )

    def write_to_dbhash(self, dbhash: AbstractDbHash) -> None:
        dbhash.set_or_delete(f'last_modified:{self.url}', self.last_modified)
        dbhash.set_or_delete(f'etag:{self.url}', self.etag)

    @classmethod
    def from_response_headers(cls: Type[T], url: str, headers: Dict[str, str]) -> T:
        return cls(
            url=url,
            etag=headers.get('ETag'),
            last_modified=headers.get('Last-Modified')
        )

    def to_request_headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        if self.etag:
            headers['If-None-Match'] = self.etag
        if self.last_modified:
            headers['If-Modified-Since'] = self.last_modified
        return headers


class UrlModTracker:
    updated_lastmods: List[LastmodInfo]

    def __init__(self, urls: List[str], dbhash: AbstractDbHash):
        self.urls = urls
        self.dbhash = AbstractDbHash
        self.updated_lastmods = []

    def did_any_urls_change(self) -> bool:
        self.updated_lastmods = []
        for url in self.urls:
            lminfo = LastmodInfo.read_from_dbhash(url, self.dbhash)
            res = requests.get(url, headers=lminfo.to_request_headers(), stream=True)
            if res.status_code == 200:
                self.updated_lastmods.append(
                    LastmodInfo.from_response_headers(url, res.headers))
            elif res.status_code != 304:
                res.raise_for_status()
            res.close()
        return len(self.updated_lastmods) > 0

    def update_lastmods(self) -> None:
        for lminfo in self.update_lastmods:
            lminfo.write_to_dbhash(self.dbhash)
