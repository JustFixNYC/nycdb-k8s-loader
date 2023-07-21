import pytest

from lib.lastmod import LastmodInfo, UrlModTracker
from lib.dbhash import DictDbHash


class TestLastmodInfo:
    def test_read_from_dbhash_works(self):
        dbh = DictDbHash(
            {"etag:http://boop": "blah", "last_modified:http://boop": "flarg"}
        )
        assert LastmodInfo.read_from_dbhash("http://boop", dbh) == LastmodInfo(
            url="http://boop", etag="blah", last_modified="flarg"
        )

        assert LastmodInfo.read_from_dbhash("http://bar", dbh) == LastmodInfo(
            "http://bar", None, None
        )

    def test_write_to_dbhash_works(self):
        dbh = DictDbHash()
        LastmodInfo("http://boop", "blah", "flarg").write_to_dbhash(dbh)
        assert dbh.d == {
            "etag:http://boop": "blah",
            "last_modified:http://boop": "flarg",
        }

        LastmodInfo("http://boop").write_to_dbhash(dbh)
        assert dbh.d == {}

    def test_from_response_headers_works(self):
        assert LastmodInfo.from_response_headers(
            "http://boop", {"ETag": "blah", "Last-Modified": "flarg"}
        ) == LastmodInfo("http://boop", "blah", "flarg")

    def test_to_request_headers_works(self):
        assert LastmodInfo("http://boop", "blah", "flarg").to_request_headers() == {
            "If-None-Match": "blah",
            "If-Modified-Since": "flarg",
        }


class TestUrlModTracker:
    def setup_method(self):
        self.dbh = DictDbHash()

    def test_it_updates_lastmods(self, requests_mock):
        requests_mock.get("https://boop", text="blah", headers={"ETag": "blah"})
        mt = UrlModTracker(["https://boop"], self.dbh)
        assert mt.did_any_urls_change() is True
        assert self.dbh.d == {}

        mt.update_lastmods()
        assert self.dbh.d == {"etag:https://boop": "blah"}

    def test_it_reports_unchanged_urls(self, requests_mock):
        mt = UrlModTracker(["https://boop"], DictDbHash({"etag:https://boop": "blah"}))
        requests_mock.get(
            "https://boop", request_headers={"If-None-Match": "blah"}, status_code=304
        )
        assert mt.did_any_urls_change() is False

    def test_it_raises_on_bad_http_responses(self, requests_mock):
        mt = UrlModTracker(["https://boop"], self.dbh)
        requests_mock.get("https://boop", status_code=500)

        with pytest.raises(Exception, match="500 Server Error"):
            mt.did_any_urls_change()
