from lastmod import LastmodInfo
from dbhash import DictDbHash


class TestLastmodInfo:
    def test_read_from_dbhash_works(self):
        dbh = DictDbHash({
            'etag:http://boop': 'blah',
            'last_modified:http://boop': 'flarg'
        })
        assert LastmodInfo.read_from_dbhash('http://boop', dbh) == LastmodInfo(
            url="http://boop", etag="blah", last_modified="flarg")
        
        assert LastmodInfo.read_from_dbhash('http://bar', dbh) == LastmodInfo(
            "http://bar", None, None)

    def test_write_to_dbhash_works(self):
        dbh = DictDbHash()
        LastmodInfo('http://boop', 'blah', 'flarg').write_to_dbhash(dbh)
        assert dbh.d == {
            'etag:http://boop': 'blah',
            'last_modified:http://boop': 'flarg'
        }

        LastmodInfo('http://boop').write_to_dbhash(dbh)
        assert dbh.d == {}


    def test_from_response_headers_works(self):
        assert LastmodInfo.from_response_headers('http://boop', {
            'ETag': 'blah',
            'Last-Modified': 'flarg'
        }) == LastmodInfo('http://boop', 'blah', 'flarg')


    def test_to_request_headers_works(self):
        assert LastmodInfo('http://boop', 'blah', 'flarg').to_request_headers() == {
            'If-None-Match': 'blah',
            'If-Modified-Since': 'flarg'
        }
