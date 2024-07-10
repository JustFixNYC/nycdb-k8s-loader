import freezegun
from datetime import datetime

from lib.dataset_tracker import DatasetTracker
from lib.dbhash import DictDbHash


class TestDatasetTracker:
    def setup_method(self):
        self.dbh = DictDbHash()

    def test_it_updates_date(self):
        with freezegun.freeze_time("2024-01-01"):
            dt = DatasetTracker("blah", self.dbh)
            assert self.dbh.d == {}

            dt.update_tracker()
            assert self.dbh.d == {"blah": datetime.now().isoformat()}

