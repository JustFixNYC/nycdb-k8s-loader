import freezegun
from datetime import datetime
import pytz

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
            update_timestamp = datetime.now(
                pytz.timezone("America/New_York")
            ).isoformat()
            assert self.dbh.d == {"blah": update_timestamp}
