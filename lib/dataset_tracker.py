from datetime import datetime
import pytz

from .dbhash import AbstractDbHash


class DatasetTracker:
    dataset: str

    def __init__(self, dataset: str, dbhash: AbstractDbHash):
        self.dataset = dataset
        self.dbhash = dbhash

    def update_tracker(self) -> None:
        update_timestamp = datetime.now(pytz.timezone("America/New_York")).isoformat()
        self.dbhash.set_or_delete(self.dataset, update_timestamp)
