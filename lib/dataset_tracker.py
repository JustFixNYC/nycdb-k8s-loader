from datetime import datetime
from .dbhash import AbstractDbHash


class DatasetTracker:
    dataset: str

    def __init__(self, dataset: str, dbhash: AbstractDbHash):
        self.dataset = dataset
        self.dbhash = dbhash

    def update_tracker(self) -> None:
        self.dbhash.set_or_delete(self.dataset, datetime.now().isoformat())
