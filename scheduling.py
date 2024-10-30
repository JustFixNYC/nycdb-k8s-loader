from enum import Enum
from typing import Dict, List
import nycdb.dataset


class Schedule(Enum):
    """
    Abstracts away scheduling frequency from the particular scheduling
    technology being used (AWS, k8s, etc). All times are in NYC local time, so
    adjust with daylight savings.
    """

    # Daily at around midnight.
    DAILY_12AM = "0 0 * * ?"

    # Daily at around 7am.
    DAILY_7AM = "0 7 * * ?"

    # Every other day around midnight.
    EVERY_OTHER_DAY = "0 0 */2 * ?"

    # Once per year.
    YEARLY = "@yearly"

    @property
    def k8s(self) -> str:
        """
        The schedule in terms Kubernetes CronJobs expects.

        For more details, see:

        https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/
        """

        return self.value


CUSTOM_DATASET_NAMES: List[str] = ["wow", "oca_address", "signature"]

# The names of all valid NYC-DB datasets.
DATASET_NAMES: List[str] = [
    *list(nycdb.dataset.datasets().keys()),
    *CUSTOM_DATASET_NAMES,
]


# The default schedule for a dataset loader, if
# otherwise unspecified.
DEFAULT_SCHEDULE = Schedule.YEARLY

DATASET_SCHEDULES: Dict[str, Schedule] = {
    "oca": Schedule.DAILY_12AM,
    "dobjobs": Schedule.DAILY_12AM,
    "dob_complaints": Schedule.DAILY_12AM,
    "dob_violations": Schedule.DAILY_12AM,
    "ecb_violations": Schedule.DAILY_12AM,
    "hpd_violations": Schedule.DAILY_12AM,
    "oath_hearings": Schedule.DAILY_12AM,
    "marshal_evictions": Schedule.DAILY_12AM,
    "oca_address": Schedule.DAILY_12AM,
    "hpd_conh": Schedule.DAILY_12AM,
    "hpd_aep": Schedule.DAILY_12AM,
    "hpd_underlying_conditions": Schedule.DAILY_12AM,
    "wow": Schedule.DAILY_7AM,
    "hpd_vacateorders": Schedule.EVERY_OTHER_DAY,
    "hpd_registrations": Schedule.EVERY_OTHER_DAY,
    "hpd_complaints": Schedule.EVERY_OTHER_DAY,
    "dof_sales": Schedule.EVERY_OTHER_DAY,
    "pad": Schedule.EVERY_OTHER_DAY,
    "acris": Schedule.EVERY_OTHER_DAY,
    "pluto_latest": Schedule.EVERY_OTHER_DAY,
    "dcp_housingdb": Schedule.EVERY_OTHER_DAY,
    "speculation_watch_list": Schedule.EVERY_OTHER_DAY,
    "hpd_affordable_production": Schedule.EVERY_OTHER_DAY,
    "dof_tax_lien_sale_list": Schedule.EVERY_OTHER_DAY,
    "dob_certificate_occupancy": Schedule.EVERY_OTHER_DAY,
    "dob_safety_violations": Schedule.EVERY_OTHER_DAY,
    "hpd_charges": Schedule.DAILY_12AM,
    "dhs_daily_shelter_count": Schedule.DAILY_12AM,
    "signature": Schedule.DAILY_7AM,
    "dohmh_rodent_inspections": Schedule.DAILY_12AM,
    "hpd_ll44": Schedule.EVERY_OTHER_DAY,
}


def get_schedule_for_dataset(dataset: str) -> Schedule:
    return DATASET_SCHEDULES.get(dataset, DEFAULT_SCHEDULE)


def sanity_check():
    for dataset in DATASET_SCHEDULES:
        assert (
            dataset in DATASET_NAMES
        ), f"'{dataset}' must be a valid NYCDB or custom dataset name"


sanity_check()
