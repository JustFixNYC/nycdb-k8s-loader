from enum import Enum
from typing import Dict, List
import nycdb.dataset


class Schedule(Enum):
    """
    Abstracts away scheduling frequency from the particular scheduling
    technology being used (AWS, k8s, etc). All times are in NYC local time, so
    adjust with daylight savings.
    """

    # Daily at around 11 pm.
    DAILY_11PM = "0 23 * * ?"

    # Daily at around 7am.
    DAILY_7AM = "0 7 * * ?"

    # Daily at around 8am EST.
    DAILY_8AM = "0 12 * * ?"

    # Alternating days around 11 pm (not perfect with Feb and Leap years, but that's ok).
    ODD_DAYS_11PM = "0 23 1-31/2 * ?"
    EVEN_DAYS_11PM = "0 23 2-30/2 * ?"

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


CUSTOM_DATASET_NAMES: List[str] = [
    "wow",
    "oca_address",
    "signature",
    "good_cause_eviction",
]

# The names of all valid NYC-DB datasets.
DATASET_NAMES: List[str] = [
    *list(nycdb.dataset.datasets().keys()),
    *CUSTOM_DATASET_NAMES,
]


# The default schedule for a dataset loader, if
# otherwise unspecified.
DEFAULT_SCHEDULE = Schedule.YEARLY

DATASET_SCHEDULES: Dict[str, Schedule] = {
    "oca": Schedule.DAILY_11PM,
    "dobjobs": Schedule.DAILY_11PM,
    "dob_complaints": Schedule.DAILY_11PM,
    "dob_violations": Schedule.DAILY_11PM,
    "ecb_violations": Schedule.DAILY_11PM,
    "hpd_violations": Schedule.DAILY_11PM,
    "oath_hearings": Schedule.DAILY_11PM,
    "marshal_evictions": Schedule.DAILY_11PM,
    "oca_address": Schedule.DAILY_11PM,
    "hpd_conh": Schedule.DAILY_11PM,
    "hpd_aep": Schedule.DAILY_11PM,
    "hpd_underlying_conditions": Schedule.DAILY_11PM,
    "wow": Schedule.DAILY_7AM,
    "good_cause_eviction": Schedule.DAILY_8AM,
    "hpd_vacateorders": Schedule.ODD_DAYS_11PM,
    "hpd_registrations": Schedule.ODD_DAYS_11PM,
    "hpd_complaints": Schedule.ODD_DAYS_11PM,
    "dof_sales": Schedule.ODD_DAYS_11PM,
    "pad": Schedule.ODD_DAYS_11PM,
    "acris": Schedule.EVEN_DAYS_11PM,
    "pluto_latest": Schedule.ODD_DAYS_11PM,
    "dcp_housingdb": Schedule.ODD_DAYS_11PM,
    "speculation_watch_list": Schedule.ODD_DAYS_11PM,
    "hpd_affordable_production": Schedule.ODD_DAYS_11PM,
    "dof_tax_lien_sale_list": Schedule.ODD_DAYS_11PM,
    "dob_certificate_occupancy": Schedule.ODD_DAYS_11PM,
    "dob_safety_violations": Schedule.ODD_DAYS_11PM,
    "hpd_charges": Schedule.DAILY_11PM,
    "dhs_daily_shelter_count": Schedule.DAILY_11PM,
    "signature": Schedule.DAILY_7AM,
    "dohmh_rodent_inspections": Schedule.DAILY_11PM,
    "hpd_ll44": Schedule.ODD_DAYS_11PM,
    "dos_active_corporations": Schedule.ODD_DAYS_11PM,
    "dof_property_valuation_and_assessments": Schedule.ODD_DAYS_11PM,
    "hpd_litigations": Schedule.DAILY_11PM,
}


def get_schedule_for_dataset(dataset: str) -> Schedule:
    return DATASET_SCHEDULES.get(dataset, DEFAULT_SCHEDULE)


def sanity_check():
    for dataset in DATASET_SCHEDULES:
        assert (
            dataset in DATASET_NAMES
        ), f"'{dataset}' must be a valid NYCDB or custom dataset name"


sanity_check()
