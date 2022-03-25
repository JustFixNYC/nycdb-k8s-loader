from enum import Enum
from typing import Dict, List
import nycdb.dataset


class Schedule(Enum):
    """
    Abstracts away scheduling frequency from the particular
    scheduling technology being used (AWS, k8s, etc).
    """

    # Daily at around midnight EST.
    DAILY = "0 5 * * ?"

    # Every other day around midnight EST.
    EVERY_OTHER_DAY = "0 5 */2 * ?"

    # Once per year.
    YEARLY = "@yearly"

    @property
    def aws(self) -> str:
        """
        The schedule in terms Amazon Web Services (AWS) expects.

        Note that all times must be specified in UTC.

        For more details on AWS's schedule expressions, see:

        https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/ScheduledEvents.html
        """

        if self == Schedule.YEARLY:
            return "rate(365 days)"
        # Note the additional "*"; AWS has a sixth field that is the year.
        return f"cron({self.value} *)"

    @property
    def k8s(self) -> str:
        """
        The schedule in terms Kubernetes CronJobs expects.

        For more details, see:

        https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/
        """

        return self.value


# The names of all valid NYC-DB datasets.
DATASET_NAMES: List[str] = list(nycdb.dataset.datasets().keys())

# The default schedule for a dataset loader, if
# otherwise unspecified.
DEFAULT_SCHEDULE = Schedule.YEARLY

DATASET_SCHEDULES: Dict[str, Schedule] = {
    "oca": Schedule.DAILY,
    "dobjobs": Schedule.DAILY,
    "dob_complaints": Schedule.DAILY,
    "dob_violations": Schedule.DAILY,
    "ecb_violations": Schedule.DAILY,
    "hpd_violations": Schedule.DAILY,
    "oath_hearings": Schedule.DAILY,
    "marshal_evictions": Schedule.DAILY,
    "hpd_vacateorders": Schedule.EVERY_OTHER_DAY,
    "hpd_registrations": Schedule.EVERY_OTHER_DAY,
    "hpd_complaints": Schedule.EVERY_OTHER_DAY,
    "dof_sales": Schedule.EVERY_OTHER_DAY,
    "pad": Schedule.EVERY_OTHER_DAY,
    "acris": Schedule.EVERY_OTHER_DAY,
}


def get_schedule_for_dataset(dataset: str) -> Schedule:
    return DATASET_SCHEDULES.get(dataset, DEFAULT_SCHEDULE)


def sanity_check():
    for dataset in DATASET_SCHEDULES:
        assert (
            dataset in DATASET_NAMES
        ), f"'{dataset}' must be a valid NYCDB dataset name"


sanity_check()
