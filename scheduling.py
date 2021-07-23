from enum import Enum


class Schedule(Enum):
    """
    Abstracts away scheduling frequency from the particular
    scheduling technology being used (AWS, k8s, etc).
    """

    # Daily at around midnight EST.
    DAILY = "0 5 * * ? *"

    # Every other day around midnight EST.
    EVERY_OTHER_DAY = '0 5 */2 * ? *'

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
            return 'rate(365 days)'
        return f"cron({self.value})"

    @property
    def k8s(self) -> str:
        """
        The schedule in terms Kubernetes CronJobs expects.

        For more details, see:

        https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/
        """

        return self.value
