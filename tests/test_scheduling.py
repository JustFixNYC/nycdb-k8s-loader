import pytest

from scheduling import Schedule


@pytest.mark.parametrize('value,expected', [
    (Schedule.DAILY, "cron(0 5 * * ? *)"),
    (Schedule.EVERY_OTHER_DAY, "cron(0 5 */2 * ? *)"),
    (Schedule.YEARLY, "rate(365 days)"),
])
def test_aws_works(value, expected):
    assert value.aws == expected


@pytest.mark.parametrize('value,expected', [
    (Schedule.DAILY, "0 5 * * ? *"),
    (Schedule.EVERY_OTHER_DAY, "0 5 */2 * ? *"),
    (Schedule.YEARLY, "@yearly"),
])
def test_k8s_works(value, expected):
    assert value.k8s == expected
