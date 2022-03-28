import pytest

from scheduling import Schedule


@pytest.mark.parametrize('value,expected', [
    (Schedule.DAILY_12AM, "0 4 * * ?"),
    (Schedule.DAILY_7AM, "0 11 * * ?"),
    (Schedule.EVERY_OTHER_DAY, "0 4 */2 * ?"),
    (Schedule.YEARLY, "@yearly"),
])
def test_k8s_works(value, expected):
    assert value.k8s == expected
