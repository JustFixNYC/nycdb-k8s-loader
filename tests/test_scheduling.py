import pytest

from scheduling import Schedule


@pytest.mark.parametrize(
    "value,expected",
    [
        (Schedule.DAILY_11PM, "0 23 * * ?"),
        (Schedule.DAILY_7AM, "0 7 * * ?"),
        (Schedule.ODD_DAYS_11PM, "0 23 1-31/2 * ?"),
        (Schedule.EVEN_DAYS_11PM, "0 23 2-30/2 * ?"),
        (Schedule.YEARLY, "@yearly"),
    ],
)
def test_k8s_works(value, expected):
    assert value.k8s == expected
