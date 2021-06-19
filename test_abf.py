from datetime import datetime, timedelta

from abf import split_timespan


def test_split_1():
    intervals = split_timespan(
        start_dt=datetime.strptime("2021-01-01", "%Y-%M-%d"),
        end_dt=datetime.strptime("2021-01-02", "%Y-%M-%d"),
        td=timedelta(hours=6),
    )

    assert intervals == [
        [datetime(2021, 1, 1, 0, 1), datetime(2021, 1, 1, 6, 1)],
        [datetime(2021, 1, 1, 6, 1), datetime(2021, 1, 1, 12, 1)],
        [datetime(2021, 1, 1, 12, 1), datetime(2021, 1, 1, 18, 1)],
        [datetime(2021, 1, 1, 18, 1), datetime(2021, 1, 2, 0, 1)],
    ]
