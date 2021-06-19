from datetime import datetime, timedelta

from abf import split_timespan


def test_split_dates():
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

    intervals = split_timespan(
        start_dt=datetime.strptime("2021-01-01", "%Y-%M-%d"),
        end_dt=datetime.strptime("2021-01-02", "%Y-%M-%d"),
        td=timedelta(hours=8),
    )

    assert intervals == [
        [datetime(2021, 1, 1, 0, 1), datetime(2021, 1, 1, 8, 1)],
        [datetime(2021, 1, 1, 8, 1), datetime(2021, 1, 1, 16, 1)],
        [datetime(2021, 1, 1, 16, 1), datetime(2021, 1, 2, 0, 1)],
    ]


def test_split_hours():
    intervals = split_timespan(
        start_dt=datetime.strptime("2021-01-01 00:00", "%Y-%m-%d %H:%M"),
        end_dt=datetime.strptime("2021-01-01 12:00", "%Y-%m-%d %H:%M"),
        td=timedelta(hours=2),
    )
    print(intervals)
    assert intervals == [
        [datetime(2021, 1, 1, 0, 0), datetime(2021, 1, 1, 2, 0)],
        [datetime(2021, 1, 1, 2, 0), datetime(2021, 1, 1, 4, 0)],
        [datetime(2021, 1, 1, 4, 0), datetime(2021, 1, 1, 6, 0)],
        [datetime(2021, 1, 1, 6, 0), datetime(2021, 1, 1, 8, 0)],
        [datetime(2021, 1, 1, 8, 0), datetime(2021, 1, 1, 10, 0)],
        [datetime(2021, 1, 1, 10, 0), datetime(2021, 1, 1, 12, 0)],
    ]
