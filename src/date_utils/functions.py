import pandas as pd

def partition_date_range(
    begin_date: pd.Timestamp, end_date: pd.Timestamp, num_dates_per_part: int
):
    """
    Divides the date range [begin_date, end_date] into parts with smaller
    ranges, each range is inclusive with `num_dates_per_part` number of dates,
    e.g. the range of "2020-01-01" to "2020-01-01" corresponds to
    num_dates_per_part = 1.

    If the date range doesn't divide evenly by num_dates_per_part, the length of
    the last part will be shorter than num_dates_per_part.

    Args:
        begin_date: beginning date
        end_date: ending date
        dates_per_part: number of days in part, default to 60 days

    Yields:
        Tuples of (sub_begin_date, sub_end_date) in each part
    """
    
    l = []
    sub_begin_date = begin_date
    while sub_begin_date <= end_date:
        sub_end_date = min(
            end_date, sub_begin_date + pd.Timedelta(days=num_dates_per_part - 1)
        )
        l.append([sub_begin_date.date().strftime('%Y-%m-%d'), sub_end_date.date().strftime('%Y-%m-%d')])
        sub_begin_date = sub_end_date + pd.Timedelta(days=1)
        
    return l