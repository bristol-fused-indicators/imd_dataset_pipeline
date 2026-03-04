from datetime import date

from dateutil.relativedelta import relativedelta


def get_window_bounds(snapshot_date: str, window_months: int) -> tuple[date, date]:
    """Calculate the start and end dates for a temporal window."""

    end = date.fromisoformat(snapshot_date)
    start = end - relativedelta(months=window_months)
    return start, end


def months_in_window(snapshot_date: str, window_months: int) -> list[str]:
    """Generate YYYY-MM strings for each month in the temporal window"""

    end = date.fromisoformat(snapshot_date)
    months = []
    for i in range(window_months - 1, -1, -1):
        month_date = end - relativedelta(months=i)
        months.append(month_date.strftime("%Y-%m"))
    return months
