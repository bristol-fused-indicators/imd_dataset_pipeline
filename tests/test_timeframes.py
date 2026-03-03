from datetime import date

from imd_pipeline.utils.timeframes import get_window_bounds, months_in_window


class TestWindowBounds:
    """Tests for window_bounds, used by Land Registry for date filtering."""

    def test_twelve_month_window(self):
        start, end = get_window_bounds("2025-12-01", 12)
        assert start == date(2024, 12, 1)
        assert end == date(2025, 12, 1)

    def test_three_month_window(self):
        start, end = get_window_bounds("2025-06-01", 3)
        assert start == date(2025, 3, 1)
        assert end == date(2025, 6, 1)

    def test_window_crossing_year_boundary(self):
        start, end = get_window_bounds("2025-03-01", 6)
        assert start == date(2024, 9, 1)
        assert end == date(2025, 3, 1)

    def test_single_month_window(self):
        start, end = get_window_bounds("2025-06-01", 1)
        assert start == date(2025, 5, 1)
        assert end == date(2025, 6, 1)


class TestMonthsInWindow:
    """Tests for months_in_window, used by Police UK for file selection."""

    def test_twelve_month_window_includes_snapshot_month(self):
        months = months_in_window("2025-12-01", 12)
        assert len(months) == 12
        assert months[0] == "2025-01"
        assert months[-1] == "2025-12"

    def test_three_month_window(self):
        months = months_in_window("2025-12-01", 3)
        assert months == ["2025-10", "2025-11", "2025-12"]

    def test_window_crossing_year_boundary(self):
        months = months_in_window("2025-03-01", 4)
        assert months == ["2024-12", "2025-01", "2025-02", "2025-03"]

    def test_single_month_window(self):
        months = months_in_window("2025-06-01", 1)
        assert months == ["2025-06"]

    def test_chronological_order(self):
        months = months_in_window("2025-06-01", 6)
        assert months == sorted(months)


class TestTemporalSemantics:
    """Documents the existing offset between the two temporal functions.

    get_window_bounds and months_in_window currently interpret the same
    parameters with a one-month offset. These tests pin that behaviour.

    We may want to change how this works later, but having the test
    should mean that we realize we are changing it deliberately and
    not break the modules that are using the code.

    Probably align and remove in the same pr that deals with all the temporal config
    """

    def test_offset_is_documented_not_hidden(self):
        """For the same inputs, the two functions cover different months."""
        start, _ = get_window_bounds("2025-12-01", 12)
        months = months_in_window("2025-12-01", 12)

        # get_window_bounds starts at Dec 2024
        assert start == date(2024, 12, 1)

        # months_in_window starts at Jan 2025 (does not include Dec 2024)
        assert months[0] == "2025-01"

        # months_in_window includes Dec 2025 (the snapshot month)
        assert months[-1] == "2025-12"
