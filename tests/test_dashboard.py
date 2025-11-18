
from src.dashboard.Trentino_Tourism_Forecast import get_forecast_weeks  

def test_next_week_normal_case():
    """Week 10 → Week 11"""
    result = get_forecast_weeks(10, 2024, "Next week")
    assert result == [(2024, 11)]

def test_next_week_year_rollover():
    """Week 52 → Week 1 of next year"""
    result = get_forecast_weeks(52, 2024, "Next week")
    assert result == [(2025, 1)]

def test_this_week():
    result = get_forecast_weeks(30, 2024, "This week")
    assert result == [(2024, 30)]

def test_this_and_next_week_in_december():
    """Week 52 of 2024 should give (2024,52) and (2025,1)"""
    result = get_forecast_weeks(52, 2024, "This and next week")
    assert result == [(2024, 52), (2025, 1)]

def test_first_week_of_january():
    """Week 1 stays in same year unless Next Week requested"""
    result = get_forecast_weeks(1, 2025, "This week")
    assert result == [(2025, 1)]
