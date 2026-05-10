"""
Calorie Calculator Service
--------------------------
Pure Python — zero Django, zero Streamlit dependencies.
Can be unit-tested in isolation or called from any view or API endpoint.
Directly ported from calculate_session_calories() in the original app.py.
"""

ACTIVITY_METS: dict[str, float] = {
    "Walking": 4.2,
    "Running": 9.8,
    "Cycling": 7.5,
    "Weight Training": 6.2,
    "Yoga": 3.2,
    "Swimming": 8.0,
    "Cardio": 7.0,
    "HIIT": 10.5,
    "Stretching": 2.5,
    "Tennis": 7.0,
}

GOAL_STEP_TARGETS: dict[str, int] = {
    "Fat Loss": 10000,
    "Muscle Gain": 8000,
    "Endurance": 12000,
}

GOAL_CALORIE_DELTAS: dict[str, int] = {
    "Fat Loss": -500,
    "Muscle Gain": 300,
    "Endurance": 100,
}


def calculate_session_calories(
    activity_type: str,
    duration_minutes: float,
    weight_kg: float,
    bmi: float,
) -> float:
    """
    Estimate calories burned for a single activity session.
    Args:
        activity_type: Name of the activity (must match ACTIVITY_METS keys).
        duration_minutes: Session length in minutes.
        weight_kg: User's weight for scaling.
        bmi: User's BMI for additional scaling.
    Returns:
        Estimated calories burned (float, >= 0).
    """
    base_factor = ACTIVITY_METS.get(activity_type, 5.5)
    weight_factor = max(float(weight_kg), 35.0) / 70.0
    bmi_factor = 1.0 + max(float(bmi) - 22.0, 0.0) * 0.015
    return round(max(float(duration_minutes), 0.0) * base_factor * weight_factor * bmi_factor, 2)
