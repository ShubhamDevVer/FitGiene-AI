"""
Weekly Planner Service
-----------------------
Generates a 7-day personalized, medically-aware workout schedule.
Ported directly from generate_weekly_plan() in app.py.
Pure Python — no Django, no Streamlit.
"""
from __future__ import annotations

EXERCISE_DB: dict[str, list[dict[str, str]]] = {
    "Weight Training": [
        {"name": "Push-ups", "gif": "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExajFweTExenlwMG1kNHpvOTZvc3U0YnBnZGY0ZDN2ZjdpaTM0aTdxcSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/3pY8FQP9uMtDKXkYqX/giphy.gif", "detail": "3 sets of 12"},
        {"name": "Bodyweight Squats", "gif": "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExaDh1YmN4OG0ybTluYXB5YzM5ZWJjZXBiNGNncHBndHNjMGJwdHJvMCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/1C1ipHPEs4Vjwglwza/giphy.gif", "detail": "3 sets of 15"},
        {"name": "Dumbbell Rows", "gif": "https://media3.giphy.com/media/v1.Y2lkPTc5MGI3NjExNGo1c2dvNGN6bXRibHIyeGg0b3ZiNXd2NDdmcGVjajQ0eGNidWxsdSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/3oEjHM9hzerMdVjYWI/giphy.gif", "detail": "3 sets of 10/side"},
    ],
    "Yoga": [
        {"name": "Downward Dog", "gif": "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExNjY4Y3FjcmhidHEzcGFya3lsdHl2bTB2bW1haWtqdm56ZDhtY29zMCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/MaOiHFdoWaJexgoIew/giphy.gif", "detail": "Hold 60 seconds"},
        {"name": "Warrior II", "gif": "https://media1.giphy.com/media/v1.Y2lkPTc5MGI3NjExYTAzdGlvZ2UybmsybTY0aTJ3ZTA5a3NwZngzenQyZHo0aWQ2djk4NyZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/YuR35DYuKZVjli8nSP/giphy.gif", "detail": "Hold 45 seconds/side"},
    ],
    "HIIT": [
        {"name": "Jumping Jacks", "gif": "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExeG9hcHU5aTQ1ZDJ1ZmUwaWU0Y3YzaDd1eGd5dmJpdGNlMDdxb2VsaiZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/6dUCG26mCktJC/giphy.gif", "detail": "45s work, 15s rest x5"},
        {"name": "Burpees", "gif": "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExeG9hcHU5aTQ1ZDJ1ZmUwaWU0Y3YzaDd1eGd5dmJpdGNlMDdxb2VsaiZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/6dUCG26mCktJC/giphy.gif", "detail": "30s work, 30s rest x6"},
    ],
    "Running": [
        {"name": "Interval Run", "gif": "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExdWwwcHpvbjNteTEzeTcxYW5hbnhqcGM2N3gzdXI2a3dwY2E1a2M1YSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/T6HHgYrBjxmUXhQFdv/giphy.gif", "detail": "6 rounds: 2 min fast / 2 min easy"},
    ],
    "Cycling": [
        {"name": "Steady Ride", "gif": "https://media1.giphy.com/media/v1.Y2lkPTc5MGI3NjExdjhja2h3ODJheDljeHNoNjhrczl3NTd3dnY2Njk3aGg2dWZvMzI0YiZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/kAnpk0N9Ph3he/giphy.gif", "detail": "25–40 minutes"},
    ],
    "Swimming": [
        {"name": "Freestyle Laps", "gif": "https://media4.giphy.com/media/v1.Y2lkPTc5MGI3NjExZ2p2dDZnNGI4Nm5uYzd3ZzE5d21jNHF2cmFkbWdya2t4MHh1d3M3MSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/8FlKgbVKVNKVYOhS5a/giphy.gif", "detail": "20–30 minutes"},
    ],
    "Walking": [
        {"name": "Brisk Walk", "gif": "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExMzVhb3p4YWwyYWVrdmVzdmljNm82NzRjNTR0eGxhd3Btamg1bnAzbyZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/madDsd98Bt7X6fwAJ7/giphy.gif", "detail": "20–40 minutes"},
    ],
    "Stretching": [
        {"name": "Hamstring Stretch", "gif": "https://media4.giphy.com/media/v1.Y2lkPTc5MGI3NjExcGozem1zb3kzN3NjcW40bTh2ZnY1ZWhwZTFjaHNtZDZ2OHI4enM4bSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/26FPHv8kM4donDPYQ/giphy.gif", "detail": "Hold 45 seconds/side"},
    ],
    "Tennis": [
        {"name": "Footwork Drills", "gif": "https://media4.giphy.com/media/v1.Y2lkPTc5MGI3NjExNXB0OG45cDBiYXNnMWFncDZlemgxc3ZyNW5rYTN3enY1MGkyOHo3YyZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/sXGxId3HqR1u9rOAL2/giphy.gif", "detail": "15 minutes"},
    ],
    "Cardio": [
        {"name": "Jump Rope", "gif": "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExeG9hcHU5aTQ1ZDJ1ZmUwaWU0Y3YzaDd1eGd5dmJpdGNlMDdxb2VsaiZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/6dUCG26mCktJC/giphy.gif", "detail": "3 rounds x 5 minutes"},
    ],
}


def generate_weekly_plan(goal: str, condition: str, available_time: int) -> list[dict]:
    """
    Generate a 7-day personalized workout schedule with medical-aware constraints.
    Returns a list of dicts for easy rendering in Django templates or DRF serializers.
    """
    goal_activity_map: dict[str, list[str]] = {
        "Fat Loss": ["HIIT", "Running", "Cycling", "Swimming", "Weight Training"],
        "Muscle Gain": ["Weight Training", "Yoga"],
        "Endurance": ["Running", "Cycling", "Swimming", "Tennis"],
    }
    recovery_activities = ["Walking", "Yoga", "Stretching"]
    active_activities = list(goal_activity_map.get(goal, goal_activity_map["Fat Loss"]))

    if condition == "Hypertension":
        active_activities = [a for a in active_activities if a != "HIIT"] or ["Walking", "Yoga", "Weight Training"]
    if condition == "Asthma":
        priority = ["Swimming", "Yoga", "Weight Training"]
        prioritized = [a for a in priority if a in active_activities]
        others = [a for a in active_activities if a not in priority]
        active_activities = (prioritized + others) if prioritized else (priority + others)

    intensity_map = {
        "HIIT": "High", "Running": "Medium", "Cycling": "Medium",
        "Swimming": "Medium", "Tennis": "Medium", "Weight Training": "Medium",
        "Yoga": "Low", "Walking": "Low", "Stretching": "Low", "Cardio": "Medium",
    }

    safe_time = max(15, int(available_time))
    recovery_duration = max(10, int(round(safe_time / 2)))
    recovery_days = {2, 5}
    schedule = []
    active_idx = recovery_idx = 0

    for i in range(7):
        if i in recovery_days:
            activity = recovery_activities[recovery_idx % len(recovery_activities)]
            recovery_idx += 1
            intensity = "Low"
            duration = recovery_duration
        else:
            activity = active_activities[active_idx % len(active_activities)]
            active_idx += 1
            intensity = intensity_map.get(activity, "Medium")
            duration = safe_time

        if condition == "Hypertension" and intensity == "High":
            intensity = "Medium"
        if condition == "Asthma" and activity == "Running":
            intensity = "Low"

        exercises = EXERCISE_DB.get(activity, EXERCISE_DB.get("Stretching", []))
        schedule.append({
            "day": f"Day {i + 1}",
            "activity": activity,
            "intensity": intensity,
            "duration": int(duration),
            "exercises": exercises,
        })

    return schedule
