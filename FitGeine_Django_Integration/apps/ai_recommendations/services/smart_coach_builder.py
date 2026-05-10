"""
Smart Coach Builder Service
----------------------------
Generates the daily coaching plan (workout, nutrition, recovery, safety alerts).
Ported from build_smart_coach_plan() in app.py.
Pure Python — no Django, no Streamlit.
"""
from __future__ import annotations

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


def build_smart_coach_plan(
    goal: str,
    condition: str,
    available_time: int,
    bmr: float,
    weight_kg: float,
) -> dict:
    """
    Build a daily coaching plan tailored to goal, medical condition, and time available.

    Returns a dict with keys: calorie_target, step_target, workout, nutrition, recovery, safety.
    """
    calorie_target = bmr + GOAL_CALORIE_DELTAS.get(goal, 0)
    step_target = GOAL_STEP_TARGETS.get(goal, 8000)

    t = max(15, int(available_time))
    warmup = max(5, int(round(t * 0.15)))
    cooldown = max(5, int(round(t * 0.10)))
    main = max(10, t - warmup - cooldown)

    plan: dict = {
        "calorie_target": round(calorie_target, 0),
        "step_target": step_target,
        "workout": [],
        "nutrition": [],
        "recovery": [],
        "safety": [],
    }

    if goal == "Fat Loss":
        plan["workout"] = [
            f"Daily {t}-min session: {warmup} min warm-up, {main} min moderate cardio + strength, {cooldown} min cool-down.",
            f"Target {step_target:,} steps/day.",
        ]
        plan["nutrition"] = [
            f"Calorie target: ~{calorie_target:.0f} kcal/day (BMR − 500).",
            "Prioritize lean protein (chicken, fish, legumes) and high-fiber vegetables.",
            "Avoid liquid calories; drink 2–3 L water/day.",
        ]
    elif goal == "Muscle Gain":
        plan["workout"] = [
            f"Daily {t}-min session: {warmup} min mobility, {main} min progressive strength, {cooldown} min stretching.",
            f"Target {step_target:,} steps/day for active recovery.",
        ]
        plan["nutrition"] = [
            f"Calorie target: ~{calorie_target:.0f} kcal/day (BMR + 300).",
            "Spread protein (1.6–2.2 g/kg bodyweight) across 4–5 meals.",
            "Eat complex carbs 1–2 hours before lifting.",
        ]
    else:  # Endurance
        plan["workout"] = [
            f"Daily {t}-min session: {warmup} min warm-up, {main} min endurance work, {cooldown} min recovery.",
            f"Target {step_target:,}+ steps/day.",
        ]
        plan["nutrition"] = [
            f"Calorie target: ~{calorie_target:.0f} kcal/day (BMR + 100).",
            "Fuel with complex carbs 2 hours before long sessions.",
            "Replenish electrolytes post-workout.",
        ]

    hydration = round(max(2.0, float(weight_kg) * 0.035), 1)
    plan["recovery"] = [
        "Sleep target: 7–9 hours/night.",
        f"Hydration target: {hydration} liters/day.",
        "Include 1–2 full rest or active-recovery days per week.",
    ]

    if condition == "Hypertension":
        plan["safety"].append("⚠️ Avoid high-intensity workouts. Keep heart rate in moderate zone.")
    if condition == "Asthma":
        plan["safety"].append("⚠️ Prefer indoor Weight Training or Yoga when symptoms flare. Carry inhaler.")
    if condition == "Diabetes":
        plan["safety"].append("⚠️ Monitor blood glucose around workouts. Pair carbs with protein post-session.")

    return plan
