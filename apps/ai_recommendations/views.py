"""
ai_recommendations/views.py
────────────────────────────
Three views:
  predictive_engine  — ML persona prediction (K-Means)
  smart_coach        — shell page (renders immediately with skeleton)
  smart_coach_generate — AJAX JSON endpoint that calls OpenRouter
"""
import json
from pathlib import Path

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_http_methods

from fitness_tracking.models import DailySummary
from .services.persona_engine import predict_persona
from .services.weekly_planner import generate_weekly_plan
from .services.openrouter_coach import get_coaching_plan, CoachServiceError


# ── Shared helper ─────────────────────────────────────────────────────────────

def _get_behavior_baseline(user) -> dict:
    """
    Returns median steps/sleep/stress from the last 30 DailySummary rows.
    """
    summaries = DailySummary.objects.filter(user=user).order_by("-date")[:30]
    if not summaries:
        return {"daily_steps": 7000.0, "hours_sleep": 7.0, "stress_level": 5.0}

    steps_list  = [s.total_steps  for s in summaries if s.total_steps]
    sleep_list  = [s.hours_sleep  for s in summaries if s.hours_sleep]
    stress_list = [s.stress_level for s in summaries if s.stress_level]

    def median(lst, fallback):
        if not lst:
            return fallback
        s = sorted(lst)
        mid = len(s) // 2
        return s[mid] if len(s) % 2 else (s[mid - 1] + s[mid]) / 2

    return {
        "daily_steps": float(median(steps_list, 7000)),
        "hours_sleep": float(median(sleep_list, 7.0)),
        "stress_level": float(median(stress_list, 5.0)),
    }


def _bmi_category(bmi: float) -> str:
    if bmi <= 0:    return "Unknown"
    if bmi < 18.5:  return "Underweight"
    if bmi < 25:    return "Normal"
    if bmi < 30:    return "Overweight"
    return "Obese"


# ── Predictive Engine ─────────────────────────────────────────────────────────

@login_required
@require_http_methods(["GET"])
def predictive_engine(request):
    """Replaces render_predictive_engine_page()."""
    user     = request.user
    baseline = _get_behavior_baseline(user)

    try:
        result = predict_persona(
            model_path=Path(settings.ML_MODEL_PATH),
            age=float(user.age or 30),
            bmi=float(user.bmi or 22.0),
            daily_steps=baseline["daily_steps"],
            hours_sleep=baseline["hours_sleep"],
            stress_level=baseline["stress_level"],
        )
        weekly_plan = generate_weekly_plan(
            goal=user.goal or "Fat Loss",
            condition=user.medical_condition or "None",
            available_time=int(user.available_time or 45),
        )
        error = None

        # Normalise metrics to 0–100 for radar chart (done in view, not template)
        def _normalise(key, val):
            scales = {
                "Age":          (15, 70,    False),
                "BMI":          (15, 40,    True),
                "Daily Steps":  (0,  15000, False),
                "Hours Sleep":  (0,  12,    False),
                "Stress Level": (0,  10,    True),
            }
            lo, hi, invert = scales.get(key, (0, 100, False))
            norm = (val - lo) / (hi - lo) * 100 if hi != lo else 50
            norm = max(0, min(100, norm))
            return round(100 - norm if invert else norm, 1)

        radar_json = json.dumps({
            "labels":  list(result["user_metrics"].keys()),
            "you":     [_normalise(k, v) for k, v in result["user_metrics"].items()],
            "avg":     [_normalise(k, v) for k, v in result["persona_averages"].items()],
        })

    except Exception as e:
        result      = None
        weekly_plan = []
        error       = str(e)
        radar_json  = json.dumps({"labels": [], "you": [], "avg": []})

    return render(request, "ai_recommendations/predictive_engine.html", {
        "result":      result,
        "weekly_plan": weekly_plan,
        "error":       error,
        "baseline":    baseline,
        "radar_json":  radar_json,
    })


# ── Smart Coach (Shell page) ──────────────────────────────────────────────────

@login_required
@require_http_methods(["GET"])
def smart_coach(request):
    """
    Renders the Smart Coach page shell immediately.
    The page loads with skeleton cards; JS calls /ai/coach/generate/ via fetch().
    """
    user     = request.user
    baseline = _get_behavior_baseline(user)

    # Try to resolve the persona name for context display; fall back gracefully
    try:
        result = predict_persona(
            model_path=Path(settings.ML_MODEL_PATH),
            age=float(user.age or 30),
            bmi=float(user.bmi or 22.0),
            daily_steps=baseline["daily_steps"],
            hours_sleep=baseline["hours_sleep"],
            stress_level=baseline["stress_level"],
        )
        persona_name = result.get("persona_name", "Balanced Builder")
    except Exception:
        persona_name = "Balanced Builder"

    return render(request, "ai_recommendations/smart_coach.html", {
        "user":         user,
        "persona_name": persona_name,
        "baseline":     baseline,
    })


# ── Smart Coach Generate (AJAX endpoint) ─────────────────────────────────────

@login_required
@require_http_methods(["GET"])
def smart_coach_generate(request):
    """
    AJAX endpoint called by the Smart Coach page JS.
    Builds the user profile dict, calls OpenRouter, returns JSON.

    Response shapes:
      200 { "status": "ok",    "plan": { ... } }
      500 { "status": "error", "message": "..." }
    """
    user     = request.user
    baseline = _get_behavior_baseline(user)

    # Resolve persona
    try:
        result       = predict_persona(
            model_path=Path(settings.ML_MODEL_PATH),
            age=float(user.age or 30),
            bmi=float(user.bmi or 22.0),
            daily_steps=baseline["daily_steps"],
            hours_sleep=baseline["hours_sleep"],
            stress_level=baseline["stress_level"],
        )
        persona_name = result.get("persona_name", "Balanced Builder")
    except Exception:
        persona_name = "Balanced Builder"

    # Build profile dict for the prompt
    profile = {
        "age":               user.age or 30,
        "gender":            user.gender or "Male",
        "weight_kg":         user.weight_kg or 70,
        "height_cm":         user.height_cm or 170,
        "bmi":               round(float(user.bmi or 22.0), 2),
        "bmi_category":      _bmi_category(float(user.bmi or 22.0)),
        "bmr":               round(float(user.bmr or 1800), 0),
        "goal":              user.goal or "Fat Loss",
        "medical_condition": user.medical_condition or "None",
        "available_time":    user.available_time or 45,
        "persona":           persona_name,
        "daily_steps":       baseline["daily_steps"],
        "hours_sleep":       baseline["hours_sleep"],
        "stress_level":      baseline["stress_level"],
    }

    try:
        plan = get_coaching_plan(
            profile  = profile,
            api_key  = settings.OPENROUTER_API_KEY,
            base_url = settings.OPENROUTER_BASE_URL,
            model    = settings.OPENROUTER_MODEL,
        )
        return JsonResponse({"status": "ok", "plan": plan})

    except CoachServiceError as exc:
        return JsonResponse({"status": "error", "message": str(exc)}, status=500)

    except Exception as exc:
        return JsonResponse(
            {"status": "error", "message": f"Unexpected error: {exc}"},
            status=500,
        )
