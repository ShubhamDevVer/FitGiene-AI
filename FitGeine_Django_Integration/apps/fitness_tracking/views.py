"""
fitness_tracking/views.py
─────────────────────────
Refactored dashboard view using the two-table architecture.

Key fixes:
  Task 1 — update_or_create on DailySummary prevents overwrite bug;
            ActivityLog is always appended (no deletion of old sessions).
  Task 1 — Chart JSON is aggregated at the DB level (Sum per date) so
            Plotly always receives exactly one point per date.
  Task 2 — has_data flag drives the empty-state UI in the template.
  Task 3 — Forms now use DateInput(type="date") — handled in forms.py.
"""
import json

from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.db.models import Sum, Count, Avg
from django.shortcuts import redirect, render
from django.utils import timezone
from django.views.decorators.http import require_http_methods

from .forms import DailySummaryForm, ActivityLogForm
from .models import ActivityLog, DailySummary
from .services.calorie_calculator import calculate_session_calories


@login_required
@require_http_methods(["GET", "POST"])
def dashboard(request):
    user = request.user

    if request.method == "POST":
        summary_form  = DailySummaryForm(request.POST)
        activity_form = ActivityLogForm(request.POST)

        if summary_form.is_valid() and activity_form.is_valid():
            chosen_date = summary_form.cleaned_data["date"]

            # ── Task 1: upsert the daily summary row (safe for multiple sessions) ──
            summary, _ = DailySummary.objects.update_or_create(
                user=user,
                date=chosen_date,
                defaults={
                    "total_steps": summary_form.cleaned_data["total_steps"],
                    "hours_sleep": summary_form.cleaned_data["hours_sleep"],
                    "stress_level": summary_form.cleaned_data["stress_level"],
                },
            )

            # ── Always append a new ActivityLog row ──────────────────────────────
            activity = activity_form.save(commit=False)
            activity.daily_summary  = summary
            activity.calories_burned = calculate_session_calories(
                activity_type=activity.activity_type,
                duration_minutes=activity.duration_minutes,
                weight_kg=float(user.weight_kg or 70.0),
                bmi=float(user.bmi or 22.0),
            )
            activity.save()

            messages.success(
                request,
                f"✓ {activity.activity_type} ({activity.duration_minutes:.0f} min) "
                f"logged for {chosen_date}."
            )
            return redirect("dashboard")

        else:
            messages.error(request, "Please correct the errors below.")

    else:
        today = timezone.localdate()
        summary_form  = DailySummaryForm(initial={"date": today})
        activity_form = ActivityLogForm()

    # ── Aggregate KPI cards ───────────────────────────────────────────────────
    agg = ActivityLog.objects.filter(daily_summary__user=user).aggregate(
        total_calories=Sum("calories_burned"),
        total_sessions=Count("id"),
    )
    sleep_agg = DailySummary.objects.filter(user=user).aggregate(
        avg_sleep=Avg("hours_sleep"),
    )
    total_calories = round(float(agg["total_calories"] or 0), 1)
    total_sessions = agg["total_sessions"] or 0
    avg_sleep      = round(float(sleep_agg["avg_sleep"] or 0), 1)

    goal_steps_val = {"Fat Loss": 10000, "Muscle Gain": 8000, "Endurance": 12000}.get(
        user.goal, 8000
    )

    # ── Build Plotly JSON — exactly ONE point per date (Task 1 core fix) ─────
    # Annotate each DailySummary with the sum of its child ActivityLogs' calories.
    chart_qs = (
        DailySummary.objects
        .filter(user=user)
        .annotate(day_calories=Sum("activities__calories_burned"))
        .order_by("date")
        .values("date", "total_steps", "day_calories")[:90]
    )

    logs_json = json.dumps({
        "dates":    [str(r["date"]) for r in chart_qs],
        "steps":    [int(r["total_steps"] or 0) for r in chart_qs],
        "calories": [round(float(r["day_calories"] or 0), 1) for r in chart_qs],
        "goal":     [goal_steps_val] * len(list(chart_qs)),
    })

    # ── Recent activity rows for the log table ────────────────────────────────
    recent_summaries = (
        DailySummary.objects
        .filter(user=user)
        .prefetch_related("activities")
        .order_by("-date")[:14]
    )

    context = {
        "summary_form":    summary_form,
        "activity_form":   activity_form,
        "recent_summaries": recent_summaries,
        "total_calories":  total_calories,
        "total_sessions":  total_sessions,
        "avg_sleep":       avg_sleep,
        "goal_steps":      goal_steps_val,
        "logs_json":       logs_json,
        "has_data":        total_sessions > 0,
        "today":           timezone.localdate(),
    }
    return render(request, "fitness_tracking/dashboard.html", context)
