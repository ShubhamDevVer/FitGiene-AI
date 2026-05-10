"""
fitness_tracking/forms.py
─────────────────────────
Two separate forms matching the two-table architecture:

  DailySummaryForm  — date, steps, sleep, stress  (upserted once per day)
  ActivityLogForm   — activity type + duration     (appended each session)

Task 3 fix: DateInput widget with type="date" → native browser calendar picker.
"""
from django import forms
from .models import ActivityLog, DailySummary


class DailySummaryForm(forms.ModelForm):
    """
    Captures whole-day health metrics.
    update_or_create keyed on (user, date) in the view — safe to submit multiple times.
    """
    class Meta:
        model  = DailySummary
        fields = ["date", "total_steps", "hours_sleep", "stress_level"]
        widgets = {
            # Task 3: explicit DateInput type="date" → renders a native calendar
            "date": forms.DateInput(
                attrs={"type": "date"},
                format="%Y-%m-%d",
            ),
            "total_steps": forms.NumberInput(attrs={"min": 0, "max": 100_000, "step": 100, "placeholder": "e.g. 8000"}),
            "hours_sleep": forms.NumberInput(attrs={"min": 0, "max": 24,      "step": "0.5", "placeholder": "e.g. 7.5"}),
            "stress_level": forms.NumberInput(attrs={"min": 1, "max": 10,     "step": 1}),
        }
        labels = {
            "date":         "Date",
            "total_steps":  "Steps Taken",
            "hours_sleep":  "Sleep (hours)",
            "stress_level": "Stress Level (1–10)",
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Ensure the date widget renders ISO format (required by type="date")
        self.fields["date"].input_formats = ["%Y-%m-%d"]


class ActivityLogForm(forms.ModelForm):
    """
    Captures a single exercise session to be appended to the day's ActivityLogs.
    calories_burned is calculated in the view — not shown to the user.
    """
    class Meta:
        model  = ActivityLog
        fields = ["activity_type", "duration_minutes"]
        widgets = {
            "duration_minutes": forms.NumberInput(attrs={"min": 5, "max": 600, "step": 5, "placeholder": "e.g. 45"}),
        }
        labels = {
            "activity_type":    "Activity",
            "duration_minutes": "Duration (minutes)",
        }
