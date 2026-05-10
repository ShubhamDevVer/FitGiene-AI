"""
fitness_tracking/models.py
──────────────────────────
Two-table architecture replacing the monolithic DailyLog:

  DailySummary  — one row per (user, date): steps, sleep, stress.
  ActivityLog   — many rows per DailySummary: one per exercise session.

Why: logging multiple activities per day no longer overwrites steps/sleep, and
Plotly always gets exactly one calories-sum per date from the DB aggregation.
"""
from django.conf import settings
from django.db import models


ACTIVITY_CHOICES = [
    ("Walking",         "Walking"),
    ("Running",         "Running"),
    ("Cycling",         "Cycling"),
    ("Weight Training", "Weight Training"),
    ("Yoga",            "Yoga"),
    ("Swimming",        "Swimming"),
    ("Cardio",          "Cardio"),
    ("HIIT",            "HIIT"),
    ("Stretching",      "Stretching"),
    ("Tennis",          "Tennis"),
]


class DailySummary(models.Model):
    """
    Daily health snapshot — unique per (user, date).
    Updating this row is safe: steps/sleep/stress are whole-day metrics.
    """
    user        = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="daily_summaries",
    )
    date        = models.DateField()
    total_steps = models.PositiveIntegerField(default=0)
    hours_sleep = models.FloatField(default=7.0)
    stress_level = models.PositiveSmallIntegerField(default=5)   # 1–10
    created_at  = models.DateTimeField(auto_now_add=True)
    updated_at  = models.DateTimeField(auto_now=True)

    class Meta:
        db_table        = "fit_daily_summaries"
        unique_together = ("user", "date")           # prevents duplicate-date crash
        ordering        = ["-date"]
        indexes         = [
            models.Index(fields=["user", "date"], name="summary_user_date_idx"),
        ]

    def __str__(self):
        return f"{self.user.username} | {self.date} | {self.total_steps} steps"


class ActivityLog(models.Model):
    """
    Individual exercise session — many per DailySummary.
    Calories are stored per-session so the view can Sum() them per date.
    """
    daily_summary    = models.ForeignKey(
        DailySummary,
        on_delete=models.CASCADE,
        related_name="activities",
    )
    activity_type    = models.CharField(max_length=50, choices=ACTIVITY_CHOICES)
    duration_minutes = models.FloatField(default=30.0)
    calories_burned  = models.FloatField(default=0.0)
    created_at       = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "fit_activity_logs"
        ordering = ["-created_at"]

    def __str__(self):
        return (
            f"{self.daily_summary.user.username} | "
            f"{self.daily_summary.date} | "
            f"{self.activity_type} | "
            f"{self.calories_burned:.0f} kcal"
        )
