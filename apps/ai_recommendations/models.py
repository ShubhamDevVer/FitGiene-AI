from django.db import models


# ai_recommendations has no DB models of its own.
# All data comes from users.FitUser and fitness_tracking.DailyLog via the service layer.
# Future: Add WorkoutPlan or CoachingSession models here if persistence is needed.
