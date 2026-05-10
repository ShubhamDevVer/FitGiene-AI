from django.contrib import admin
from .models import ActivityLog, DailySummary


class ActivityLogInline(admin.TabularInline):
    model  = ActivityLog
    extra  = 0
    fields = ("activity_type", "duration_minutes", "calories_burned")
    readonly_fields = ("calories_burned",)


@admin.register(DailySummary)
class DailySummaryAdmin(admin.ModelAdmin):
    list_display  = ("user", "date", "total_steps", "hours_sleep", "stress_level")
    list_filter   = ("date",)
    search_fields = ("user__username",)
    date_hierarchy = "date"
    ordering      = ("-date",)
    inlines       = [ActivityLogInline]


@admin.register(ActivityLog)
class ActivityLogAdmin(admin.ModelAdmin):
    list_display  = ("__str__", "activity_type", "duration_minutes", "calories_burned")
    list_filter   = ("activity_type",)
    search_fields = ("daily_summary__user__username", "activity_type")
    ordering      = ("-created_at",)
