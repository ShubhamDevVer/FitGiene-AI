from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from .models import FitUser


@admin.register(FitUser)
class FitUserAdmin(UserAdmin):
    list_display = ("username", "email", "goal", "medical_condition", "date_joined", "is_staff")
    list_filter = ("goal", "medical_condition", "gender", "is_staff")
    fieldsets = UserAdmin.fieldsets + (
        ("Fitness Profile", {
            "fields": ("age", "gender", "height_cm", "weight_kg", "goal", "medical_condition", "available_time")
        }),
    )
    add_fieldsets = UserAdmin.add_fieldsets + (
        ("Fitness Profile", {
            "fields": ("email", "age", "gender", "height_cm", "weight_kg", "goal", "medical_condition")
        }),
    )
