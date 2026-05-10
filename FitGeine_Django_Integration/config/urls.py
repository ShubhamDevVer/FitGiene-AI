from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path("admin/", admin.site.urls),
    path("users/", include("users.urls")),
    path("dashboard/", include("fitness_tracking.urls")),
    path("ai/", include("ai_recommendations.urls")),
    path("", include("fitness_tracking.urls")),  # root redirects to dashboard
]
