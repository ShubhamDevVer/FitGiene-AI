from django.urls import path
from . import views

urlpatterns = [
    path("predict/",         views.predictive_engine,   name="predictive_engine"),
    path("coach/",           views.smart_coach,          name="smart_coach"),
    path("coach/generate/",  views.smart_coach_generate, name="smart_coach_generate"),
]
