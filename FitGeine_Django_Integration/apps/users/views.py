from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.shortcuts import redirect, render
from django.views.decorators.http import require_http_methods

from .forms import LoginForm, SignUpForm, ProfileSetupForm


@require_http_methods(["GET", "POST"])
def login_view(request):
    """Replaces render_auth_page() login tab."""
    if request.user.is_authenticated:
        return redirect("dashboard")

    form = LoginForm(request.POST or None)
    if request.method == "POST" and form.is_valid():
        identifier = form.cleaned_data["identifier"]
        password = form.cleaned_data["password"]
        user = authenticate(request, username=identifier, password=password)
        if user:
            login(request, user)
            return redirect(request.GET.get("next", "dashboard"))
        else:
            messages.error(request, "Invalid username/email or password. Please try again.")

    return render(request, "users/login.html", {"form": form})


@require_http_methods(["GET", "POST"])
def signup_view(request):
    """Replaces render_auth_page() signup tab."""
    if request.user.is_authenticated:
        return redirect("dashboard")

    form = SignUpForm(request.POST or None)
    if request.method == "POST" and form.is_valid():
        user = form.save()
        login(request, user)
        messages.success(request, f"Welcome to FitGenie AI, {user.username}! Set up your profile to get started.")
        return redirect("profile_setup")

    return render(request, "users/signup.html", {"form": form})


@login_required
@require_http_methods(["GET", "POST"])
def profile_setup_view(request):
    """Replaces render_profile_setup_page()."""
    form = ProfileSetupForm(request.POST or None, instance=request.user)
    if request.method == "POST" and form.is_valid():
        form.save()
        messages.success(request, "Profile updated successfully.")
        return redirect("profile_setup")

    user = request.user
    context = {
        "form": form,
        "bmi": user.bmi,
        "bmr": user.bmr,
        "body_fat": user.body_fat_percent,
    }
    return render(request, "users/profile_setup.html", context)


def logout_view(request):
    logout(request)
    return redirect("login")
