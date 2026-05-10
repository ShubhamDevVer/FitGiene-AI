from django import forms
from django.contrib.auth import get_user_model

User = get_user_model()


class LoginForm(forms.Form):
    identifier = forms.CharField(
        label="Username or Email",
        max_length=254,
        widget=forms.TextInput(attrs={"placeholder": "Username or Email", "autofocus": True}),
    )
    password = forms.CharField(
        widget=forms.PasswordInput(attrs={"placeholder": "Password"})
    )


class SignUpForm(forms.ModelForm):
    password = forms.CharField(
        widget=forms.PasswordInput(attrs={"placeholder": "Password"}),
        min_length=8,
    )
    confirm_password = forms.CharField(
        widget=forms.PasswordInput(attrs={"placeholder": "Confirm Password"}),
        label="Confirm Password",
    )

    class Meta:
        model = User
        fields = ["email", "username", "password"]
        widgets = {
            "email": forms.EmailInput(attrs={"placeholder": "Email"}),
            "username": forms.TextInput(attrs={"placeholder": "Username"}),
        }

    def clean(self):
        cleaned = super().clean()
        if cleaned.get("password") != cleaned.get("confirm_password"):
            raise forms.ValidationError("Passwords do not match.")
        return cleaned

    def save(self, commit=True):
        user = super().save(commit=False)
        user.set_password(self.cleaned_data["password"])
        if commit:
            user.save()
        return user


class ProfileSetupForm(forms.ModelForm):
    class Meta:
        model = User
        fields = [
            "age", "gender", "height_cm", "weight_kg",
            "goal", "medical_condition", "available_time",
        ]
        widgets = {
            "age": forms.NumberInput(attrs={"min": 15, "max": 80}),
            "height_cm": forms.NumberInput(attrs={"min": 120, "max": 230, "step": "0.5"}),
            "weight_kg": forms.NumberInput(attrs={"min": 30, "max": 250, "step": "0.1"}),
            "available_time": forms.NumberInput(attrs={"min": 15, "max": 180, "step": 5}),
        }
