from django.contrib.auth.models import AbstractBaseUser, BaseUserManager, PermissionsMixin
from django.db import models


class FitUserManager(BaseUserManager):
    """Custom manager that uses email + username instead of Django's default username-only."""

    def create_user(self, email, username, password=None, **extra_fields):
        if not email:
            raise ValueError("An email address is required.")
        if not username:
            raise ValueError("A username is required.")
        email = self.normalize_email(email)
        user = self.model(email=email, username=username, **extra_fields)
        user.set_password(password)  # Django handles PBKDF2-SHA256 — no manual hashing needed
        user.save(using=self._db)
        return user

    def create_superuser(self, email, username, password=None, **extra_fields):
        extra_fields.setdefault("is_staff", True)
        extra_fields.setdefault("is_superuser", True)
        return self.create_user(email, username, password, **extra_fields)


class FitUser(AbstractBaseUser, PermissionsMixin):
    """
    Replaces the raw `users` MySQL table from the Streamlit app.
    Django auto-handles PBKDF2 password hashing via set_password() / check_password().
    """

    GOAL_CHOICES = [
        ("Fat Loss", "Fat Loss"),
        ("Muscle Gain", "Muscle Gain"),
        ("Endurance", "Endurance"),
    ]
    CONDITION_CHOICES = [
        ("None", "None"),
        ("Diabetes", "Diabetes"),
        ("Hypertension", "Hypertension"),
        ("Asthma", "Asthma"),
    ]
    GENDER_CHOICES = [
        ("Male", "Male"),
        ("Female", "Female"),
        ("Other", "Other"),
    ]

    # --- Auth fields ---
    email = models.EmailField(unique=True)
    username = models.CharField(max_length=150, unique=True)

    # --- Profile fields (filled in on Profile Setup page) ---
    age = models.PositiveSmallIntegerField(null=True, blank=True)
    gender = models.CharField(max_length=10, choices=GENDER_CHOICES, default="Male")
    height_cm = models.FloatField(null=True, blank=True)
    weight_kg = models.FloatField(null=True, blank=True)
    goal = models.CharField(max_length=20, choices=GOAL_CHOICES, default="Fat Loss")
    medical_condition = models.CharField(max_length=20, choices=CONDITION_CHOICES, default="None")
    available_time = models.PositiveSmallIntegerField(default=45)  # minutes per day

    # --- Django internals ---
    is_active = models.BooleanField(default=True)
    is_staff = models.BooleanField(default=False)
    date_joined = models.DateTimeField(auto_now_add=True)

    USERNAME_FIELD = "username"
    REQUIRED_FIELDS = ["email"]

    objects = FitUserManager()

    class Meta:
        db_table = "fit_users"   # Renamed to avoid PK conflict with legacy 'users' table (user_id vs id)
        verbose_name = "Fit User"
        verbose_name_plural = "Fit Users"

    def __str__(self):
        return f"{self.username} ({self.email})"

    # --- Computed properties (replaces calculate_bmi, calculate_bmr, estimate_body_fat_percent) ---
    @property
    def bmi(self) -> float:
        if not self.height_cm or not self.weight_kg:
            return 0.0
        h = max(float(self.height_cm), 1.0) / 100.0
        return round(float(self.weight_kg) / (h ** 2), 2)

    @property
    def bmr(self) -> float:
        if not all([self.age, self.height_cm, self.weight_kg]):
            return 0.0
        adj = 5 if str(self.gender).lower().startswith("m") else -161
        return round(
            (10 * float(self.weight_kg))
            + (6.25 * float(self.height_cm))
            - (5 * int(self.age))
            + adj,
            2,
        )

    @property
    def body_fat_percent(self) -> float:
        bmi = self.bmi
        if not bmi or not self.age:
            return 0.0
        is_male = 1 if str(self.gender).lower().startswith("m") else 0
        value = (1.2 * bmi) + (0.23 * int(self.age)) - (10.8 * is_male) - 5.4
        return round(max(5.0, min(value, 60.0)), 2)
