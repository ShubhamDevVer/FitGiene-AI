"""
Custom Authentication Backend
Allows users to log in with either their username OR email address.
Registered in settings.py via AUTHENTICATION_BACKENDS.
"""
from django.contrib.auth import get_user_model
from django.contrib.auth.backends import ModelBackend

User = get_user_model()


class UsernameOrEmailBackend(ModelBackend):
    """
    Authenticates against username OR email.
    Replaces the original authenticate_user() raw SQL function from app.py.
    """

    def authenticate(self, request, username=None, password=None, **kwargs):
        if not username or not password:
            return None

        # Try username first, then email
        try:
            user = User.objects.get(username=username)
        except User.DoesNotExist:
            try:
                user = User.objects.get(email=username)
            except User.DoesNotExist:
                # Run the hasher anyway to prevent timing attacks
                User().set_password(password)
                return None

        if user.check_password(password) and self.user_can_authenticate(user):
            return user
        return None
