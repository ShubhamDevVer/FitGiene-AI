from __future__ import annotations

from pathlib import Path
from typing import Any
import hashlib
import hmac
import secrets

import joblib
import mysql.connector
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from mysql.connector import Error, IntegrityError

# Streamlit layout config must be the first Streamlit command.
st.set_page_config(
    page_title="AI-Powered Fitness & Lifestyle Coach",
    layout="wide",
    initial_sidebar_state="expanded",
)

NAV_PAGES = ["Profile Setup", "Predictive Engine", "Daily Dashboard", "Smart Coach"]
PRIMARY_GOALS = ["Fat Loss", "Muscle Gain", "Endurance"]
MEDICAL_CONDITIONS = ["None", "Diabetes", "Hypertension", "Asthma"]
GOAL_STEP_TARGETS = {"Fat Loss": 10000, "Muscle Gain": 8000, "Endurance": 12000}
GOAL_CALORIE_DELTAS = {"Fat Loss": -500, "Muscle Gain": 300, "Endurance": 100}
PASSWORD_HASH_ITERATIONS = 260000
EXERCISE_DB: dict[str, list[dict[str, str]]] = {
    "Weight Training": [
        {"name": "Push-ups", "gif": "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExajFweTExenlwMG1kNHpvOTZvc3U0YnBnZGY0ZDN2ZjdpaTM0aTdxcSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/3pY8FQP9uMtDKXkYqX/giphy.gif", "reps": "3 sets of 12"},
        {"name": "Bodyweight Squats", "gif": "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExaDh1YmN4OG0ybTluYXB5YzM5ZWJjZXBiNGNncHBndHNjMGJwdHJvMCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/1C1ipHPEs4Vjwglwza/giphy.gif", "reps": "3 sets of 15"},
        {"name": "Dumbbell Rows", "gif": "https://media3.giphy.com/media/v1.Y2lkPTc5MGI3NjExNGo1c2dvNGN6bXRibHIyeGg0b3ZiNXd2NDdmcGVjajQ0eGNidWxsdSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/3oEjHM9hzerMdVjYWI/giphy.gif", "reps": "3 sets of 10/side"},
    ],
    "Yoga": [
        {"name": "Downward Dog", "gif": "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExNjY4Y3FjcmhidHEzcGFya3lsdHl2bTB2bW1haWtqdm56ZDhtY29zMCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/MaOiHFdoWaJexgoIew/giphy.gif", "duration": "Hold 60 seconds"},
        {"name": "Warrior II", "gif": "https://media1.giphy.com/media/v1.Y2lkPTc5MGI3NjExYTAzdGlvZ2UybmsybTY0aTJ3ZTA5a3NwZngzenQyZHo0aWQ2djk4NyZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/YuR35DYuKZVjli8nSP/giphy.gif", "duration": "Hold 45 seconds/side"},
        {"name": "Child's Pose", "gif": "https://tenor.com/wehx.gif", "duration": "Hold 90 seconds"},
    ],
    "HIIT": [
        {"name": "Jumping Jacks", "gif": "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExeG9hcHU5aTQ1ZDJ1ZmUwaWU0Y3YzaDd1eGd5dmJpdGNlMDdxb2VsaiZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/6dUCG26mCktJC/giphy.gif", "duration": "45s work, 15s rest x 5"},
        {"name": "Burpees", "gif": "https://tenor.com/bimbM.gif", "duration": "30s work, 30s rest x 6"},
        {"name": "Mountain Climbers", "gif": "https://tenor.com/bPuUR.gif", "duration": "40s work, 20s rest x 5"},
    ],
    "Stretching": [
        {"name": "Hamstring Stretch", "gif": "https://media4.giphy.com/media/v1.Y2lkPTc5MGI3NjExcGozem1zb3kzN3NjcW40bTh2ZnY1ZWhwZTFjaHNtZDZ2OHI4enM4bSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/26FPHv8kM4donDPYQ/giphy.gif", "duration": "Hold 45 seconds/side"},
        {"name": "Hip Flexor Stretch", "gif": "https://media1.giphy.com/media/v1.Y2lkPTc5MGI3NjExcmV0Nnp5bWk3OXRkZWMyangzNHA1NTlwb2Zsa3c4ZXVsMHp1d2FieCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/629rg7lnzlkdWP1tah/giphy.gif", "duration": "Hold 45 seconds/side"},
        {"name": "Chest Opener Stretch", "gif": "https://media4.giphy.com/media/v1.Y2lkPTc5MGI3NjExN25xbnhmb3o2ZWd4dGltY3V2YjFkanp1M3JuaDcwcDAwaXExajhtayZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/Y0sGNBbnI3CY1B9opW/giphy.gif", "duration": "Hold 60 seconds"},
    ],
    "Running": [
        {"name": "Warm-up Jog", "gif": "https://media1.giphy.com/media/v1.Y2lkPTc5MGI3NjExd3hsMmJkNXcwaDE1ZTRoaHVleDNiODh6YmIyMDltY3kzbHc1anY5aCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/bVI9cEHE3hAwix9JdA/giphy.gif", "duration": "10 minutes"},
        {"name": "Interval Run", "gif": "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExdWwwcHpvbjNteTEzeTcxYW5hbnhqcGM2N3gzdXI2a3dwY2E5a2M5YSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/T6HHgYrBjxmUXhQFdv/giphy.gif", "duration": "6 rounds: 2 min fast / 2 min easy"},
    ],
    "Cycling": [
        {"name": "Steady Ride", "gif": "https://media1.giphy.com/media/v1.Y2lkPTc5MGI3NjExdjhja2h3ODJheDljeHNoNjhrczl3NTd3dnY2Njk3aGg2dWZvMzI0YiZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/kAnpk0N9Ph3he/giphy.gif", "duration": "25-40 minutes"},
        {"name": "Hill Intervals", "gif": "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExcG40N2J3b3h2N3VldzQxNW1mOHRrbW94a2thbm45dXZuMTlrbHh0YSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/TKM32j0A08LTtUstf0/giphy.gif", "duration": "6 rounds x 2 minutes"},
    ],
    "Swimming": [
        {"name": "Freestyle Laps", "gif": "https://media4.giphy.com/media/v1.Y2lkPTc5MGI3NjExZ2p2dDZnNGI4Nm5uYzd3ZzE5d21jNHF2cmFkbWdya2t4MHh1d3M3MSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/8FlKgbVKVNKVYOhS5a/giphy.gif", "duration": "20-30 minutes"},
        {"name": "Kickboard Drills", "gif": "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExY3pwcmJxcTM4eXIxaW5sdHF5bWsyeHo2N2U1cGx1c2Zpbnplem5vaSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/j3u1wvgXA0tL5JaOcW/giphy.gif", "duration": "10 minutes"},
    ],
    "Walking": [
        {"name": "Brisk Walk", "gif": "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExMzVhb3p4YWwyYWVrdmVzdmljNm82NzRjNTR0eGxhd3Btamg1bnAzbyZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/madDsd98Bt7X6fwAJ7/giphy.gif", "duration": "20-40 minutes"},
    ],
    "Tennis": [
        {"name": "Footwork Drills", "gif": "https://media4.giphy.com/media/v1.Y2lkPTc5MGI3NjExNXB0OG45cDBiYXNnMWFncDZlemgxc3ZyNW5rYTN3enY1MGkyOHo3YyZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/sXGxId3HqR1u9rOAL2/giphy.gif", "duration": "15 minutes"},
        {"name": "Rally Practice", "gif": "https://media4.giphy.com/media/v1.Y2lkPTc5MGI3NjExeWh5eW9sbXE3NHZ4YmsyamxrY2ZtYnc5cWk5dGI1Mml3OXpiYWsxZyZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/3ohhwN7TULWddSDVCw/giphy.gif", "duration": "20-30 minutes"},
    ],
}


def read_table_with_fallbacks(data_path: Path) -> pd.DataFrame:
    """Read xls/xlsx/csv files with explicit parser fallbacks."""
    suffix = data_path.suffix.lower()
    attempted: list[str] = []
    last_error: Exception | None = None

    if suffix in {".xls", ".xlsx", ".xlsm"}:
        engines = ["xlrd", "openpyxl"] if suffix == ".xls" else ["openpyxl", "xlrd"]
        for engine in engines:
            attempted.append(f"excel:{engine}")
            try:
                return pd.read_excel(data_path, engine=engine)
            except Exception as error:  # noqa: BLE001
                last_error = error

        # Some files are mislabeled but contain CSV text.
        attempted.append("csv")
        try:
            return pd.read_csv(data_path)
        except Exception as error:  # noqa: BLE001
            last_error = error

    else:
        attempted.append("csv")
        try:
            return pd.read_csv(data_path)
        except Exception as error:  # noqa: BLE001
            last_error = error

        # Non-standard extension but actual Excel payload.
        for engine in ["openpyxl", "xlrd"]:
            attempted.append(f"excel:{engine}")
            try:
                return pd.read_excel(data_path, engine=engine)
            except Exception as error:  # noqa: BLE001
                last_error = error

    attempted_str = ", ".join(attempted)
    raise ValueError(
        f"Could not parse file '{data_path}'. Tried parsers: {attempted_str}. Last error: {last_error}"
    )


@st.cache_data(show_spinner=False)
def load_data(path: str) -> pd.DataFrame:
    """Load and normalize the fitness dataset with caching for speed."""
    data_path = Path(str(path).strip().strip('"').strip("'"))
    if not data_path.exists():
        raise FileNotFoundError(f"Dataset not found at: {data_path}")

    df = read_table_with_fallbacks(data_path)

    df.columns = [str(col).strip().lower() for col in df.columns]

    aliases: dict[str, list[str]] = {
        "calories_total": ["calories_burned", "calories", "total_calories"],
        "hours_sleep": ["sleep_hours", "sleep"],
        "daily_steps": ["steps", "step_count"],
        "body_fat_percent": ["body_fat", "body_fat_pct"],
        "health_condition": ["medical_condition", "condition"],
    }
    for canonical, options in aliases.items():
        if canonical not in df.columns:
            for alt in options:
                if alt in df.columns:
                    df[canonical] = df[alt]
                    break

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    numeric_columns = [
        "age",
        "height_cm",
        "weight_kg",
        "duration_minutes",
        "avg_heart_rate",
        "hours_sleep",
        "stress_level",
        "daily_steps",
        "bmi",
        "calories_total",
        "bmr",
        "body_fat_percent",
    ]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "bmi" not in df.columns and {"height_cm", "weight_kg"}.issubset(df.columns):
        height_m = df["height_cm"] / 100.0
        df["bmi"] = df["weight_kg"] / np.where(height_m > 0, height_m**2, np.nan)

    return df


@st.cache_resource(show_spinner=False)
def get_db_connection() -> Any:
    """Create and cache a MySQL connection for Streamlit reruns."""
    return mysql.connector.connect(
        host=st.secrets["DB_HOST"],
        user=st.secrets["DB_USER"],
        password=st.secrets["DB_PASSWORD"],
        database=st.secrets["DB_NAME"],
        port=int(st.secrets["DB_PORT"]),
        autocommit=False,
        use_pure=True
    )


def ensure_connection_alive(connection: Any) -> None:
    connection.ping(reconnect=True, attempts=3, delay=2)


def hash_password(password: str) -> str:
    """Secure PBKDF2 password hash storage format."""
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        bytes.fromhex(salt),
        PASSWORD_HASH_ITERATIONS,
    ).hex()
    return f"pbkdf2_sha256${PASSWORD_HASH_ITERATIONS}${salt}${digest}"


def verify_password(password: str, stored_hash: str) -> bool:
    """Validate password against stored PBKDF2 hash."""
    try:
        scheme, iter_str, salt_hex, digest = stored_hash.split("$", 3)
        if scheme != "pbkdf2_sha256":
            return False
        candidate = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            bytes.fromhex(salt_hex),
            int(iter_str),
        ).hex()
        return hmac.compare_digest(candidate, digest)
    except (ValueError, TypeError):
        return False


def create_user_account(connection: Any, email: str, username: str, password: str) -> int:
    """Insert user auth row and return new user_id."""
    ensure_connection_alive(connection)
    cursor = connection.cursor()
    try:
        query = "INSERT INTO users (email, username, password_hash) VALUES (%s, %s, %s)"
        cursor.execute(query, (email, username, hash_password(password)))
        connection.commit()
        return int(cursor.lastrowid)
    except Error:
        connection.rollback()
        raise
    finally:
        cursor.close()


def authenticate_user(connection: Any, identifier: str, password: str) -> dict[str, Any] | None:
    """Authenticate by username OR email and return user row on success."""
    ensure_connection_alive(connection)
    cursor = connection.cursor(dictionary=True)
    try:
        query = (
            "SELECT user_id, email, username, password_hash, age, gender, height_cm, weight_kg, goal, medical_condition "
            "FROM users WHERE username = %s OR email = %s LIMIT 1"
        )
        cursor.execute(query, (identifier, identifier))
        row = cursor.fetchone()
        if not row:
            return None
        if not verify_password(password, str(row.get("password_hash", ""))):
            return None
        return row
    finally:
        cursor.close()


def fetch_user_profile(connection: Any, user_id: int) -> dict[str, Any] | None:
    """Fetch profile columns for one user."""
    ensure_connection_alive(connection)
    cursor = connection.cursor(dictionary=True)
    try:
        query = (
            "SELECT user_id, email, username, age, gender, height_cm, weight_kg, goal, medical_condition "
            "FROM users WHERE user_id = %s LIMIT 1"
        )
        cursor.execute(query, (user_id,))
        return cursor.fetchone()
    finally:
        cursor.close()


def update_user_profile(
    connection: Any,
    user_id: int,
    age: int,
    gender: str,
    height_cm: float,
    weight_kg: float,
    goal: str,
    medical_condition: str,
) -> None:
    """UPDATE profile fields after signup."""
    ensure_connection_alive(connection)
    cursor = connection.cursor()
    try:
        query = (
            "UPDATE users SET age = %s, gender = %s, height_cm = %s, weight_kg = %s, "
            "goal = %s, medical_condition = %s WHERE user_id = %s"
        )
        cursor.execute(query, (age, gender, height_cm, weight_kg, goal, medical_condition, user_id))
        connection.commit()
    except Error:
        connection.rollback()
        raise
    finally:
        cursor.close()


def insert_daily_logs(connection: Any, records: list[tuple[Any, ...]]) -> None:
    """Insert multiple activity rows in one transaction."""
    ensure_connection_alive(connection)
    cursor = connection.cursor()
    try:
        query = (
            "INSERT INTO daily_logs "
            "(user_id, log_date, steps, hours_sleep, stress_level, activity_type, duration_minutes, calories_burned) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        )
        cursor.executemany(query, records)
        connection.commit()
    except Error:
        connection.rollback()
        raise
    finally:
        cursor.close()


def fetch_user_daily_logs(connection: Any, user_id: int) -> pd.DataFrame:
    """Load all daily logs for one user into DataFrame."""
    ensure_connection_alive(connection)
    cursor = connection.cursor(dictionary=True)
    try:
        query = (
            "SELECT log_date, steps, hours_sleep, stress_level, activity_type, duration_minutes, calories_burned "
            "FROM daily_logs WHERE user_id = %s ORDER BY log_date"
        )
        cursor.execute(query, (user_id,))
        return pd.DataFrame(cursor.fetchall())
    finally:
        cursor.close()


def filter_dataset(
    df: pd.DataFrame,
    age: int | None = None,
    gender: str | None = None,
    medical_condition: str | None = None,
    goal: str | None = None,
) -> pd.DataFrame:
    """Filter dataset by profile context."""
    filtered = df.copy()
    if age is not None and "age" in filtered.columns:
        filtered = filtered[filtered["age"].between(age - 5, age + 5)]
    if gender and "gender" in filtered.columns:
        filtered = filtered[filtered["gender"].astype(str).str.lower() == gender.strip().lower()]
    if medical_condition and medical_condition != "None" and "health_condition" in filtered.columns:
        filtered = filtered[
            filtered["health_condition"].astype(str).str.lower().str.contains(medical_condition.lower(), na=False)
        ]
    if goal and "activity_type" in filtered.columns:
        goal_map = {
            "Fat Loss": ["Cardio", "Running", "Cycling", "Hiit", "Walking"],
            "Muscle Gain": ["Strength", "Weight Training", "Resistance"],
            "Endurance": ["Running", "Cycling", "Swimming", "Cardio"],
        }
        keywords = [kw.lower() for kw in goal_map.get(goal, [])]
        if keywords:
            mask = filtered["activity_type"].astype(str).str.lower().apply(lambda v: any(k in v for k in keywords))
            subset = filtered[mask]
            if not subset.empty:
                filtered = subset
    return filtered


def init_session_state() -> None:
    """Initialize authentication, profile, and dashboard defaults."""
    defaults: dict[str, Any] = {
        "logged_in": False,
        "current_user_id": None,
        "current_username": "",
        "current_email": "",
        "selected_page": "Profile Setup",
        "age": 30,
        "gender": "Male",
        "height_cm": 170.0,
        "weight_kg": 70.0,
        "primary_goal": "Fat Loss",
        "available_time": 45,
        "medical_condition": "None",
        "bmi": 0.0,
        "bmr": 0.0,
        "body_fat_percent": 0.0,
        "activity_count": 1,
        "log_date": pd.Timestamp.today().date(),
        "log_steps": 7000,
        "log_sleep": 7.0,
        "log_stress": 5,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def calculate_bmi(height_cm: float, weight_kg: float) -> float:
    height_m = max(height_cm, 1.0) / 100.0
    return weight_kg / (height_m**2)


def calculate_bmr(age: int, gender: str, height_cm: float, weight_kg: float) -> float:
    gender_lower = gender.strip().lower()
    if gender_lower.startswith("m"):
        adjustment = 5
    elif gender_lower.startswith("f"):
        adjustment = -161
    else:
        adjustment = -78
    return (10 * weight_kg) + (6.25 * height_cm) - (5 * age) + adjustment


def estimate_body_fat_percent(age: int, gender: str, bmi: float) -> float:
    is_male = 1 if gender.strip().lower().startswith("m") else 0
    value = (1.2 * bmi) + (0.23 * age) - (10.8 * is_male) - 5.4
    return float(np.clip(value, 5.0, 60.0))


def refresh_profile_metrics() -> None:
    st.session_state.bmi = calculate_bmi(st.session_state.height_cm, st.session_state.weight_kg)
    st.session_state.bmr = calculate_bmr(
        st.session_state.age,
        st.session_state.gender,
        st.session_state.height_cm,
        st.session_state.weight_kg,
    )
    st.session_state.body_fat_percent = estimate_body_fat_percent(
        st.session_state.age,
        st.session_state.gender,
        st.session_state.bmi,
    )


def load_profile_into_session(user_row: dict[str, Any]) -> None:
    """Hydrate session state from auth/profile row."""
    st.session_state.current_user_id = int(user_row["user_id"])
    st.session_state.current_username = str(user_row.get("username") or "")
    st.session_state.current_email = str(user_row.get("email") or "")

    if user_row.get("age") is not None:
        st.session_state.age = int(user_row["age"])
    if user_row.get("gender"):
        st.session_state.gender = str(user_row["gender"])
    if user_row.get("height_cm") is not None:
        st.session_state.height_cm = float(user_row["height_cm"])
    if user_row.get("weight_kg") is not None:
        st.session_state.weight_kg = float(user_row["weight_kg"])
    if user_row.get("goal"):
        st.session_state.primary_goal = str(user_row["goal"])
    if user_row.get("medical_condition"):
        st.session_state.medical_condition = str(user_row["medical_condition"])

    refresh_profile_metrics()


def get_plotly_template() -> str:
    return "plotly_dark" if str(st.get_option("theme.base")).lower() == "dark" else "plotly_white"


def style_plotly_figure(fig: Any) -> Any:
    fig.update_layout(
        template=get_plotly_template(),
        margin=dict(l=20, r=20, t=50, b=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend_title_text="",
        height=380,
    )
    return fig

@st.cache_resource(show_spinner=False)
def load_persona_artifacts(path: str = "fitness_model.joblib") -> dict[str, Any]:
    """Load pre-trained clustering artifacts for lightweight inference in Streamlit."""
    artifacts = joblib.load(path)
    required = {"model", "scaler", "cluster_medians"}
    missing = required - set(artifacts.keys())
    if missing:
        raise ValueError(f"Missing keys in {path}: {', '.join(sorted(missing))}")
    return artifacts


def fetch_user_behavior_inputs(connection: Any, user_id: int | None) -> dict[str, float]:
    """Get steps/sleep/stress from MySQL logs, with session-state fallback."""
    fallback = {
        "daily_steps": float(st.session_state.get("log_steps", GOAL_STEP_TARGETS[st.session_state.primary_goal] * 0.75)),
        "hours_sleep": float(st.session_state.get("log_sleep", 7.0)),
        "stress_level": float(st.session_state.get("log_stress", 5)),
    }
    if user_id is None:
        return fallback

    ensure_connection_alive(connection)
    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute(
            "SELECT steps, hours_sleep, stress_level FROM daily_logs WHERE user_id = %s ORDER BY log_date DESC LIMIT 30",
            (int(user_id),),
        )
        rows = cursor.fetchall()
        if not rows:
            return fallback

        logs_df = pd.DataFrame(rows)
        for col in ["steps", "hours_sleep", "stress_level"]:
            logs_df[col] = pd.to_numeric(logs_df[col], errors="coerce")
        logs_df = logs_df.dropna(subset=["steps", "hours_sleep", "stress_level"])
        if logs_df.empty:
            return fallback

        return {
            "daily_steps": float(logs_df["steps"].median()),
            "hours_sleep": float(logs_df["hours_sleep"].median()),
            "stress_level": float(logs_df["stress_level"].median()),
        }
    except Error:
        return fallback
    finally:
        cursor.close()


def build_persona_name_map(cluster_medians: dict[Any, Any]) -> dict[int, str]:
    """Assign human-friendly persona names from cluster median signals."""
    rows: list[dict[str, float]] = []
    for key, metrics in cluster_medians.items():
        cluster_id = int(key)
        rows.append(
            {
                "cluster": cluster_id,
                "daily_steps": float(metrics.get("daily_steps", 0.0)),
                "hours_sleep": float(metrics.get("hours_sleep", 0.0)),
                "bmi": float(metrics.get("bmi", 99.0)),
                "stress_level": float(metrics.get("stress_level", 10.0)),
            }
        )

    ranking = pd.DataFrame(rows).set_index("cluster")
    ranking["steps_rank"] = ranking["daily_steps"].rank(ascending=False, method="min")
    ranking["sleep_rank"] = ranking["hours_sleep"].rank(ascending=False, method="min")
    ranking["bmi_rank"] = ranking["bmi"].rank(ascending=True, method="min")
    ranking["stress_rank"] = ranking["stress_level"].rank(ascending=True, method="min")
    ranking["combined_rank"] = (
        ranking["steps_rank"] + ranking["sleep_rank"] + ranking["bmi_rank"] + ranking["stress_rank"]
    )
    ordered_clusters = ranking.sort_values("combined_rank").index.tolist()
    persona_names = ["Peak Performer", "Balanced Builder", "Recovery Rebalancer", "Reset Starter"]
    return {int(cluster_id): persona_names[idx] for idx, cluster_id in enumerate(ordered_clusters)}
def get_planner_profile(connection: Any, user_id: int | None) -> dict[str, Any]:
    """Fetch planner inputs from DB with a safe fallback when optional columns are missing."""
    fallback = {
        "goal": st.session_state.primary_goal,
        "medical_condition": st.session_state.medical_condition,
        "available_time": int(st.session_state.available_time),
    }
    if user_id is None:
        return fallback

    ensure_connection_alive(connection)
    cursor = connection.cursor(dictionary=True)
    try:
        try:
            cursor.execute(
                "SELECT goal, medical_condition, available_time FROM users WHERE user_id = %s LIMIT 1",
                (int(user_id),),
            )
            row = cursor.fetchone()
        except Error:
            # Backward-compatible fallback for schemas without available_time.
            cursor.execute(
                "SELECT goal, medical_condition FROM users WHERE user_id = %s LIMIT 1",
                (int(user_id),),
            )
            row = cursor.fetchone()

        if not row:
            return fallback

        return {
            "goal": str(row.get("goal") or fallback["goal"]),
            "medical_condition": str(row.get("medical_condition") or fallback["medical_condition"]),
            "available_time": int(row.get("available_time") or fallback["available_time"]),
        }
    except Error:
        return fallback
    finally:
        cursor.close()


def generate_weekly_plan(goal: str, condition: str, available_time: int) -> pd.DataFrame:
    """Generate a 7-day personalized workout schedule with medical-aware constraints."""
    goal_activity_map: dict[str, list[str]] = {
        "Fat Loss": ["HIIT", "Running", "Cycling", "Swimming", "Weight Training"],
        "Muscle Gain": ["Weight Training", "Yoga"],
        "Endurance": ["Running", "Cycling", "Swimming", "Tennis"],
    }
    recovery_activities = ["Walking", "Yoga", "Stretching"]
    active_activities = list(goal_activity_map.get(goal, goal_activity_map["Fat Loss"]))

    if condition == "Hypertension":
        active_activities = [activity for activity in active_activities if activity != "HIIT"]
        if not active_activities:
            active_activities = ["Walking", "Yoga", "Weight Training"]

    if condition == "Asthma":
        priority = ["Swimming", "Yoga", "Weight Training"]
        prioritized = [activity for activity in priority if activity in active_activities]
        others = [activity for activity in active_activities if activity not in priority]
        active_activities = prioritized + others if prioritized else priority + others

    intensity_map = {
        "HIIT": "High",
        "Running": "Medium",
        "Cycling": "Medium",
        "Swimming": "Medium",
        "Tennis": "Medium",
        "Weight Training": "Medium",
        "Yoga": "Low",
        "Walking": "Low",
        "Stretching": "Low",
    }

    safe_available_time = max(15, int(available_time))
    active_duration = safe_available_time
    recovery_duration = max(10, int(round(safe_available_time / 2)))

    recovery_day_indexes = {2, 5}  # Day 3 and Day 6
    schedule_rows: list[dict[str, Any]] = []
    active_idx = 0
    recovery_idx = 0

    for idx in range(7):
        day_label = f"Day {idx + 1}"
        if idx in recovery_day_indexes:
            activity = recovery_activities[recovery_idx % len(recovery_activities)]
            recovery_idx += 1
            target_intensity = "Low"
            duration = recovery_duration
        else:
            activity = active_activities[active_idx % len(active_activities)]
            active_idx += 1
            target_intensity = intensity_map.get(activity, "Medium")
            duration = active_duration

        if condition == "Hypertension" and target_intensity == "High":
            target_intensity = "Medium"
        if condition == "Asthma" and activity == "Running":
            target_intensity = "Low"

        schedule_rows.append(
            {
                "Day": day_label,
                "Activity": activity,
                "Target Intensity": target_intensity,
                "Duration (mins)": int(duration),
            }
        )

    return pd.DataFrame(schedule_rows)


def render_auth_page(db_connection: Any) -> None:
    """Authentication entrypoint shown when user is not logged in."""
    st.title("Authentication")
    st.caption("Sign in or create an account to access the fitness dashboard.")

    login_tab, signup_tab = st.tabs(["Login", "Sign Up"])

    with login_tab:
        with st.form("login_form"):
            login_identifier = st.text_input("Username or Email")
            login_password = st.text_input("Password", type="password")
            login_submitted = st.form_submit_button("Login", use_container_width=True)

        if login_submitted:
            if not login_identifier.strip() or not login_password:
                st.warning("Please enter both Username/Email and Password.")
            else:
                try:
                    user_row = authenticate_user(db_connection, login_identifier.strip(), login_password)
                    if not user_row:
                        st.error("Invalid credentials. Please try again.")
                    else:
                        load_profile_into_session(user_row)
                        st.session_state.logged_in = True
                        st.session_state.selected_page = "Daily Dashboard"
                        st.rerun()
                except Error as error:
                    st.error(f"Login failed: {error}")

    with signup_tab:
        with st.form("signup_form"):
            signup_email = st.text_input("Email")
            signup_username = st.text_input("Username")
            signup_password = st.text_input("Password", type="password")
            signup_confirm_password = st.text_input("Confirm Password", type="password")
            signup_submitted = st.form_submit_button("Create Account", use_container_width=True)

        if signup_submitted:
            email = signup_email.strip()
            username = signup_username.strip()
            if not email or not username or not signup_password:
                st.warning("Email, Username, and Password are required.")
            elif signup_password != signup_confirm_password:
                st.warning("Password and Confirm Password do not match.")
            else:
                try:
                    user_id = create_user_account(db_connection, email, username, signup_password)
                    row = fetch_user_profile(db_connection, user_id)
                    if row:
                        load_profile_into_session(row)
                    else:
                        st.session_state.current_user_id = user_id
                        st.session_state.current_username = username
                        st.session_state.current_email = email
                    st.session_state.logged_in = True
                    st.session_state.selected_page = "Profile Setup"
                    st.rerun()
                except IntegrityError as error:
                    message = str(error).lower()
                    if "email" in message:
                        st.error("That email is already registered.")
                    elif "username" in message:
                        st.error("That username is already taken.")
                    else:
                        st.error(f"Sign-up failed: {error}")
                except Error as error:
                    st.error(f"Sign-up failed: {error}")


def render_profile_setup_page(db_connection: Any) -> None:
    st.title("Profile Setup")
    st.caption("Configure your baseline profile after signing up.")
    st.caption(f"Logged in as {st.session_state.current_username} ({st.session_state.current_email})")

    left_col, right_col = st.columns(2)

    with left_col:
        st.session_state.age = int(
            st.number_input("Age", min_value=15, max_value=80, value=int(st.session_state.age), step=1)
        )

        gender_options = ["Male", "Female"]
        gender_index = gender_options.index(st.session_state.gender) if st.session_state.gender in gender_options else 0
        st.session_state.gender = st.selectbox("Gender", options=gender_options, index=gender_index)

        st.session_state.height_cm = st.number_input(
            "Height (cm)", min_value=120.0, max_value=230.0, value=float(st.session_state.height_cm), step=0.5
        )
        st.session_state.weight_kg = st.number_input(
            "Weight (kg)", min_value=30.0, max_value=250.0, value=float(st.session_state.weight_kg), step=0.1
        )

    with right_col:
        goal_index = PRIMARY_GOALS.index(st.session_state.primary_goal) if st.session_state.primary_goal in PRIMARY_GOALS else 0
        st.session_state.primary_goal = st.selectbox("Primary Goal", options=PRIMARY_GOALS, index=goal_index)

        st.session_state.available_time = int(
            st.number_input(
                "Available Time (minutes/day)",
                min_value=15,
                max_value=180,
                value=int(st.session_state.available_time),
                step=5,
            )
        )

        medical_index = MEDICAL_CONDITIONS.index(st.session_state.medical_condition) if st.session_state.medical_condition in MEDICAL_CONDITIONS else 0
        st.session_state.medical_condition = st.selectbox("Medical Condition", options=MEDICAL_CONDITIONS, index=medical_index)

    refresh_profile_metrics()

    c1, c2, c3 = st.columns(3)
    c1.metric("Current BMI", f"{st.session_state.bmi:.1f}")
    c2.metric("BMR (kcal/day)", f"{st.session_state.bmr:.0f}")
    c3.metric("Estimated Body Fat %", f"{st.session_state.body_fat_percent:.1f}%")

    if st.button("Save Profile", type="primary", use_container_width=True):
        try:
            update_user_profile(
                connection=db_connection,
                user_id=int(st.session_state.current_user_id),
                age=int(st.session_state.age),
                gender=st.session_state.gender,
                height_cm=float(st.session_state.height_cm),
                weight_kg=float(st.session_state.weight_kg),
                goal=st.session_state.primary_goal,
                medical_condition=st.session_state.medical_condition,
            )
            st.success("Profile updated successfully.")
        except Error as error:
            st.error(f"Unable to update profile: {error}")


def render_predictive_engine_page(db_connection: Any) -> None:
    st.title("Predictive Engine")
    st.caption("Fast persona inference using pre-trained clustering artifacts.")

    refresh_profile_metrics()
    try:
        artifacts = load_persona_artifacts("fitness_model.joblib")
    except Exception as error:  # noqa: BLE001
        st.error(
            "Unable to load fitness_model.joblib. Please run train_model.py locally first "
            f"and place the artifact in the app directory. Details: {error}"
        )
        st.stop()

    model = artifacts["model"]
    scaler = artifacts["scaler"]
    cluster_medians = artifacts["cluster_medians"]
    persona_map = build_persona_name_map(cluster_medians)

    behavior_baseline = fetch_user_behavior_inputs(db_connection, st.session_state.current_user_id)
    user_vector = np.array(
        [
            [
                float(st.session_state.age),
                float(st.session_state.bmi),
                float(behavior_baseline["daily_steps"]),
                float(behavior_baseline["hours_sleep"]),
                float(behavior_baseline["stress_level"]),
            ]
        ]
    )
    user_scaled = scaler.transform(user_vector)
    predicted_cluster = int(model.predict(user_scaled)[0])
    persona_name = persona_map.get(predicted_cluster, f"Persona {predicted_cluster + 1}")

    cluster_profile = cluster_medians.get(predicted_cluster) or cluster_medians.get(str(predicted_cluster), {})

    st.success(f"Predicted Fitness Persona: **{persona_name}**")
    st.caption("Persona comparison is powered by precomputed cluster medians from fitness_model.joblib.")

    comparison_df = pd.DataFrame(
        {
            "Metric": ["Age", "BMI", "Daily Steps", "Hours Sleep", "Stress Level"],
            "You": [
                round(float(st.session_state.age), 2),
                round(float(st.session_state.bmi), 2),
                round(float(behavior_baseline["daily_steps"]), 2),
                round(float(behavior_baseline["hours_sleep"]), 2),
                round(float(behavior_baseline["stress_level"]), 2),
            ],
            "Persona Average": [
                round(float(cluster_profile.get("age", st.session_state.age)), 2),
                round(float(cluster_profile.get("bmi", st.session_state.bmi)), 2),
                round(float(cluster_profile.get("daily_steps", behavior_baseline["daily_steps"])), 2),
                round(float(cluster_profile.get("hours_sleep", behavior_baseline["hours_sleep"])), 2),
                round(float(cluster_profile.get("stress_level", behavior_baseline["stress_level"])), 2),
            ],
        }
    )
    st.dataframe(comparison_df, use_container_width=True, hide_index=True)
    planner_profile = get_planner_profile(db_connection, st.session_state.current_user_id)
    weekly_plan_df = generate_weekly_plan(
        goal=str(planner_profile.get("goal", st.session_state.primary_goal)),
        condition=str(planner_profile.get("medical_condition", st.session_state.medical_condition)),
        available_time=int(planner_profile.get("available_time", st.session_state.available_time)),
    )

    st.subheader("\U0001F5D3\ufe0f Your Personalized Weekly Blueprint")
    st.dataframe(weekly_plan_df, use_container_width=True, hide_index=True)

    st.subheader("\U0001F525 Today's Recommended Routine")
    if weekly_plan_df.empty:
        st.info("No weekly plan could be generated yet. Please complete your profile and try again.")
    else:
        today_activity = str(weekly_plan_df.iloc[0].get("Activity", "")).strip()
        exercise_list = EXERCISE_DB.get(today_activity, EXERCISE_DB.get("Stretching", []))
        if not exercise_list:
            st.info("No exercise visuals found for today's activity yet.")
        else:
            cols = st.columns(min(3, len(exercise_list)))
            for idx, exercise in enumerate(exercise_list):
                with cols[idx % len(cols)]:
                    exercise_name = str(exercise.get("name", "Exercise"))
                    gif_url = str(exercise.get("gif", "")).strip()
                    detail = str(exercise.get("reps") or exercise.get("duration") or "Follow good form and controlled tempo.")
                    st.markdown(f"**{exercise_name}**")
                    if gif_url:
                        st.image(gif_url, use_container_width=True)
                    st.caption(detail)

def get_activity_options() -> list[str]:
    fallback_options = ["Walking", "Running", "Cycling", "Weight Training", "Yoga", "Swimming", "Cardio", "HIIT"]
    return fallback_options


def calculate_session_calories(activity_type: str, duration_minutes: float) -> float:
    activity_intensity = {
        "Walking": 4.2,
        "Running": 9.8,
        "Cycling": 7.5,
        "Weight Training": 6.2,
        "Yoga": 3.2,
        "Swimming": 8.0,
        "Cardio": 7.0,
        "HIIT": 10.5,
    }
    base_factor = activity_intensity.get(activity_type, 5.5)
    weight_factor = max(float(st.session_state.weight_kg), 35.0) / 70.0
    bmi_factor = 1.0 + max(float(st.session_state.bmi) - 22.0, 0.0) * 0.015
    return round(max(duration_minutes, 0.0) * base_factor * weight_factor * bmi_factor, 2)


def reset_daily_log_form() -> None:
    st.session_state.activity_count = 1
    st.session_state.log_date = pd.Timestamp.today().date()
    st.session_state.log_steps = 7000
    st.session_state.log_sleep = 7.0
    st.session_state.log_stress = 5
    st.session_state["activity_duration_0"] = 0

    dynamic_keys = [
        k for k in st.session_state.keys() if k.startswith("activity_type_") or k.startswith("activity_duration_")
    ]
    for key in dynamic_keys:
        suffix = key.rsplit("_", 1)[-1]
        if suffix.isdigit() and int(suffix) > 0:
            del st.session_state[key]


def submit_and_reset_log(db_connection: Any) -> None:
    """Callback: insert rows into MySQL and reset widget state."""
    if st.session_state.current_user_id is None:
        st.session_state.dashboard_error = "Please log in first."
        return
    if db_connection is None:
        st.session_state.dashboard_error = "Database connection is unavailable."
        return

    records: list[tuple[Any, ...]] = []
    log_date = st.session_state.get("log_date", pd.Timestamp.today().date())

    for idx in range(int(st.session_state.get("activity_count", 1))):
        activity_type = str(st.session_state.get(f"activity_type_{idx}", "")).strip()
        duration_minutes = float(st.session_state.get(f"activity_duration_{idx}", 0))
        if not activity_type or duration_minutes <= 0:
            continue

        calories_burned = calculate_session_calories(activity_type, duration_minutes)
        records.append(
            (
                int(st.session_state.current_user_id),
                log_date,
                int(st.session_state.get("log_steps", 0)),
                float(st.session_state.get("log_sleep", 0.0)),
                int(st.session_state.get("log_stress", 1)),
                activity_type,
                float(duration_minutes),
                float(calories_burned),
            )
        )

    if not records:
        st.session_state.dashboard_warning = "Please enter at least one activity with duration greater than 0."
        return

    try:
        insert_daily_logs(db_connection, records)
        st.session_state.dashboard_success = f"Saved {len(records)} activity record(s) for {log_date}."
        reset_daily_log_form()
    except Error as error:
        st.session_state.dashboard_error = f"Unable to save daily logs: {error}"


def render_daily_dashboard_page(db_connection: Any) -> None:
    st.title("Daily Dashboard")
    st.caption("Log your daily health stats and multiple activities, then visualize trends from MySQL.")

    if "dashboard_success" in st.session_state:
        st.success(st.session_state.pop("dashboard_success"))
    if "dashboard_warning" in st.session_state:
        st.warning(st.session_state.pop("dashboard_warning"))
    if "dashboard_error" in st.session_state:
        st.error(st.session_state.pop("dashboard_error"))

    if st.session_state.current_user_id is None:
        st.info("Please complete Profile Setup and save profile before logging activities.")
        return

    activity_options = get_activity_options()

    st.subheader("Log Daily Activity")
    with st.container(border=True):
        c1, c2 = st.columns(2)
        with c1:
            st.date_input("Date", key="log_date")
            st.number_input("Steps", min_value=0, max_value=100000, step=100, key="log_steps")
        with c2:
            st.number_input(
                "Sleep (hours)",
                min_value=0.0,
                max_value=24.0,
                step=0.5,
                key="log_sleep",
            )
            st.number_input(
                "Stress (1-10)",
                min_value=1,
                max_value=10,
                step=1,
                key="log_stress",
            )

        st.markdown("**Activities**")
        for idx in range(int(st.session_state.activity_count)):
            a_col, d_col = st.columns(2)
            with a_col:
                st.selectbox(f"Activity Type #{idx + 1}", options=activity_options, key=f"activity_type_{idx}")
            with d_col:
                st.number_input(
                    f"Duration (minutes) #{idx + 1}",
                    min_value=0,
                    max_value=600,
                    step=5,
                    key=f"activity_duration_{idx}",
                )

        action_col_1, action_col_2 = st.columns(2)
        with action_col_1:
            if st.button("+ Add another activity", key="add_activity_btn"):
                st.session_state.activity_count = int(st.session_state.activity_count) + 1
                st.rerun()
        with action_col_2:
            if st.button("- Remove last activity", key="remove_activity_btn"):
                if int(st.session_state.activity_count) > 1:
                    st.session_state.activity_count = int(st.session_state.activity_count) - 1
                    st.rerun()

        st.button(
            "Save Daily Log",
            type="primary",
            use_container_width=True,
            on_click=submit_and_reset_log,
            args=(db_connection,),
        )

    try:
        logs_df = fetch_user_daily_logs(db_connection, int(st.session_state.current_user_id))
    except Error as error:
        st.error(f"Unable to load daily logs: {error}")
        return

    if logs_df.empty:
        st.info("No MySQL activity logs yet. Save your first daily log above to generate charts.")
        return

    logs_df["log_date"] = pd.to_datetime(logs_df["log_date"], errors="coerce")
    for column in ["steps", "hours_sleep", "stress_level", "duration_minutes", "calories_burned"]:
        logs_df[column] = pd.to_numeric(logs_df[column], errors="coerce")
    logs_df = logs_df.dropna(subset=["log_date", "steps", "hours_sleep", "stress_level", "calories_burned"])

    st.subheader("Recent Activity Rows (MySQL)")
    st.dataframe(logs_df.sort_values("log_date", ascending=False).head(20), use_container_width=True, hide_index=True)

    st.subheader("Weekly Steps vs Goal")
    daily_steps_df = logs_df.groupby("log_date", as_index=False).agg(steps=("steps", "max")).sort_values("log_date")
    daily_steps_df["week_start"] = daily_steps_df["log_date"].dt.to_period("W").dt.start_time
    weekly_steps = daily_steps_df.groupby("week_start", as_index=False)["steps"].sum().sort_values("week_start")
    weekly_steps["Goal"] = GOAL_STEP_TARGETS[st.session_state.primary_goal] * 7
    weekly_long = weekly_steps.melt(id_vars="week_start", value_vars=["steps", "Goal"], var_name="Series", value_name="Steps")
    weekly_long["Series"] = weekly_long["Series"].replace({"steps": "Weekly Steps"})
    fig_weekly = px.line(weekly_long, x="week_start", y="Steps", color="Series", markers=True)
    style_plotly_figure(fig_weekly)
    st.plotly_chart(fig_weekly, use_container_width=True)

    st.subheader("Hours of Sleep vs Stress Level")
    sleep_stress_df = logs_df.groupby("log_date", as_index=False).agg(hours_sleep=("hours_sleep", "mean"), stress_level=("stress_level", "mean")).sort_values("log_date")
    fig_sleep_stress = px.scatter(
        sleep_stress_df,
        x="hours_sleep",
        y="stress_level",
        hover_data=["log_date"],
        labels={"hours_sleep": "Sleep (hours)", "stress_level": "Stress Level"},
    )
    style_plotly_figure(fig_sleep_stress)
    st.plotly_chart(fig_sleep_stress, use_container_width=True)

    st.subheader("Calories Burned by Activity Type")
    calories_df = logs_df.groupby("activity_type", as_index=False)["calories_burned"].sum().sort_values("calories_burned", ascending=False)
    fig_calories = px.bar(
        calories_df,
        x="activity_type",
        y="calories_burned",
        color="activity_type",
        labels={"activity_type": "Activity Type", "calories_burned": "Calories Burned"},
    )
    style_plotly_figure(fig_calories)
    fig_calories.update_xaxes(categoryorder="total descending")
    st.plotly_chart(fig_calories, use_container_width=True)


def build_smart_coach_plan() -> dict[str, Any]:
    refresh_profile_metrics()
    goal = st.session_state.primary_goal
    condition = st.session_state.medical_condition
    available_time = int(st.session_state.available_time)
    bmr = float(st.session_state.bmr)
    calorie_target = bmr + GOAL_CALORIE_DELTAS[goal]

    warmup = max(5, int(round(available_time * 0.15)))
    cooldown = max(5, int(round(available_time * 0.10)))
    main = max(10, available_time - warmup - cooldown)

    plan: dict[str, Any] = {"calorie_target": calorie_target, "workout": [], "nutrition": [], "recovery": [], "safety": []}

    if goal == "Fat Loss":
        plan["workout"] = [
            f"Daily {available_time}-minute session: {warmup} min warm-up, {main} min moderate cardio + strength, {cooldown} min cool-down.",
            f"Target {GOAL_STEP_TARGETS[goal]:,} steps/day.",
        ]
        plan["nutrition"] = [f"Calorie target: ~{calorie_target:.0f} kcal/day (BMR - 500).", "Prioritize protein and fiber."]
    elif goal == "Muscle Gain":
        plan["workout"] = [
            f"Daily {available_time}-minute session: {warmup} min mobility, {main} min progressive strength, {cooldown} min stretching.",
            f"Target {GOAL_STEP_TARGETS[goal]:,} steps/day.",
        ]
        plan["nutrition"] = [f"Calorie target: ~{calorie_target:.0f} kcal/day (BMR + 300).", "Spread protein across 3-5 meals."]
    else:
        plan["workout"] = [
            f"Daily {available_time}-minute session: {warmup} min warm-up, {main} min endurance work, {cooldown} min recovery.",
            f"Target {GOAL_STEP_TARGETS[goal]:,}+ steps/day.",
        ]
        plan["nutrition"] = [f"Calorie target: ~{calorie_target:.0f} kcal/day (BMR + 100).", "Fuel with carbs before long sessions."]

    plan["recovery"] = [
        "Sleep target: 7-9 hours/night.",
        f"Hydration target: {max(2.0, st.session_state.weight_kg * 0.035):.1f} liters/day.",
    ]

    if condition == "Hypertension":
        plan["safety"].append("Avoid high-intensity workouts.")
    if condition == "Asthma":
        plan["safety"].append("Prefer indoor Weight Training or Yoga when symptoms flare.")
    if condition == "Diabetes":
        plan["safety"].append("Monitor glucose around workouts and pair carbs with protein.")

    return plan


def render_smart_coach_page() -> None:
    st.title("Smart Coach")
    st.caption("Actionable daily coaching generated from your profile.")

    plan = build_smart_coach_plan()
    c1, c2 = st.columns(2)
    c1.metric("Daily Caloric Target", f"{plan['calorie_target']:.0f} kcal")
    c2.metric("Daily Step Target", f"{GOAL_STEP_TARGETS[st.session_state.primary_goal]:,} steps")

    st.subheader("Daily Action Plan")
    st.markdown("**Training**")
    for item in plan["workout"]:
        st.markdown(f"- {item}")
    st.markdown("**Nutrition**")
    for item in plan["nutrition"]:
        st.markdown(f"- {item}")
    st.markdown("**Recovery**")
    for item in plan["recovery"]:
        st.markdown(f"- {item}")
    if plan["safety"]:
        st.subheader("Medical Overrides & Safety")
        for item in plan["safety"]:
            st.warning(item)


def main() -> None:
    init_session_state()
    refresh_profile_metrics()

    try:
        db_connection = get_db_connection()
    except (Error, KeyError) as error:
        st.error(f"MySQL connection unavailable: {error}")
        st.stop()

    # Authentication gate: hide main app/nav until login.
    if not st.session_state.logged_in:
        render_auth_page(db_connection)
        return

    if st.session_state.selected_page not in NAV_PAGES:
        st.session_state.selected_page = "Profile Setup"

    with st.sidebar:
        st.title("Fitness Coach")
        st.caption(f"Signed in as {st.session_state.current_username} ({st.session_state.current_email})")
        if st.button("Log Out", use_container_width=True):
            st.session_state.logged_in = False
            st.session_state.current_user_id = None
            st.session_state.current_username = ""
            st.session_state.current_email = ""
            st.session_state.selected_page = "Profile Setup"
            st.rerun()

        st.markdown("---")
        st.radio("Navigation", options=NAV_PAGES, key="selected_page")

    selected_page = st.session_state.selected_page
    if selected_page == "Profile Setup":
        render_profile_setup_page(db_connection)
    elif selected_page == "Predictive Engine":
        render_predictive_engine_page(db_connection)
    elif selected_page == "Daily Dashboard":
        render_daily_dashboard_page(db_connection)
    else:
        render_smart_coach_page()


if __name__ == "__main__":
    main()
