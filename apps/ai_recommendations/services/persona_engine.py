"""
Persona Engine Service
-----------------------
Loads the pre-trained fitness_model.joblib and runs clustering inference.
Pure Python — no Django imports. Can be called from views, DRF API endpoints,
or Celery tasks without any refactoring.

Ported from: load_persona_artifacts(), build_persona_name_map(),
             fetch_user_behavior_inputs(), predict_persona() in app.py
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

# Module-level singleton — replaces @st.cache_resource
_artifacts_cache: dict[str, Any] | None = None


def _load_artifacts(model_path: Path) -> dict[str, Any]:
    """Load and cache joblib artifacts in memory for the process lifetime."""
    global _artifacts_cache
    if _artifacts_cache is None:
        artifacts = joblib.load(model_path)
        required = {"model", "scaler", "cluster_medians"}
        if missing := required - set(artifacts.keys()):
            raise ValueError(f"Missing keys in model artifact: {', '.join(sorted(missing))}")
        _artifacts_cache = artifacts
    return _artifacts_cache


def build_persona_name_map(cluster_medians: dict) -> dict[int, str]:
    """Assign human-friendly persona names from cluster median signals."""
    rows = [
        {
            "cluster": int(k),
            "daily_steps": float(v.get("daily_steps", 0.0)),
            "hours_sleep": float(v.get("hours_sleep", 0.0)),
            "bmi": float(v.get("bmi", 99.0)),
            "stress_level": float(v.get("stress_level", 10.0)),
        }
        for k, v in cluster_medians.items()
    ]
    df = pd.DataFrame(rows).set_index("cluster")
    df["combined_rank"] = (
        df["daily_steps"].rank(ascending=False, method="min")
        + df["hours_sleep"].rank(ascending=False, method="min")
        + df["bmi"].rank(ascending=True, method="min")
        + df["stress_level"].rank(ascending=True, method="min")
    )
    ordered = df.sort_values("combined_rank").index.tolist()
    names = ["Peak Performer", "Balanced Builder", "Recovery Rebalancer", "Reset Starter"]
    return {int(cid): names[i] for i, cid in enumerate(ordered)}


def predict_persona(
    model_path: Path,
    age: float,
    bmi: float,
    daily_steps: float,
    hours_sleep: float,
    stress_level: float,
) -> dict[str, Any]:
    """
    Run ML clustering inference and return persona + comparison metrics.

    Returns a clean dict — the view/API just passes it to template/serializer.
    """
    artifacts = _load_artifacts(model_path)
    model = artifacts["model"]
    scaler = artifacts["scaler"]
    cluster_medians = artifacts["cluster_medians"]
    persona_map = build_persona_name_map(cluster_medians)

    user_vector = np.array([[age, bmi, daily_steps, hours_sleep, stress_level]])
    scaled = scaler.transform(user_vector)
    cluster_id = int(model.predict(scaled)[0])

    persona_name = persona_map.get(cluster_id, f"Persona {cluster_id + 1}")
    cluster_profile = cluster_medians.get(cluster_id) or cluster_medians.get(str(cluster_id), {})

    return {
        "persona_name": persona_name,
        "cluster_id": cluster_id,
        "user_metrics": {
            "Age": round(age, 1),
            "BMI": round(bmi, 2),
            "Daily Steps": round(daily_steps, 0),
            "Hours Sleep": round(hours_sleep, 1),
            "Stress Level": round(stress_level, 1),
        },
        "persona_averages": {
            "Age": round(float(cluster_profile.get("age", age)), 1),
            "BMI": round(float(cluster_profile.get("bmi", bmi)), 2),
            "Daily Steps": round(float(cluster_profile.get("daily_steps", daily_steps)), 0),
            "Hours Sleep": round(float(cluster_profile.get("hours_sleep", hours_sleep)), 1),
            "Stress Level": round(float(cluster_profile.get("stress_level", stress_level)), 1),
        },
    }
