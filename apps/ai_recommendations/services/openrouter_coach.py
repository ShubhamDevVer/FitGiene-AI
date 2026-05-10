"""
ai_recommendations/services/openrouter_coach.py
─────────────────────────────────────────────────
Service layer for the Smart Coach LLM feature.

Responsibilities:
  - Build a structured system + user prompt from the FitUser profile.
  - Call the OpenRouter Chat Completions API (Minimax M2.5 model).
  - Parse and validate the JSON plan from the response.
  - Return a clean Python dict to the view — no Django imports here.

Design notes:
  - Pure Python — no Django imports — so it can be called from views,
    Celery tasks, or DRF endpoints without refactoring.
  - Uses urllib.request (stdlib) so no extra dependencies needed.
    (httpx / requests are fine alternatives if already in requirements.txt)
  - API key is injected at call time from settings — never hardcoded.
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

# ── Prompt constants ──────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are FitGenie's elite AI fitness coach — a board-certified
personal trainer, sports nutritionist, and physical therapist rolled into one.

Your job is to generate a HIGHLY PERSONALISED coaching plan based on the user
profile provided. The plan must be safe, realistic, and account for any medical
conditions listed.

OUTPUT RULES — follow these exactly:
1. Your entire response must be a single valid JSON object.
2. Do NOT include any markdown, code fences, commentary, or text outside the JSON.
3. Start your response with { and end with } — nothing else.
4. All string values must use escaped double quotes inside JSON.

The JSON object must contain exactly these four keys:

{
  "training_summary": "string — 3-4 sentences on their workout strategy given their persona and goal",
  "nutrition_guide": {
    "strategy": "string — 2-3 sentences on dietary approach",
    "macros": { "protein_g": number, "carbs_g": number, "fat_g": number, "calories": number },
    "meals": [
      { "name": "string", "description": "string", "kcal": number },
      { "name": "string", "description": "string", "kcal": number },
      { "name": "string", "description": "string", "kcal": number }
    ]
  },
  "recovery_protocol": {
    "sleep": "string — specific sleep recommendation",
    "injury_prevention": "string — condition-specific advice",
    "techniques": ["string", "string", "string"]
  },
  "action_steps": [
    { "step": 1, "title": "string", "description": "string" },
    { "step": 2, "title": "string", "description": "string" },
    { "step": 3, "title": "string", "description": "string" }
  ]
}"""


def _build_user_prompt(profile: dict[str, Any]) -> str:
    """Convert the user's profile dict to a rich natural-language prompt."""
    return f"""Generate a personalised coaching plan for this FitGenie user:

── BIOMETRICS ──────────────────────────────
Age:              {profile.get('age', 'Unknown')}
Gender:           {profile.get('gender', 'Unknown')}
Weight:           {profile.get('weight_kg', '?')} kg
Height:           {profile.get('height_cm', '?')} cm
BMI:              {profile.get('bmi', '?')} ({profile.get('bmi_category', 'Unknown')})
BMR (est.):       {profile.get('bmr', '?')} kcal/day at rest

── GOALS & PROFILE ─────────────────────────
Primary Goal:     {profile.get('goal', 'Fat Loss')}
ML Persona:       {profile.get('persona', 'Balanced Builder')}
Available Time:   {profile.get('available_time', 45)} min/day

── HEALTH FLAGS ────────────────────────────
Medical Condition: {profile.get('medical_condition', 'None')}

── ACTIVITY BASELINE (30-day median) ───────
Daily Steps:      {profile.get('daily_steps', 7000):.0f}
Avg Sleep:        {profile.get('hours_sleep', 7.0):.1f} hrs/night
Stress Level:     {profile.get('stress_level', 5):.1f}/10

Produce the full JSON plan now. Every recommendation must account for
the medical condition and be safe for someone with that condition.
The tone should be encouraging but direct."""


# ── Public API ────────────────────────────────────────────────────────────────

def get_coaching_plan(
    profile: dict[str, Any],
    api_key: str,
    base_url: str = "https://openrouter.ai/api/v1",
    model: str = "minimax/minimax-01",
    timeout: int = 45,
) -> dict[str, Any]:
    """
    Call the OpenRouter Chat Completions endpoint and return a parsed plan dict.

    Args:
        profile:  Dict of user biometrics, goal, persona, and activity baseline.
        api_key:  OpenRouter secret key — sourced from settings, never hardcoded.
        base_url: OpenRouter base URL (overridable for testing).
        model:    Model identifier on OpenRouter.
        timeout:  Request timeout in seconds.

    Returns:
        Parsed plan dict with keys:
          training_summary, nutrition_guide, recovery_protocol, action_steps

    Raises:
        CoachServiceError: wraps any network or parse failure with a clean message.
    """
    if not api_key:
        raise CoachServiceError(
            "OPENROUTER_API_KEY is not configured. "
            "Add it to your .env file and restart the server."
        )

    payload = {
        "model": model,
        "messages": [
            {"role": "system",  "content": SYSTEM_PROMPT},
            {"role": "user",    "content": _build_user_prompt(profile)},
        ],
        "temperature": 0.7,
        "max_tokens":  1800,
    }

    body    = json.dumps(payload).encode("utf-8")
    headers = {
        "Authorization":  f"Bearer {api_key}",
        "Content-Type":   "application/json",
        "HTTP-Referer":   "https://fitgenie.ai",      # required by OpenRouter
        "X-Title":        "FitGenie AI Smart Coach",
    }

    req = urllib.request.Request(
        url     = f"{base_url}/chat/completions",
        data    = body,
        headers = headers,
        method  = "POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise CoachServiceError(
            f"OpenRouter returned HTTP {exc.code}: {error_body[:400]}"
        ) from exc
    except urllib.error.URLError as exc:
        raise CoachServiceError(
            f"Network error reaching OpenRouter: {exc.reason}"
        ) from exc

    # Extract the assistant message content
    try:
        content = raw["choices"][0]["message"]["content"]
    except (KeyError, IndexError) as exc:
        raise CoachServiceError(
            f"Unexpected response structure from OpenRouter: {raw}"
        ) from exc

    # Parse the JSON plan — strip markdown fences if the model ignored instructions
    try:
        plan = json.loads(_extract_json(content))
    except json.JSONDecodeError as exc:
        raise CoachServiceError(
            f"Model returned non-JSON content: {content[:400]}"
        ) from exc

    _validate_plan(plan)
    return plan


def _extract_json(text: str) -> str:
    """
    Strip markdown code fences and extract the raw JSON object string.
    Handles patterns like:
      ```json { ... } ```
      ``` { ... } ```
      plain { ... }
    """
    import re
    # Remove ```json ... ``` or ``` ... ``` wrappers
    text = re.sub(r"^```(?:json)?\s*", "", text.strip(), flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text.strip())
    # Find the first { and last } to extract just the JSON object
    start = text.find("{")
    end   = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return text[start : end + 1]
    return text.strip()


def _validate_plan(plan: dict) -> None:
    """Raise CoachServiceError if required top-level keys are missing."""
    required = {"training_summary", "nutrition_guide", "recovery_protocol", "action_steps"}
    missing  = required - set(plan.keys())
    if missing:
        raise CoachServiceError(
            f"LLM response is missing required keys: {', '.join(sorted(missing))}"
        )


class CoachServiceError(Exception):
    """Raised for any failure in the OpenRouter coaching pipeline."""
