"""Discover the Bedrock models actually available in the configured account.

Guessing model ids (base vs. region inference profile) is fragile — what's
invocable depends on the account, region, and enabled model access. This module
asks Bedrock directly (``list_inference_profiles`` + ``list_foundation_models``)
so the UI can offer real, usable ids instead of hard-coded guesses.
"""
from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger("stream_backend.bedrock_catalog")


def _aws_region() -> str | None:
    return (
        os.getenv("RESEARCH_AGENT_AWS_REGION")
        or os.getenv("AWS_BEDROCK_REGION")
        or os.getenv("AWS_REGION")
        or os.getenv("AWS_DEFAULT_REGION")
    )


def list_bedrock_models() -> dict[str, Any]:
    """Return ``{ok, models, message}`` where ``models`` is a list of {name,label}.

    ``models`` combines:
    - cross-region **inference profiles** (ids like ``eu.anthropic.claude-...``),
      which is how most Anthropic/Meta/Mistral models must be invoked, and
    - **on-demand foundation models** (bare ``vendor.model`` ids) that don't need
      a profile (e.g. Amazon Nova).
    """
    region = _aws_region()
    profile = os.getenv("AWS_PROFILE") or os.getenv("AWS_BEDROCK_PROFILE")
    endpoint = os.getenv("AWS_BEDROCK_ENDPOINT_URL")
    try:
        import boto3
    except Exception as exc:  # pragma: no cover - environment dependent
        return {"ok": False, "models": [], "message": f"boto3 is not installed: {exc}"}

    try:
        session = boto3.Session(profile_name=profile) if profile else boto3.Session()
        client_kwargs: dict[str, str] = {}
        if region:
            client_kwargs["region_name"] = region
        if endpoint:
            client_kwargs["endpoint_url"] = endpoint
        client = session.client("bedrock", **client_kwargs)
    except Exception as exc:
        logger.warning("bedrock_catalog.client_failed", exc_info=True)
        return {"ok": False, "models": [], "message": f"Could not create Bedrock client: {exc}"}

    models: list[dict[str, str]] = []

    try:
        paginator_profiles = client.list_inference_profiles()
        for prof in paginator_profiles.get("inferenceProfileSummaries", []):
            profile_id = prof.get("inferenceProfileId")
            if not profile_id:
                continue
            name = prof.get("inferenceProfileName") or profile_id
            models.append({"name": profile_id, "label": f"{name} (profile)"})
    except Exception as exc:
        logger.warning("bedrock_catalog.list_inference_profiles_failed error=%s", exc)

    try:
        foundation = client.list_foundation_models()
        for summary in foundation.get("modelSummaries", []):
            model_id = summary.get("modelId")
            if not model_id:
                continue
            types = summary.get("inferenceTypesSupported") or []
            # Only on-demand ids are directly invocable without a profile.
            if "ON_DEMAND" not in types:
                continue
            label = summary.get("modelName") or model_id
            provider_name = summary.get("providerName") or ""
            models.append({"name": model_id, "label": f"{label}" + (f" — {provider_name}" if provider_name else "")})
    except Exception as exc:
        logger.warning("bedrock_catalog.list_foundation_models_failed error=%s", exc)

    # Dedupe by id, keep first (profiles listed first).
    seen: set[str] = set()
    unique: list[dict[str, str]] = []
    for entry in models:
        if entry["name"] in seen:
            continue
        seen.add(entry["name"])
        unique.append(entry)

    if not unique:
        return {
            "ok": False,
            "models": [],
            "message": (
                f"No invocable Bedrock models found in region {region or '<default>'}. "
                "Check the region and that model access is enabled in the AWS console."
            ),
        }
    logger.info("bedrock_catalog.listed region=%s count=%s", region, len(unique))
    return {"ok": True, "models": unique, "message": f"{len(unique)} models available in {region or 'default region'}."}
