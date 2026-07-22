# Bedrock run fails: "The provided model identifier is invalid"

**Date:** 2026-07-22
**Area:** Backend — assistant model building (AWS Bedrock)

## Symptom

A research run on the `deep-agent` assistant crashed in the worker with:

```
botocore.errorfactory.ValidationException: An error occurred (ValidationException)
when calling the ConverseStream operation: The provided model identifier is invalid.
```

## Root causes (two, compounding)

1. **Inconsistent provider/model pair.** The seeded `deep-agent` assistant had
   `provider: "bedrock"` but `name: "gemini-2.5-pro"` — a Google model id sent to
   Bedrock. `default_seed_assistants()` read the provider and the model from
   independent env vars (`RESEARCH_AGENT_PROVIDER`, `RESEARCH_AGENT_MODEL`) with a
   Google default, so a bedrock provider could pair with a Gemini id.

2. **Builder ignored the assistant's model.** `assistant_builder.build_model`'s
   Bedrock branch called `bedrock_model_kwargs()`, which resolves the id from the
   `RESEARCH_AGENT_MODEL` / `AWS_BEDROCK_MODEL_ID` env vars — not the assistant's
   configured `model.name`. The configured name only partially overrode it, so a
   stale/foreign env id leaked through.

3. **Region inference profiles.** Anthropic (and Meta/Mistral) models on Bedrock
   must be invoked through a cross-region inference profile
   (e.g. `eu.anthropic.claude-3-5-sonnet-20240620-v1:0`) in regions like
   `eu-north-1`; the bare `anthropic.*` id is rejected as invalid.

## Related files

- `stream-backend/app/assistants.py` — seed + new consistency helpers.
- `stream-backend/app/assistant_builder.py` — Bedrock model construction.
- `stream-backend/app/assistant_catalog.py` — per-provider model catalog.
- `stream-backend/assistants/{deep-agent,general-purpose}/assistant.json` — data.

## Solution

- `build_model` Bedrock branch now uses the assistant's own `model.name`
  directly, pulling only region/profile/endpoint from the AWS env.
- Added `model_matches_provider()` / `consistent_model()` / `default_model_for()`
  and made `default_seed_assistants()` normalize the pair, so a mismatched
  env model is replaced by the provider's valid default.
- Added `bedrock_region_prefix()` (from `AWS_REGION`) and made the model catalog
  region-aware, offering `eu.`/`us.`/`apac.` inference-profile ids plus Amazon
  Nova (on-demand). Removed the invalid `moonshotai.kimi-k2.5` entry.
- Repaired the on-disk `deep-agent` / `general-purpose` configs to a consistent
  `eu.anthropic.claude-3-5-sonnet-20240620-v1:0`.

## Best practices

- Keep provider and model **coupled**: never source them from independent env
  vars without validating the pair.
- Build models from the persisted config, not ambient env, so what you tested is
  what runs.
- For Bedrock, prefer region inference-profile ids; use the `Test model` button
  (`POST /assistants/assist/test-model`) to confirm an id resolves in the target
  account/region before saving.
