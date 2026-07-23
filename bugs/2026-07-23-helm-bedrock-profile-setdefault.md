# Helm deploy: `set_env()` forces a bogus `AWS_BEDROCK_PROFILE` and breaks IRSA

**Date:** 2026-07-23
**Area:** Kubernetes/Helm deployment (`deploy/helm/deep-research`) + backend env bootstrap

## Root cause

`stream-backend/app/main.py` `set_env()` runs at import time and does:

```python
os.environ.setdefault("AWS_BEDROCK_PROFILE", "my-profile")
```

`setdefault` only leaves the variable alone if it is **already present** in the
environment. When deploying to EKS with IRSA (the recommended Bedrock auth), no
`AWS_BEDROCK_PROFILE` is provided, so `set_env()` injects the literal
`"my-profile"`. The agent builder then reads it:

```python
profile = os.getenv("AWS_PROFILE") or os.getenv("AWS_BEDROCK_PROFILE")
if profile:
    bedrock_kwargs["credentials_profile_name"] = profile
```

boto3 then looks for a profile named `my-profile` in `~/.aws/credentials` inside
the pod — which does not exist — instead of falling back to the web-identity /
instance-role credential chain. Bedrock calls fail with a profile/credentials
error even though IRSA is correctly configured.

## Related files

- `stream-backend/app/main.py` (`set_env()`, the `setdefault` line)
- `stream-backend/app/assistant_builder.py`, `research_runtime.py`, `bedrock_catalog.py` (read the profile)
- `deploy/helm/deep-research/templates/_helpers.tpl` (`deep-research.backendEnv`)

## Solution

The chart **always** emits `AWS_BEDROCK_PROFILE`, setting it to an empty string
when `app.aws.useProfile=false`. An empty string is present-but-falsy: `setdefault`
sees the key already exists and does not overwrite it, and the `if profile:` guard
treats `""` as "no profile", so boto3 uses the default credential chain (IRSA /
instance role / static keys). When `useProfile=true`, the configured profile name
is emitted instead.

## Best practices

- A container that relies on `setdefault` for config is only safe if every such
  key is *explicitly* set (even to empty) by the orchestrator; otherwise baked-in
  developer defaults leak into production.
- Prefer an empty env value over an unset one to positively disable a feature that
  has a non-empty hardcoded default upstream.
- For AWS auth in-cluster, prefer IRSA and assert no `AWS_*_PROFILE` is set.
