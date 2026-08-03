# Helm: global.imageRegistry wrongly redirected postgres/rabbitmq to the app registry

**Date:** 2026-07-23
**Area:** Helm chart image resolution (`deploy/helm/deep-agent-ga`)

## Root cause

The `deep-agent-ga.image` helper prefixed **every** image with
`global.imageRegistry`, including the third-party `postgres` and `rabbitmq`
images. Setting `global.imageRegistry` to a private ECR (so the app images pull
from ECR) also rewrote:

```
postgres:16-alpine          -> 553138586148.dkr.ecr.us-east-1.amazonaws.com/postgres:16-alpine
rabbitmq:3.13-management    -> 553138586148.dkr.ecr.us-east-1.amazonaws.com/rabbitmq:3.13-management
```

Those images do not exist in the user's ECR, so both StatefulSets would fail to
pull (`ImagePullBackOff`) and the whole stack would never come up.

## Related files

- `deploy/helm/deep-agent-ga/templates/_helpers.tpl` (`deep-agent-ga.image`)
- `deploy/helm/deep-agent-ga/templates/{apiserver,worker,ui,postgres,rabbitmq}.yaml`
- `deploy/helm/deep-agent-ga/values.yaml`

## Solution

Split the helper in two:

- `deep-agent-ga.appImage` — used by apiserver/worker/ui; applies
  `global.imageRegistry` (or a per-image `registry`). These live in the user's
  registry.
- `deep-agent-ga.image` — used by postgres/rabbitmq; ignores
  `global.imageRegistry` and pulls from Docker Hub, with an optional per-image
  `registry` for teams that mirror via an ECR pull-through cache.

Regression test: `test_global_registry_only_prefixes_app_images`.

## Best practices

- A single "global registry prefix" is only safe for images you actually publish.
  Third-party images must have an independent registry knob (or none), or the
  prefix silently breaks their pulls.
- Assert image references in chart render tests — an `ImagePullBackOff` is
  otherwise only discovered at deploy time.
