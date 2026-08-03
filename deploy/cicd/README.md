# CI/CD — Jenkins on EKS

Continuous delivery for Deep Research: **every merge to `main` builds, pushes,
and deploys** the app to the `deepres` EKS cluster via the Helm chart; **every
pull request** is validated (lint + render + unit tests + a no-push image build)
before it can merge.

The pipeline is defined in [`../../Jenkinsfile`](../../Jenkinsfile).

```
 PR opened ─▶  Validate ─▶ Chart tests ─▶ Build (--no-push)                (no deploy)
 merge main ─▶ Validate ─▶ Chart tests ─▶ Build & PUSH ─▶ Deploy ─▶ Verify rollout
                                          <git-sha>+latest   helm upgrade
```

## How it runs (in-cluster, no static keys)

Jenkins agents run **as pods on the EKS cluster** (Kubernetes plugin). One
`ServiceAccount` (`jenkins-agent`) carries two grants:

| Concern | Mechanism | Used by |
|---|---|---|
| Push images to ECR | **IRSA** role (`eks.amazonaws.com/role-arn` annotation) | the `kaniko` container |
| Deploy the Helm release | **in-cluster RBAC** (`Role`/`RoleBinding` in `deep-research`) | the `tools` (helm/kubectl) container |

Because agents run in-cluster, `helm`/`kubectl` use the pod's own in-cluster
config — **no `aws eks update-kubeconfig`, no kubeconfig file, no IAM user keys**
for deploys. Images are built with **kaniko** (no Docker daemon / no privileged
pod); kaniko reads ECR credentials straight from the IRSA identity.

## Image tags

- `REGISTRY/deepresrepo:<git-sha>` and `:latest` — backend (apiserver + worker).
- `REGISTRY/uirepo:<git-sha>` and `:latest` — UI.

The deploy pins **`image.tag=<git-sha>`** for all three workloads, so a release
is reproducible and rollback is `helm rollback` (or re-deploy an older SHA).
`:latest` is kept only for humans / manual `kubectl rollout restart`.

---

## One-time setup

### 1. Prerequisites
- The `deepres` EKS cluster with **IRSA/OIDC enabled**
  (`eksctl utils associate-iam-oidc-provider --cluster deepres --region us-east-1 --approve`).
- Jenkins running with the **Kubernetes**, **Git**, and **GitHub Branch Source**
  plugins. Jenkins can run in the cluster (recommended) or anywhere that can
  reach the cluster API; the **agents** must run as pods in the cluster.
- The ECR repos `deepresrepo` and `uirepo` already exist.
- The `deep-research` namespace already exists (the deploy does not create it).

### 2. IAM role for ECR push (IRSA)
```bash
aws iam create-policy \
  --policy-name DeepResearchJenkinsEcrPush \
  --policy-document file://deploy/cicd/iam-policy-jenkins-ecr.json

# Creates the jenkins-agent ServiceAccount in the `jenkins` namespace AND the
# IAM role, and wires the IRSA trust/annotation for you:
eksctl create iamserviceaccount \
  --cluster deepres --region us-east-1 \
  --namespace jenkins --name jenkins-agent \
  --role-name deep-research-jenkins-agent \
  --attach-policy-arn arn:aws:iam::553138586148:policy/DeepResearchJenkinsEcrPush \
  --approve
```
> If your Jenkins agents run in a namespace other than `jenkins`, use it in both
> the command above and in [`jenkins-agent-serviceaccount.yaml`](jenkins-agent-serviceaccount.yaml).

### 3. Deploy RBAC for the agent SA
```bash
kubectl apply -f deploy/cicd/jenkins-agent-serviceaccount.yaml
```
This grants `jenkins-agent` permission to manage the chart's resources (and
helm's release Secrets) **only** in the `deep-research` namespace. If eksctl
already created the SA, the SA block is a harmless re-declaration that documents
the required IRSA annotation — the `Role`/`RoleBinding` are the parts that matter.

### 4. Jenkins credentials (model/tool API keys)
The deploy stage injects API keys from Jenkins credentials (Secret text), so they
never live in the repo:

| Credential ID | Value |
|---|---|
| `deep-research-tavily-api-key` | `TAVILY_API_KEY` |
| `deep-research-anthropic-api-key` | `ANTHROPIC_API_KEY` |

> **Alternative (recommended for prod):** pre-create a Kubernetes Secret with all
> keys once and set `--set secrets.create=false --set secrets.existingSecret=<name>`
> in the deploy step, so the pipeline never handles the keys at all. Bedrock auth
> should use the app's own IRSA role (`serviceAccount.annotations`), not keys.

### 5. Create the multibranch pipeline job
- **New Item → Multibranch Pipeline**, source = this GitHub repo.
- **Build Configuration → Script Path** = `examples/deep_research/Jenkinsfile`.
- Add a **GitHub webhook** (`.../github-webhook/`) so PRs and merges trigger builds
  automatically (or enable periodic scan as a fallback).
- Discover branches + Discover pull requests from origin.

---

## What each stage does

| Stage | Runs on | Container | Action |
|---|---|---|---|
| Setup | all | tools | Compute `IMAGE_TAG=<git-sha>`, set `IS_MAIN`. |
| Validate | all | tools / python | `helm lint` + `helm template`; pipeline unit tests (`deploy/cicd/tests`). |
| Chart render tests | all | tools | `pytest deploy/helm/deep-research/tests` (chart wiring). |
| Build images | all | kaniko | Build both images; **push** on `main`, `--no-push` on PRs. |
| Deploy | `main` only | tools | `helm upgrade --install` pinning `image.tag=<git-sha>`, `--wait`. |
| Verify rollout | `main` only | tools | `kubectl rollout status` for apiserver/worker/ui. |

## Tests

The pipeline contract is unit-tested (no cluster needed):

```bash
python -m pytest examples/deep_research/deploy/cicd/tests -q   # use the `dra` conda env
```

## Troubleshooting

- **kaniko `PutImage` / `denied`** → the IRSA role/policy isn't attached to the
  agent SA, or the repo ARNs in `iam-policy-jenkins-ecr.json` don't match.
- **helm deploy `forbidden`** → the `Role`/`RoleBinding` weren't applied, or the
  `subjects[0].namespace` doesn't match the namespace your agents actually run in.
- **`ImagePullBackOff` after deploy** → EKS **node** role needs
  `AmazonEC2ContainerRegistryReadOnly` (pull is the node's job, separate from the
  agent's push role).
- **PR build tries to deploy** → deploy/verify are gated on `env.IS_MAIN`; ensure
  the branch source reports the target branch name as `main`.
