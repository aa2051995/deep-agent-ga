"""Unit tests for the Jenkins CI/CD pipeline definition.

These are structural/contract checks — they do NOT require Jenkins, kubectl,
helm, or AWS. They guard the wiring that is easy to break silently:

  * the pipeline builds BOTH app images from the right contexts;
  * on `main` it pushes immutable <git-sha> AND rolling :latest, and on a PR it
    does NOT push (--no-push);
  * the deploy pins every app image to the built tag and waits for rollout;
  * the agent ServiceAccount carries the IRSA annotation and the deploy RBAC;
  * the ECR IAM policy grants auth + layer-push on both repositories.
"""
from __future__ import annotations

import json
from pathlib import Path

import yaml

# examples/deep_research/deploy/cicd/tests/test_pipeline.py -> examples/deep_research
APP_DIR = Path(__file__).resolve().parents[3]
CICD_DIR = APP_DIR / "deploy" / "cicd"

JENKINSFILE = APP_DIR / "Jenkinsfile"
SA_MANIFEST = CICD_DIR / "jenkins-agent-serviceaccount.yaml"
IAM_POLICY = CICD_DIR / "iam-policy-jenkins-ecr.json"


def _jenkinsfile() -> str:
    assert JENKINSFILE.is_file(), f"missing {JENKINSFILE}"
    return JENKINSFILE.read_text(encoding="utf-8")


# --------------------------------------------------------------------------- #
# Jenkinsfile
# --------------------------------------------------------------------------- #
def test_jenkinsfile_has_expected_stages():
    text = _jenkinsfile()
    for stage in ("Setup", "Validate", "Build images", "Deploy", "Verify rollout"):
        assert f"stage('{stage}')" in text, f"missing stage {stage!r}"


def test_builds_both_app_contexts():
    text = _jenkinsfile()
    assert "/stream-backend" in text, "backend build context not referenced"
    assert '"${env.APP_DIR}/ui"' in text or "${APP_DIR}/ui" in text, "ui build context not referenced"
    assert "buildImage(env.BACKEND_REPO" in text
    assert "buildImage(env.UI_REPO" in text


def test_main_pushes_sha_and_latest_pr_does_not_push():
    text = _jenkinsfile()
    # On main: both the immutable sha tag and rolling latest.
    assert "${dest}:${env.IMAGE_TAG}" in text
    assert "${dest}:latest" in text
    # On a PR (not main): kaniko must not push.
    assert "--no-push" in text
    assert "env.IS_MAIN == 'true'" in text, "push/deploy must be gated on main"


def test_image_tag_is_immutable_git_sha():
    text = _jenkinsfile()
    assert "git rev-parse --short" in text, "IMAGE_TAG should derive from the git SHA"


def test_deploy_pins_all_app_image_tags():
    text = _jenkinsfile()
    for comp in ("apiserver", "worker", "ui"):
        assert f"--set {comp}.image.tag=" in text, f"deploy does not pin {comp} image tag"
    assert "helm upgrade --install" in text
    assert "-f \"${VALUES}\"" in text
    assert "--wait" in text, "deploy should wait for readiness"


def test_deploy_and_verify_gated_on_main():
    text = _jenkinsfile()
    # Both Deploy and Verify use the main-only guard expression.
    assert text.count("when { expression { env.IS_MAIN == 'true' } }") >= 2


def test_kaniko_needs_no_docker_login():
    # IRSA supplies ECR creds to kaniko; an explicit docker login would be a smell.
    text = _jenkinsfile()
    assert "docker login" not in text


# --------------------------------------------------------------------------- #
# Agent ServiceAccount + RBAC
# --------------------------------------------------------------------------- #
def _sa_docs() -> list[dict]:
    assert SA_MANIFEST.is_file(), f"missing {SA_MANIFEST}"
    return [d for d in yaml.safe_load_all(SA_MANIFEST.read_text(encoding="utf-8")) if d]


def test_service_account_has_irsa_annotation():
    sa = next(d for d in _sa_docs() if d.get("kind") == "ServiceAccount")
    ann = sa.get("metadata", {}).get("annotations", {})
    assert "eks.amazonaws.com/role-arn" in ann, "SA missing IRSA role annotation"
    assert ann["eks.amazonaws.com/role-arn"].startswith("arn:aws:iam::")


def test_deploy_role_grants_core_workloads():
    role = next(d for d in _sa_docs() if d.get("kind") == "Role")
    granted: dict[str, set[str]] = {}
    for rule in role["rules"]:
        for group in rule["apiGroups"]:
            granted.setdefault(group, set()).update(rule["resources"])
    assert "deployments" in granted.get("apps", set())
    assert "statefulsets" in granted.get("apps", set())
    assert "secrets" in granted.get("", set()), "helm stores release state as Secrets"
    assert "ingresses" in granted.get("networking.k8s.io", set())
    assert "horizontalpodautoscalers" in granted.get("autoscaling", set())


def test_rolebinding_targets_the_agent_sa():
    sa = next(d for d in _sa_docs() if d.get("kind") == "ServiceAccount")
    rb = next(d for d in _sa_docs() if d.get("kind") == "RoleBinding")
    subj = rb["subjects"][0]
    assert subj["kind"] == "ServiceAccount"
    assert subj["name"] == sa["metadata"]["name"]
    assert rb["roleRef"]["name"] == "deep-research-deployer"


# --------------------------------------------------------------------------- #
# IAM policy for kaniko ECR push
# --------------------------------------------------------------------------- #
def test_iam_policy_allows_ecr_push():
    assert IAM_POLICY.is_file(), f"missing {IAM_POLICY}"
    policy = json.loads(IAM_POLICY.read_text(encoding="utf-8"))
    actions: set[str] = set()
    resources: set[str] = set()
    for stmt in policy["Statement"]:
        assert stmt["Effect"] == "Allow"
        acts = stmt["Action"]
        actions.update([acts] if isinstance(acts, str) else acts)
        res = stmt["Resource"]
        resources.update([res] if isinstance(res, str) else res)
    assert "ecr:GetAuthorizationToken" in actions
    assert "ecr:PutImage" in actions
    assert "ecr:UploadLayerPart" in actions
    # Push scoped to the two app repositories.
    assert any(r.endswith("repository/deepresrepo") for r in resources)
    assert any(r.endswith("repository/uirepo") for r in resources)
