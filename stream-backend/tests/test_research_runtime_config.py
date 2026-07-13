import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from app.research_runtime import (
    ResearchRuntimeUnavailable,
    bedrock_model_kwargs,
    ensure_research_agent_import_paths,
    provider_from_env,
)


class ResearchRuntimeConfigTests(unittest.TestCase):
    def test_provider_from_env_normalizes_value(self) -> None:
        with patch.dict(os.environ, {"RESEARCH_AGENT_PROVIDER": "AWS-BEDROCK"}):
            self.assertEqual(provider_from_env(), "aws-bedrock")

    def test_bedrock_model_kwargs_requires_model(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ResearchRuntimeUnavailable, "RESEARCH_AGENT_MODEL"):
                bedrock_model_kwargs()

    def test_bedrock_model_kwargs_reads_aws_settings(self) -> None:
        with patch.dict(
            os.environ,
            {
                "AWS_BEDROCK_MODEL_ID": "provider.model-version",
                "AWS_REGION": "us-west-2",
                "AWS_PROFILE": "research-profile",
                "AWS_BEDROCK_ENDPOINT_URL": "https://bedrock-runtime.us-west-2.amazonaws.com",
                "AWS_BEARER_TOKEN_BEDROCK": "test-token",
                "RESEARCH_AGENT_TEMPERATURE": "0.2",
                "RESEARCH_AGENT_MAX_TOKENS": "4096",
            },
            clear=True,
        ):
            kwargs = bedrock_model_kwargs()

        self.assertEqual(kwargs["model"], "provider.model-version")
        self.assertEqual(kwargs["region_name"], "us-west-2")
        self.assertEqual(kwargs["credentials_profile_name"], "research-profile")
        self.assertEqual(kwargs["endpoint_url"], "https://bedrock-runtime.us-west-2.amazonaws.com")
        self.assertEqual(kwargs["temperature"], 0.2)
        self.assertEqual(kwargs["max_tokens"], 4096)
        self.assertNotIn("AWS_BEARER_TOKEN_BEDROCK", kwargs)
        self.assertNotIn("bearer_token", kwargs)

    def test_ensure_research_agent_import_paths_adds_project_paths(self) -> None:
        backend_dir = str(Path(__file__).resolve().parents[1])
        project_dir = str(Path(__file__).resolve().parents[2])
        original = list(sys.path)
        try:
            sys.path = [path for path in sys.path if path not in {backend_dir, project_dir}]

            added = ensure_research_agent_import_paths()

            self.assertTrue({backend_dir, project_dir}.intersection(sys.path))
            self.assertTrue(set(added).issubset({backend_dir, project_dir}))
            if backend_dir in sys.path and project_dir in sys.path:
                self.assertLess(sys.path.index(backend_dir), sys.path.index(project_dir))
        finally:
            sys.path = original


if __name__ == "__main__":
    unittest.main()
