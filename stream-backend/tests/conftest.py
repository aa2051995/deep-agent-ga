"""Shared pytest configuration.

The dummy agent streams with realistic per-token/per-step delays by default (so
the UI shows progressive streaming). Force them to 0 in the test suite so runs
complete instantly.
"""
import os

os.environ["STREAM_BACKEND_FIXTURE_TOKEN_DELAY"] = "0"
os.environ["STREAM_BACKEND_FIXTURE_STEP_DELAY"] = "0"
