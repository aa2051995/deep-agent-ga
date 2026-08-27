"""Centralised configuration for the Deep Agent GA backend.

Every tunable -- connection strings, provider selection, and **all** API keys --
is read from the process environment. Nothing secret is hardcoded here; this
module only supplies *non-secret* operational defaults (store backend, queue
name, log level, ...) so a bare ``uvicorn app.main:app`` still boots in a sane
single-node configuration.

Resolution order for any setting (first hit wins):

1. A real environment variable already exported in the process.
2. A key/value from a ``.env`` file (see :func:`load_dotenv_files`).
3. The non-secret default in :data:`DEFAULTS` (secrets have no default).

Usage -- call :func:`configure` once, as early as possible, in every entrypoint
(the FastAPI app, the Celery worker). It is idempotent::

    from .settings import configure
    settings = configure()

Downstream modules keep reading ``os.getenv(...)`` as before: :func:`configure`
exports the resolved values back into ``os.environ``, so the settings object and
the environment never disagree.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger("stream_backend.settings")

# --------------------------------------------------------------------------
# .env discovery
# --------------------------------------------------------------------------

#: Filenames looked for in each candidate directory, nearest-first.
_ENV_FILENAMES = (".env", ".env.local")


def _candidate_dirs() -> list[Path]:
    """Return directories searched for a ``.env`` file, nearest-first."""
    here = Path(__file__).resolve()
    stream_backend_dir = here.parent.parent      # .../stream-backend
    repo_root = stream_backend_dir.parent        # repository root
    dirs = [Path.cwd(), stream_backend_dir, repo_root]
    seen: set[Path] = set()
    unique: list[Path] = []
    for directory in dirs:
        try:
            resolved = directory.resolve()
        except OSError:  # pragma: no cover - unreadable cwd
            continue
        if resolved not in seen:
            seen.add(resolved)
            unique.append(resolved)
    return unique


def _parse_env_file(path: Path) -> dict[str, str]:
    """Parse a minimal ``KEY=value`` .env file (``#`` comments, optional quotes)."""
    values: dict[str, str] = {}
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:  # pragma: no cover - unreadable file behaves like a missing one
        return values
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("export "):
            line = line[7:].lstrip()
        key, sep, value = line.partition("=")
        if not sep:
            continue
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        if not value:
            # `.env.example` ships every key with a blank value as documentation.
            # Exporting "" would shadow a real value from a wider-scoped .env and
            # hand libraries an empty credential; treat blank as "not set".
            continue
        values[key] = value
    return values


def load_dotenv_files() -> list[Path]:
    """Load ``.env`` files into ``os.environ`` without overriding real env vars.

    Returns the list of files that were actually read, nearest-first.
    """
    loaded: list[Path] = []
    for directory in _candidate_dirs():
        for filename in _ENV_FILENAMES:
            path = directory / filename
            if not path.is_file():
                continue
            for key, value in _parse_env_file(path).items():
                # A real exported env var (or an earlier, nearer .env) wins.
                os.environ.setdefault(key, value)
            loaded.append(path)
    return loaded


# --------------------------------------------------------------------------
# Non-secret defaults
# --------------------------------------------------------------------------

#: Operational defaults applied when the variable is unset. NEVER put an API
#: key, password, or any other credential in this mapping -- secrets must come
#: from the environment or a .env file that is git-ignored.
DEFAULTS: dict[str, str] = {
    # --- Persistence & transports -----------------------------------------
    "STREAM_BACKEND_STORE": "memory",              # memory | postgres
    "STREAM_BACKEND_EVENT_BROKER": "memory",       # memory | rabbitmq
    "STREAM_BACKEND_RUNNER_BACKEND": "asyncio",    # asyncio | celery
    "STREAM_BACKEND_CELERY_QUEUE": "deep-agent-ga-runs",
    "STREAM_BACKEND_ASSISTANT_STORE": "filesystem",  # filesystem | pg
    # --- Agent ------------------------------------------------------------
    "STREAM_BACKEND_AGENT_MODE": "auto",           # auto | research | fixture
    "STREAM_BACKEND_TEST_AGENT": "false",
    "RESEARCH_AGENT_PROVIDER": "google",           # google | anthropic | openai | bedrock
    # --- Celery behaviour -------------------------------------------------
    # terminate=True needs a prefork pool (see worker/README.md); cancellation
    # is cooperative by default via research_runtime's cancel_requested poll.
    "STREAM_BACKEND_CELERY_TERMINATE_ON_CANCEL": "false",
    # --- Logging ----------------------------------------------------------
    "STREAM_BACKEND_LOG_LEVEL": "INFO",
    "STREAM_BACKEND_LIBRARY_LOG_LEVEL": "WARNING",
    "STREAM_BACKEND_LOG_COLOR": "true",
}

#: Variables treated as credentials: never defaulted, always masked in logs.
SECRET_KEYS: tuple[str, ...] = (
    "TAVILY_API_KEY",
    "GOOGLE_API_KEY",
    "ANTHROPIC_API_KEY",
    "OPENAI_API_KEY",
    "LANGSMITH_API_KEY",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "STREAM_BACKEND_POSTGRES_URI",
    "POSTGRES_URI",
    "DATABASE_URL",
    "STREAM_BACKEND_CELERY_BROKER_URL",
    "CELERY_BROKER_URL",
    "STREAM_BACKEND_CELERY_RESULT_BACKEND",
    "CELERY_RESULT_BACKEND",
    "RABBITMQ_STREAM_URL",
    "RABBITMQ_URL",
)

#: API keys required per research provider -- used by :meth:`Settings.missing_secrets`.
PROVIDER_KEYS: dict[str, tuple[str, ...]] = {
    "google": ("GOOGLE_API_KEY",),
    "anthropic": ("ANTHROPIC_API_KEY",),
    "openai": ("OPENAI_API_KEY",),
    "bedrock": (),  # uses the AWS credential chain (IRSA / profile / static keys)
}


def mask(value: str | None) -> str:
    """Render a secret safe for logs, or ``<unset>`` when absent."""
    if not value:
        return "<unset>"
    if len(value) <= 8:
        return "***"
    return value[:4] + "..." + value[-2:]


# --------------------------------------------------------------------------
# Settings object
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Settings:
    """A read-only view of the resolved configuration."""

    env_files: tuple[Path, ...] = field(default_factory=tuple)

    # -- generic accessors (always reflect the live environment) -----------
    def get(self, key: str, default: str | None = None) -> str | None:
        """Return the resolved value of ``key``."""
        return os.environ.get(key, default)

    def flag(self, key: str, default: bool = False) -> bool:
        """Return ``key`` parsed as a boolean."""
        raw = os.environ.get(key)
        if raw is None:
            return default
        return raw.strip().lower() not in {"0", "false", "no", "off", ""}

    def integer(self, key: str, default: int) -> int:
        """Return ``key`` parsed as an int, falling back to ``default``."""
        try:
            return int(os.environ[key])
        except (KeyError, ValueError):
            return default

    # -- named settings ----------------------------------------------------
    @property
    def store(self) -> str:
        """Durable store backend: ``memory`` or ``postgres``."""
        return os.environ.get("STREAM_BACKEND_STORE", "memory").lower()

    @property
    def event_broker(self) -> str:
        """Event-bus backend: ``memory`` or ``rabbitmq``."""
        return (
            os.environ.get("STREAM_BACKEND_EVENT_BROKER")
            or os.environ.get("STREAM_BACKEND_STREAM_BROKER")
            or "memory"
        ).lower()

    @property
    def runner_backend(self) -> str:
        """Run execution backend: ``asyncio`` (in-process) or ``celery``."""
        return (
            os.environ.get("STREAM_BACKEND_RUNNER_BACKEND")
            or os.environ.get("STREAM_BACKEND_EXECUTION_BACKEND")
            or "asyncio"
        ).lower()

    @property
    def postgres_uri(self) -> str | None:
        """Application-store DSN, from any of the three accepted variables."""
        return (
            os.environ.get("STREAM_BACKEND_POSTGRES_URI")
            or os.environ.get("POSTGRES_URI")
            or os.environ.get("DATABASE_URL")
        )

    @property
    def celery_broker_url(self) -> str:
        """AMQP URL Celery enqueues run tasks on."""
        return (
            os.environ.get("STREAM_BACKEND_CELERY_BROKER_URL")
            or os.environ.get("CELERY_BROKER_URL")
            or "amqp://guest:guest@localhost:5672//"
        )

    @property
    def rabbitmq_stream_url(self) -> str:
        """RabbitMQ *Streams* URL (port 5552) used for event fan-out."""
        return (
            os.environ.get("RABBITMQ_STREAM_URL")
            or os.environ.get("RABBITMQ_URL")
            or "rabbitmq-stream://guest:guest@localhost:5552/"
        )

    @property
    def research_provider(self) -> str:
        """Fallback model provider for assistants that do not pin their own."""
        return os.environ.get("RESEARCH_AGENT_PROVIDER", "google").strip().lower()

    @property
    def research_model(self) -> str | None:
        """Fallback model id for assistants that do not pin their own."""
        return os.environ.get("RESEARCH_AGENT_MODEL")

    @property
    def aws_region(self) -> str | None:
        """Region used for Bedrock, from any of the accepted variables."""
        return (
            os.environ.get("RESEARCH_AGENT_AWS_REGION")
            or os.environ.get("AWS_BEDROCK_REGION")
            or os.environ.get("AWS_REGION")
            or os.environ.get("AWS_DEFAULT_REGION")
        )

    # -- validation / diagnostics ------------------------------------------
    def missing_secrets(self) -> list[str]:
        """Return credentials this configuration needs but did not receive.

        Only checks what is actually required: the API key for the selected
        research provider, ``TAVILY_API_KEY`` (the web-search tool every
        research assistant uses), and the Postgres DSN when
        ``STREAM_BACKEND_STORE=postgres``.
        """
        missing: list[str] = []
        if not os.environ.get("TAVILY_API_KEY"):
            missing.append("TAVILY_API_KEY")
        for key in PROVIDER_KEYS.get(self.research_provider, ()):
            if not os.environ.get(key):
                missing.append(key)
        if self.store == "postgres" and not self.postgres_uri:
            missing.append("STREAM_BACKEND_POSTGRES_URI")
        return missing

    def summary(self) -> dict[str, str]:
        """Return a log-safe view of the resolved configuration."""
        view = {
            "store": self.store,
            "event_broker": self.event_broker,
            "runner_backend": self.runner_backend,
            "assistant_store": os.environ.get("STREAM_BACKEND_ASSISTANT_STORE", "filesystem"),
            "celery_queue": os.environ.get("STREAM_BACKEND_CELERY_QUEUE", "celery"),
            "research_provider": self.research_provider,
            "research_model": self.research_model or "<provider default>",
            "aws_region": self.aws_region or "<unset>",
            "postgres_uri": mask(self.postgres_uri),
            "celery_broker_url": mask(self.celery_broker_url),
            "rabbitmq_stream_url": mask(self.rabbitmq_stream_url),
        }
        for key in ("TAVILY_API_KEY", "GOOGLE_API_KEY", "ANTHROPIC_API_KEY",
                    "OPENAI_API_KEY", "AWS_ACCESS_KEY_ID"):
            view[key.lower()] = mask(os.environ.get(key))
        return view


_settings: Settings | None = None


def configure(*, force: bool = False) -> Settings:
    """Load ``.env`` files, apply non-secret defaults, and return the snapshot.

    Idempotent: repeated calls return the cached :class:`Settings` unless
    ``force=True``. Safe to call from every entrypoint (apiserver, worker) and
    from tests.
    """
    global _settings
    if _settings is not None and not force:
        return _settings

    env_files = load_dotenv_files()
    for key, value in DEFAULTS.items():
        os.environ.setdefault(key, value)

    _settings = Settings(env_files=tuple(env_files))
    return _settings


def get_settings() -> Settings:
    """Return the process-wide settings, configuring them on first use."""
    return configure()


def log_configuration(settings: Settings | None = None) -> None:
    """Log one INFO line with the resolved config and warn about missing keys.

    Secrets are masked; nothing here ever prints a full credential.
    """
    settings = settings or get_settings()
    if settings.env_files:
        logger.info(
            "settings.env_files loaded=%s",
            ", ".join(str(path) for path in settings.env_files),
        )
    logger.info(
        "settings.resolved %s",
        " ".join(f"{key}={value}" for key, value in settings.summary().items()),
    )
    missing = settings.missing_secrets()
    if missing:
        logger.warning(
            "settings.missing credentials=%s -- set them in the environment or a .env "
            "file (see .env.example); runs that need them will fail.",
            ", ".join(missing),
        )
