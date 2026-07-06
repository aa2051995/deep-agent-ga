from __future__ import annotations

import logging
import os

from .event_bus import PublishingRepository, create_event_broker
from .store import InMemoryRepository, Repository

logger = logging.getLogger("stream_backend.runtime")


def create_repository() -> Repository:
    mode = os.getenv("STREAM_BACKEND_STORE", "memory").lower()
    logger.info("repository.create.start mode=%s", mode)
    if mode == "postgres":
        dsn = (
            os.getenv("STREAM_BACKEND_POSTGRES_URI")
            or os.getenv("POSTGRES_URI")
            or os.getenv("DATABASE_URL")
        )
        if not dsn:
            raise RuntimeError(
                "STREAM_BACKEND_STORE=postgres requires STREAM_BACKEND_POSTGRES_URI, "
                "POSTGRES_URI, or DATABASE_URL."
            )
        from .store_postgres import PostgresRepository

        logger.info("repository.create.postgres")
        return PostgresRepository(dsn)
    logger.info("repository.create.memory")
    return InMemoryRepository()


def create_publishing_repository() -> PublishingRepository:
    return PublishingRepository(create_repository(), create_event_broker())
