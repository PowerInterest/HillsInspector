from __future__ import annotations

import os
from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker


DEFAULT_PG_DSN = "postgresql+psycopg://hills:hills_dev@localhost:5432/hills_sunbiz"


def resolve_pg_dsn(explicit_dsn: str | None = None) -> str:
    if explicit_dsn:
        return explicit_dsn
    return os.getenv("SUNBIZ_PG_DSN", DEFAULT_PG_DSN)


@lru_cache(maxsize=8)
def get_engine(dsn: str) -> object:
    return create_engine(dsn, pool_pre_ping=True)


def get_session_factory(dsn: str) -> sessionmaker[Session]:
    engine = get_engine(dsn)
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)

