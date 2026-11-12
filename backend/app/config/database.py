"""Engine / session factory shared by the FastAPI app and the Celery worker."""
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.config.settings import settings


class Base(DeclarativeBase):
    pass


engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_size=8,
    max_overflow=4,
    future=True,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, class_=Session)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _ensure_incremental_schema() -> None:
    """Upgrade databases created before partition scheduling existed.

    ``create_all`` only creates missing tables; it never alters existing ones,
    so the new ``compact_jobs`` columns are added here idempotently.
    """
    from sqlalchemy import inspect, text

    inspector = inspect(engine)
    if "compact_jobs" not in set(inspector.get_table_names()):
        return
    columns = {column["name"] for column in inspector.get_columns("compact_jobs")}
    statements = []
    if "admission_state" not in columns:
        statements.append(
            "ALTER TABLE compact_jobs ADD COLUMN admission_state VARCHAR(16) NOT NULL DEFAULT 'waiting'"
        )
    if "waiting_reason" not in columns:
        statements.append("ALTER TABLE compact_jobs ADD COLUMN waiting_reason TEXT NULL")
    if "admitted_at" not in columns:
        statements.append("ALTER TABLE compact_jobs ADD COLUMN admitted_at DATETIME NULL")
    if "snapshot_version" not in columns:
        statements.append("ALTER TABLE compact_jobs ADD COLUMN snapshot_version INT NULL")
    if "idempotency_key" not in columns:
        statements.append("ALTER TABLE compact_jobs ADD COLUMN idempotency_key VARCHAR(64) NULL")
    if statements:
        with engine.begin() as connection:
            for statement in statements:
                connection.execute(text(statement))
        inspector = inspect(engine)
    indexes = {index["name"] for index in inspector.get_indexes("compact_jobs")}
    if "uq_compact_job_idempotency" not in indexes:
        with engine.begin() as connection:
            connection.execute(
                text("CREATE UNIQUE INDEX uq_compact_job_idempotency ON compact_jobs (idempotency_key)")
            )


def init_db(max_attempts: int = 30, delay: float = 2.0) -> None:
    """Create all tables, waiting for MySQL to finish its first boot."""
    import time

    import app.models  # noqa: F401

    last_error = None
    for _ in range(max_attempts):
        try:
            Base.metadata.create_all(bind=engine)
            _ensure_incremental_schema()
            return
        except Exception as exc:  # pragma: no cover - infra retry
            last_error = exc
            time.sleep(delay)
    raise RuntimeError(f"database bootstrap failed: {last_error}")
