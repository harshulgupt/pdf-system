"""
Database layer — currently SQLite via SQLAlchemy.
To swap to PostgreSQL: change DATABASE_URL and install psycopg2.
To swap to MongoDB: replace this file and update the repository.
"""
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./pdf_store.db")

# SQLite needs this connect_args; remove for Postgres
connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

engine = create_engine(DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class Base(DeclarativeBase):
    pass

def get_db():
    """FastAPI dependency — yields a DB session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db():
    from app.db import models  # noqa: F401 — registers models
    Base.metadata.create_all(bind=engine)
