"""
Repository Pattern — this is the KEY architectural piece the interviewer wants to see.

The service layer calls ONLY these methods.
To swap DB: implement a new class with the same interface (e.g. MongoChunkRepository).
Routes never touch the DB directly.
"""
from abc import ABC, abstractmethod
from typing import List
from sqlalchemy.orm import Session
from app.db.models import PDFChunk


# ---------- Abstract interface ----------

class AbstractChunkRepository(ABC):
    @abstractmethod
    def save(self, chunk: PDFChunk) -> PDFChunk: ...

    @abstractmethod
    def search(self, query: str, limit: int) -> List[PDFChunk]: ...

    @abstractmethod
    def get_by_file(self, file_id: str) -> List[PDFChunk]: ...


# ---------- SQLite / PostgreSQL implementation ----------

class SQLChunkRepository(AbstractChunkRepository):
    def __init__(self, db: Session):
        self.db = db

    def save(self, chunk: PDFChunk) -> PDFChunk:
        self.db.add(chunk)
        self.db.commit()
        self.db.refresh(chunk)
        return chunk

    def search(self, query: str, limit: int = 20) -> List[PDFChunk]:
        """
        SQLite: simple LIKE search.
        Postgres upgrade path: use tsvector full-text search.
        Elasticsearch upgrade path: replace this method body only.
        """
        return (
            self.db.query(PDFChunk)
            .filter(PDFChunk.content.ilike(f"%{query}%"))
            .limit(limit)
            .all()
        )

    def get_by_file(self, file_id: str) -> List[PDFChunk]:
        return (
            self.db.query(PDFChunk)
            .filter(PDFChunk.file_id == file_id)
            .order_by(PDFChunk.chunk_index)
            .all()
        )


# ---------- Example stub: swap to this for MongoDB ----------
# class MongoChunkRepository(AbstractChunkRepository):
#     def __init__(self, collection): self.col = collection
#     def save(self, chunk): self.col.insert_one(chunk.__dict__); return chunk
#     def search(self, query, limit=20): return list(self.col.find({"$text": {"$search": query}}).limit(limit))
#     def get_by_file(self, file_id): return list(self.col.find({"file_id": file_id}))
