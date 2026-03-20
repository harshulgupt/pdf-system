"""
Repository Pattern — this is the KEY architectural piece the interviewer wants to see.

The service layer calls ONLY these methods.
To swap DB: implement a new class with the same interface (e.g. MongoChunkRepository).
Routes never touch the DB directly.
"""
from abc import ABC, abstractmethod
from typing import List
from sqlalchemy.orm import Session
from sqlalchemy import func
from app.db.models import PDFChunk


# ---------- Abstract interface ----------

class AbstractChunkRepository(ABC):
    @abstractmethod
    def save(self, chunk: PDFChunk) -> PDFChunk: ...

    @abstractmethod
    def search(self, query: str, limit: int) -> List[PDFChunk]: ...

    @abstractmethod
    def get_by_file(self, file_id: str) -> List[PDFChunk]: ...

    @abstractmethod
    def delete_by_filename(self, filename: str) -> int: ...

    @abstractmethod
    def delete_all(self) -> int: ...


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
        Case-insensitive search.
        SQLite LIKE is case-insensitive for ASCII but not Unicode — using
        func.lower() on both sides makes it reliable for all characters.
        Postgres upgrade: swap this body for a tsvector @@ tsquery call.
        """
        q = query.lower()
        return (
            self.db.query(PDFChunk)
            .filter(func.lower(PDFChunk.content).contains(q))
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

    def delete_by_filename(self, filename: str) -> int:
        """
        Delete all chunks for a given filename before re-uploading.
        This prevents stale results from old versions of the same file.
        Returns count of rows deleted.
        """
        deleted = (
            self.db.query(PDFChunk)
            .filter(PDFChunk.filename == filename)
            .delete(synchronize_session=False)
        )
        self.db.commit()
        return deleted

    def delete_all(self) -> int:
        """Wipe the entire index. Returns count deleted."""
        deleted = self.db.query(PDFChunk).delete(synchronize_session=False)
        self.db.commit()
        return deleted


# ---------- Example stub: swap to this for MongoDB ----------
# class MongoChunkRepository(AbstractChunkRepository):
#     def __init__(self, collection): self.col = collection
#     def save(self, chunk): self.col.insert_one(chunk.__dict__); return chunk
#     def search(self, query, limit=20): return list(self.col.find({"$text": {"$search": query}}).limit(limit))
#     def get_by_file(self, file_id): return list(self.col.find({"file_id": file_id}))
#     def delete_by_filename(self, filename): return self.col.delete_many({"filename": filename}).deleted_count
#     def delete_all(self): return self.col.delete_many({}).deleted_count
