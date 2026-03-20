"""
Service Layer — all business logic lives here.
Routes call services. Services call repositories + storage.
No SQLAlchemy imports in routes. No HTTP logic here.
"""
import uuid
import io
from typing import List

import pypdf

from app.db.models import PDFChunk
from app.repositories.chunk_repository import AbstractChunkRepository
from app.storage.storage import AbstractStorage

CHUNK_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB per chunk


class PDFService:
    def __init__(self, repo: AbstractChunkRepository, storage: AbstractStorage):
        self.repo = repo
        self.storage = storage

    def ingest_chunk(
        self,
        chunk_data: bytes,
        filename: str,
        file_id: str,
        chunk_index: int,
    ) -> PDFChunk:
        """
        Process one uploaded chunk:
        1. Store raw bytes in storage (local/S3)
        2. Extract text with pypdf
        3. Save metadata + text to DB via repository
        """
        storage_path = self.storage.store_chunk(chunk_data, file_id, chunk_index)
        text = self._extract_text(chunk_data)

        chunk = PDFChunk(
            file_id=file_id,
            filename=filename,
            chunk_index=chunk_index,
            content=text or f"[binary chunk {chunk_index} — no extractable text]",
            storage_path=storage_path,
        )
        return self.repo.save(chunk)

    def search(self, query: str, limit: int = 20) -> List[PDFChunk]:
        """Full-text search across all ingested chunks."""
        return self.repo.search(query, limit)

    def _extract_text(self, data: bytes) -> str:
        """Best-effort PDF text extraction. Returns empty string on failure."""
        try:
            reader = pypdf.PdfReader(io.BytesIO(data))
            return "\n".join(
                page.extract_text() or "" for page in reader.pages
            ).strip()
        except Exception:
            return ""
