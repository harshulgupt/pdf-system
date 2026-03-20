"""
Service Layer — all business logic lives here.
Routes call services. Services call repositories + storage.
No SQLAlchemy imports in routes. No HTTP logic here.
"""
import io
from typing import List

import pypdf

from app.db.models import PDFChunk
from app.repositories.chunk_repository import AbstractChunkRepository
from app.storage.storage import AbstractStorage

TEXT_CHUNK_CHARS = 2000
OVERLAP_CHARS    = 200


class PDFService:
    def __init__(self, repo: AbstractChunkRepository, storage: AbstractStorage):
        self.repo = repo
        self.storage = storage

    def ingest_pdf(self, pdf_data: bytes, filename: str, file_id: str) -> List[PDFChunk]:
        """
        Full ingestion pipeline:
        1. DELETE any existing chunks for this filename (prevents stale results)
        2. Store raw bytes in storage
        3. Extract all text from the complete PDF
        4. Split text into overlapping chunks and save each to DB
        """
        # Step 1 — replace, don't append
        removed = self.repo.delete_by_filename(filename)

        # Step 2 — store raw file
        storage_path = self.storage.store_chunk(pdf_data, file_id, 0)

        # Step 3 — extract text from the COMPLETE pdf
        # (sliced bytes are not valid PDFs — pypdf would fail on fragments)
        full_text = self._extract_text(pdf_data)

        if not full_text:
            chunk = PDFChunk(
                file_id=file_id,
                filename=filename,
                chunk_index=0,
                content="[no extractable text — scanned or image-only PDF]",
                storage_path=storage_path,
            )
            return [self.repo.save(chunk)]

        # Step 4 — chunk the text and index it
        text_chunks = self._split_text(full_text, TEXT_CHUNK_CHARS, OVERLAP_CHARS)
        saved = []
        for i, text in enumerate(text_chunks):
            chunk = PDFChunk(
                file_id=file_id,
                filename=filename,
                chunk_index=i,
                content=text,
                storage_path=storage_path,
            )
            saved.append(self.repo.save(chunk))
        return saved

    def clear_all(self) -> int:
        """Wipe the entire index. Returns number of chunks deleted."""
        return self.repo.delete_all()

    def search(self, query: str, limit: int = 20) -> List[PDFChunk]:
        return self.repo.search(query, limit)

    def _extract_text(self, data: bytes) -> str:
        try:
            reader = pypdf.PdfReader(io.BytesIO(data))
            pages = [page.extract_text() or "" for page in reader.pages]
            return "\n".join(pages).strip()
        except Exception:
            return ""

    def _split_text(self, text: str, size: int, overlap: int) -> List[str]:
        chunks, start = [], 0
        while start < len(text):
            chunks.append(text[start:start + size])
            start += size - overlap
        return chunks
