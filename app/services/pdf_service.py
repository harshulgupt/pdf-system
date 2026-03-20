"""
Service Layer — all business logic. No HTTP, no SQLAlchemy imports.

THE CHUNKED UPLOAD FLOW:
  1. Client calls POST /api/upload/start  → creates session
  2. Client slices file into 5MB binary chunks
  3. Client calls POST /api/upload/chunk  for each chunk (parallel, any order)
  4. Server saves each binary chunk to disk, increments received counter
  5. When received == total_chunks, route kicks off background task
  6. Background task: assembles binary chunks → parses PDF → indexes text
  7. Client polls GET /api/upload/status/{upload_id} until status == 'indexed'

NOTE: receive_chunk() no longer triggers assembly — the route does that
via FastAPI BackgroundTasks so the chunk response is instant.
"""
from typing import List

import fitz  # pymupdf — 10-20x faster than pypdf for text extraction

from app.db.models import PDFChunk, UploadSession
from app.repositories.chunk_repository import AbstractChunkRepository
from app.storage.storage import AbstractStorage

TEXT_CHUNK_CHARS = 2000
OVERLAP_CHARS    = 200


class PDFService:
    def __init__(self, repo: AbstractChunkRepository, storage: AbstractStorage):
        self.repo    = repo
        self.storage = storage

    def start_upload(self, upload_id: str, filename: str, total_chunks: int) -> UploadSession:
        return self.repo.create_session(upload_id, filename, total_chunks)

    def receive_chunk(self, upload_id: str, passage_index: int, data: bytes) -> dict:
        """
        Save one binary chunk to disk and update counter.
        Does NOT trigger assembly — caller (route) decides when to do that.
        Returns session state including whether all chunks are now received.
        """
        session = self.repo.get_session(upload_id)
        if not session:
            raise ValueError(f"Unknown upload_id: {upload_id}")
        if session.status != "uploading":
            raise ValueError(f"Session status is '{session.status}', cannot accept more chunks.")

        self.storage.save_binary_chunk(upload_id, passage_index, data)
        session = self.repo.increment_received(upload_id)

        return {
            "received":      session.received,
            "total":         session.total_chunks,
            "status":        session.status,
            "all_received":  session.received == session.total_chunks,
        }

    def assemble_and_index(self, upload_id: str) -> None:
        """
        Called as a background task after all chunks are received.
        Runs AFTER the HTTP response is already sent to the client —
        so upload speed is never affected by how long this takes.

        Steps:
          assembling → read chunk files in order, write assembled.pdf
          indexing   → pypdf extracts text, split into passages, save to DB
        """
        session = self.repo.get_session(upload_id)
        if not session:
            return

        self.repo.set_status(upload_id, "assembling")
        try:
            assembled_path = self.storage.assemble(upload_id, session.total_chunks)

            self.repo.set_status(upload_id, "indexing")
            pdf_bytes = self.storage.read_file(assembled_path)
            full_text = self._extract_text(pdf_bytes)

            passages = (
                self._split_text(full_text, TEXT_CHUNK_CHARS, OVERLAP_CHARS)
                if full_text
                else ["[no extractable text — scanned or image-only PDF]"]
            )
            # Bulk insert — one transaction for all passages instead of one per passage.
            # For a 500-passage book: 1 commit vs 500 commits.
            self.repo.bulk_save_text_chunks([
                PDFChunk(
                    upload_id     = upload_id,
                    filename      = session.filename,
                    passage_index = i,
                    content       = text,
                )
                for i, text in enumerate(passages)
            ])

            self.repo.set_status(upload_id, "indexed")

        except Exception:
            self.repo.set_status(upload_id, "failed")
            raise

    def get_status(self, upload_id: str) -> dict:
        session = self.repo.get_session(upload_id)
        if not session:
            raise ValueError(f"Unknown upload_id: {upload_id}")
        return {
            "upload_id": upload_id,
            "filename":  session.filename,
            "received":  session.received,
            "total":     session.total_chunks,
            "status":    session.status,
            "done":      session.status == "indexed",
        }

    def search(self, query: str, limit: int = 20) -> List[PDFChunk]:
        return self.repo.search(query, limit)

    def clear_all(self) -> int:
        return self.repo.delete_all()

    def _extract_text(self, data: bytes) -> str:
        """
        pymupdf (fitz) is 10-20x faster than pypdf for text extraction.
        It uses MuPDF under the hood — a C library optimised for PDF rendering.
        Fallback returns empty string so the caller can handle gracefully.
        """
        try:
            doc = fitz.open(stream=data, filetype="pdf")
            return "\n".join(page.get_text() for page in doc).strip()
        except Exception:
            return ""

    def _split_text(self, text: str, size: int, overlap: int) -> List[str]:
        chunks, start = [], 0
        while start < len(text):
            chunks.append(text[start:start + size])
            start += size - overlap
        return chunks
