"""
Service Layer — all business logic. No HTTP, no SQLAlchemy imports.

THE CHUNKED UPLOAD FLOW:
  1. Client calls POST /api/upload/start  → gets upload_id, confirms total_chunks
  2. Client slices file into 5MB binary chunks
  3. Client calls POST /api/upload/chunk  for each chunk (any order)
  4. Server saves each binary chunk to disk, increments received counter
  5. When received == total_chunks, server:
       a. Reassembles all binary chunks into one complete PDF file
       b. Extracts text from the complete PDF (pypdf needs the whole file)
       c. Splits extracted text into overlapping text chunks
       d. Saves text chunks to DB for search
  6. Client polls GET /api/upload/status/{upload_id} to know when done
"""
import io
from typing import List

import pypdf

from app.db.models import PDFChunk, UploadSession
from app.repositories.chunk_repository import AbstractChunkRepository
from app.storage.storage import AbstractStorage

TEXT_CHUNK_CHARS = 2000
OVERLAP_CHARS    = 200


class PDFService:
    def __init__(self, repo: AbstractChunkRepository, storage: AbstractStorage):
        self.repo    = repo
        self.storage = storage

    # ── Step 1: start a new upload session ───────────────────────────────────

    def start_upload(self, upload_id: str, filename: str, total_chunks: int) -> UploadSession:
        """Register intent to upload. Client must call this first."""
        return self.repo.create_session(upload_id, filename, total_chunks)

    # ── Step 2: receive one binary chunk ─────────────────────────────────────

    def receive_chunk(self, upload_id: str, passage_index: int, data: bytes) -> dict:
        """
        Save one binary chunk to disk and update the session counter.
        When all chunks have arrived, trigger reassembly + indexing automatically.
        Returns current session state so the client knows progress.
        """
        session = self.repo.get_session(upload_id)
        if not session:
            raise ValueError(f"Unknown upload_id: {upload_id}")
        if session.status not in ("uploading",):
            raise ValueError(f"Session status is '{session.status}', cannot accept more chunks.")

        # Save raw bytes to disk
        self.storage.save_binary_chunk(upload_id, passage_index, data)

        # Increment counter
        session = self.repo.increment_received(upload_id)

        # All chunks received? → assemble + index
        if session.received == session.total_chunks:
            self._assemble_and_index(session)

        return {
            "received": session.received,
            "total":    session.total_chunks,
            "status":   session.status,
            "done":     session.status == "indexed",
        }

    # ── Step 3: poll for completion ──────────────────────────────────────────

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

    # ── Search ────────────────────────────────────────────────────────────────

    def search(self, query: str, limit: int = 20) -> List[PDFChunk]:
        return self.repo.search(query, limit)

    def clear_all(self) -> int:
        return self.repo.delete_all()

    # ── Private: reassembly + indexing ───────────────────────────────────────

    def _assemble_and_index(self, session: UploadSession) -> None:
        """
        Called automatically once all binary chunks are on disk.

        Why reassemble before parsing?
          PDF is a structured binary format. Each 5MB slice is meaningless
          on its own — the cross-reference table, object definitions, and
          stream data span the whole file. pypdf requires the complete file.
          We write all chunks in order into one file, then parse that.
        """
        self.repo.set_status(session.upload_id, "assembling")
        try:
            # Concatenate chunk_000000.bin … chunk_N.bin → assembled.pdf
            assembled_path = self.storage.assemble(session.upload_id, session.total_chunks)

            # Read the complete file and extract text
            pdf_bytes  = self.storage.read_file(assembled_path)
            full_text  = self._extract_text(pdf_bytes)

            # Split text into overlapping chunks and index
            text_chunks = self._split_text(full_text, TEXT_CHUNK_CHARS, OVERLAP_CHARS) if full_text else [
                "[no extractable text — scanned or image-only PDF]"
            ]
            for i, text in enumerate(text_chunks):
                self.repo.save_text_chunk(PDFChunk(
                    upload_id   = session.upload_id,
                    filename    = session.filename,
                    passage_index = i,
                    content     = text,
                ))

            self.repo.set_status(session.upload_id, "indexed")

        except Exception as e:
            self.repo.set_status(session.upload_id, "failed")
            raise

    def _extract_text(self, data: bytes) -> str:
        try:
            reader = pypdf.PdfReader(io.BytesIO(data))
            return "\n".join(p.extract_text() or "" for p in reader.pages).strip()
        except Exception:
            return ""

    def _split_text(self, text: str, size: int, overlap: int) -> List[str]:
        chunks, start = [], 0
        while start < len(text):
            chunks.append(text[start:start + size])
            start += size - overlap
        return chunks
