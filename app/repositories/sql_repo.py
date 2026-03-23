import logging
from typing import Optional

from sqlalchemy.orm import Session

from app.models.models import PDFUpload, PDFChunk, UploadStatus, ReceivedChunk
from sqlalchemy.exc import IntegrityError
from app.repositories.base import AbstractUploadRepository, AbstractSearchRepository
from app.services.storage import download_text

logger = logging.getLogger(__name__)

CONTEXT_CHARS = 60  # characters on each side of a match in a snippet


def _extract_all_snippets(full_text: str, query: str) -> list[str]:
    """Return a context snippet for every occurrence of query in full_text."""
    snippets = []
    text_lower = full_text.lower()
    query_lower = query.lower()
    query_len = len(query)
    start = 0

    while True:
        pos = text_lower.find(query_lower, start)
        if pos == -1:
            break
        snippet_start = max(0, pos - CONTEXT_CHARS)
        snippet_end = min(len(full_text), pos + query_len + CONTEXT_CHARS)
        snippet = (
            ("..." if snippet_start > 0 else "")
            + full_text[snippet_start:snippet_end]
            + ("..." if snippet_end < len(full_text) else "")
        )
        snippets.append(snippet)
        start = pos + query_len

    return snippets


def _count_occurrences(text: str, query: str) -> int:
    if not text or not query:
        return 0
    return text.lower().count(query.lower())


# ---------------------------------------------------------------------------
# Upload repository
# ---------------------------------------------------------------------------

class SQLUploadRepository(AbstractUploadRepository):

    def __init__(self, db: Session):
        self.db = db

    def create_upload(
        self,
        upload_id: str,
        filename: str,
        total_chunks: int,
        multipart_upload_id: str,
        file_hash: Optional[str] = None,
    ) -> PDFUpload:
        upload = PDFUpload(
            id=upload_id,
            filename=filename,
            file_hash=file_hash,
            total_chunks=total_chunks,
            multipart_upload_id=multipart_upload_id,
        )
        self.db.add(upload)
        self.db.commit()
        self.db.refresh(upload)
        return upload

    def get_upload_by_hash(self, file_hash: str) -> Optional[PDFUpload]:
        return (
            self.db.query(PDFUpload)
            .filter(PDFUpload.file_hash == file_hash, PDFUpload.status == UploadStatus.ready)
            .first()
        )

    def get_upload(self, upload_id: str) -> Optional[PDFUpload]:
        return self.db.query(PDFUpload).filter(PDFUpload.id == upload_id).first()

    def increment_received_chunks(self, upload_id: str, chunk_index: int) -> PDFUpload:
        upload = self.get_upload(upload_id)
        if not upload:
            raise ValueError(f"Upload {upload_id} not found")

        chunk_record = ReceivedChunk(upload_id=upload_id, chunk_index=chunk_index)
        self.db.add(chunk_record)
        try:
            self.db.commit()
        except IntegrityError:
            self.db.rollback()  # duplicate chunk — ignore

        count = (
            self.db.query(ReceivedChunk)
            .filter(ReceivedChunk.upload_id == upload_id)
            .count()
        )
        upload.received_chunks = count
        self.db.commit()
        self.db.refresh(upload)
        return upload

    def set_status(self, upload_id: str, status: UploadStatus) -> PDFUpload:
        upload = self.get_upload(upload_id)
        if not upload:
            raise ValueError(f"Upload {upload_id} not found")
        upload.status = status
        self.db.commit()
        self.db.refresh(upload)
        return upload

    def save_chunk_record(self, upload_id: str, chunk_index: int, r2_key: str) -> PDFChunk:
        chunk = PDFChunk(upload_id=upload_id, chunk_index=chunk_index, r2_key=r2_key)
        self.db.add(chunk)
        self.db.commit()
        self.db.refresh(chunk)
        return chunk


# ---------------------------------------------------------------------------
# Search repository — fetches text from B2, searches in Python
# ---------------------------------------------------------------------------

class SQLSearchRepository(AbstractSearchRepository):

    def __init__(self, db: Session):
        self.db = db

    def save_chunk_text_key(self, chunk_id: str, r2_text_key: str) -> None:
        self.db.query(PDFChunk).filter(PDFChunk.id == chunk_id).update(
            {"r2_text_key": r2_text_key}
        )
        self.db.commit()

    def search(self, query: str, upload_id: Optional[str], limit: int) -> dict:
        """
        Fetch chunk metadata from Postgres, download text from B2,
        and search in Python. No FTS needed — Postgres stays tiny.
        """
        # If a specific upload_id is requested, check its status first
        if upload_id:
            upload = self.db.query(PDFUpload).filter(PDFUpload.id == upload_id).first()
            if not upload:
                return {"total_occurrences": 0, "results": [], "status": "not_found", "message": "Upload not found."}
            if upload.status != UploadStatus.ready:
                extracted_chunks = self.db.query(PDFChunk).filter(PDFChunk.upload_id == upload_id).count()
                pages_so_far = extracted_chunks * 5
                
                msg = f"⏳ Extracting text... roughly {pages_so_far} pages processed so far. Please wait for it to finish."
                
                return {
                    "total_occurrences": 0, 
                    "results": [{
                        "chunk_id": "processing-status",
                        "upload_id": upload.id,
                        "chunk_index": 0,
                        "filename": upload.filename,
                        "snippets": [msg],
                        "occurrences_in_chunk": 0
                    }], 
                    "status": upload.status.value, 
                    "message": msg
                }

        q = (
            self.db.query(PDFChunk.id, PDFChunk.upload_id, PDFChunk.chunk_index,
                          PDFChunk.r2_text_key, PDFUpload.filename)
            .join(PDFUpload, PDFUpload.id == PDFChunk.upload_id)
            .filter(PDFChunk.r2_text_key.isnot(None))
        )
        if upload_id:
            q = q.filter(PDFChunk.upload_id == upload_id)

        # Only search chunks for uploads that are ready
        q = q.filter(PDFUpload.status == UploadStatus.ready)
        chunks = q.all()

        results = []
        total_occurrences = 0

        for chunk in chunks:
            try:
                full_text = download_text(chunk.r2_text_key)
            except Exception as e:
                logger.warning("Failed to download text for chunk %s: %s", chunk.id, e)
                continue

            occ_count = _count_occurrences(full_text, query)
            if occ_count == 0:
                continue

            total_occurrences += occ_count
            snippets = _extract_all_snippets(full_text, query)
            results.append({
                "chunk_id": chunk.id,
                "upload_id": chunk.upload_id,
                "chunk_index": chunk.chunk_index,
                "filename": chunk.filename,
                "snippets": snippets,
                "occurrences_in_chunk": len(snippets),
            })

            if len(results) >= limit:
                break

        return {"total_occurrences": total_occurrences, "results": results}
