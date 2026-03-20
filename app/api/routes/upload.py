"""
Upload route — receives one chunk at a time from the client.

Why per-chunk uploads instead of one giant request?
- A 20GB file can't fit in RAM; chunking keeps memory bounded.
- Each chunk can be retried independently on network failure.
- Enables parallel uploads (multiple chunks in flight simultaneously).

Expected multipart/form-data fields:
  file       : the raw chunk bytes
  file_id    : UUID assigned by client (groups chunks for one file)
  filename   : original filename
  chunk_index: 0-based integer
"""
from fastapi import APIRouter, Depends, UploadFile, File, Form, HTTPException
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.repositories.chunk_repository import SQLChunkRepository
from app.services.pdf_service import PDFService
from app.storage.storage import get_storage

router = APIRouter()

MAX_CHUNK_BYTES = 10 * 1024 * 1024  # 10 MB hard limit per chunk request


@router.post("/upload-chunk")
async def upload_chunk(
    file: UploadFile = File(...),
    file_id: str = Form(...),
    filename: str = Form(...),
    chunk_index: int = Form(...),
    db: Session = Depends(get_db),
):
    # --- Basic hardening ---
    if not filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Only PDF files are accepted.")
    if chunk_index < 0:
        raise HTTPException(400, "chunk_index must be non-negative.")

    data = await file.read()
    if len(data) > MAX_CHUNK_BYTES:
        raise HTTPException(413, f"Chunk exceeds {MAX_CHUNK_BYTES} bytes.")
    if len(data) == 0:
        raise HTTPException(400, "Empty chunk received.")

    repo = SQLChunkRepository(db)
    storage = get_storage()
    service = PDFService(repo, storage)

    chunk = service.ingest_chunk(
        chunk_data=data,
        filename=filename,
        file_id=file_id,
        chunk_index=chunk_index,
    )

    return {
        "status": "ok",
        "chunk_id": chunk.id,
        "file_id": file_id,
        "chunk_index": chunk_index,
        "text_length": len(chunk.content),
    }
