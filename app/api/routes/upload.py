"""
Upload routes — three endpoints for chunked upload protocol.

POST   /api/upload/start          → register upload, get upload_id
POST   /api/upload/chunk          → send one binary chunk
GET    /api/upload/status/{id}    → poll until status == 'indexed'
DELETE /api/clear                 → wipe everything
"""
from fastapi import APIRouter, Depends, UploadFile, File, Form, HTTPException
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.repositories.chunk_repository import SQLChunkRepository
from app.services.pdf_service import PDFService
from app.storage.storage import get_storage

router = APIRouter()

MAX_CHUNK_BYTES = 10 * 1024 * 1024  # 10 MB per chunk


def _service(db: Session) -> PDFService:
    return PDFService(SQLChunkRepository(db), get_storage())


@router.post("/upload/start")
def start_upload(
    upload_id:    str = Form(...),
    filename:     str = Form(...),
    total_chunks: int = Form(...),
    db: Session = Depends(get_db),
):
    """
    Client calls this once before sending any chunks.
    Creates a session row so the server knows how many chunks to expect.
    """
    if not filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Only PDF files accepted.")
    if total_chunks < 1:
        raise HTTPException(400, "total_chunks must be >= 1.")

    session = _service(db).start_upload(upload_id, filename, total_chunks)
    return {
        "upload_id":    session.upload_id,
        "filename":     session.filename,
        "total_chunks": session.total_chunks,
        "status":       session.status,
    }


@router.post("/upload/chunk")
async def upload_chunk(
    upload_id:   str        = Form(...),
    passage_index: int        = Form(...),
    file:        UploadFile = File(...),
    db: Session = Depends(get_db),
):
    """
    Receive one binary chunk. Can be called in parallel or out of order.
    When the last chunk arrives, reassembly + indexing starts automatically.
    """
    if passage_index < 0:
        raise HTTPException(400, "passage_index must be >= 0.")

    data = await file.read()
    if len(data) == 0:
        raise HTTPException(400, "Empty chunk.")
    if len(data) > MAX_CHUNK_BYTES:
        raise HTTPException(413, f"Chunk too large (max {MAX_CHUNK_BYTES // 1024 // 1024} MB).")

    try:
        result = _service(db).receive_chunk(upload_id, passage_index, data)
    except ValueError as e:
        raise HTTPException(400, str(e))

    return result


@router.get("/upload/status/{upload_id}")
def upload_status(upload_id: str, db: Session = Depends(get_db)):
    """Poll this until done=true."""
    try:
        return _service(db).get_status(upload_id)
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.delete("/clear")
def clear_index(db: Session = Depends(get_db)):
    deleted = _service(db).clear_all()
    return {"status": "ok", "chunks_deleted": deleted}
