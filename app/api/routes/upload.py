"""
Upload route — receives the complete PDF and replaces any previous version.

POST /api/upload-chunk   multipart/form-data: file, file_id, filename
DELETE /api/clear        wipes the entire index
"""
from fastapi import APIRouter, Depends, UploadFile, File, Form, HTTPException
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.repositories.chunk_repository import SQLChunkRepository
from app.services.pdf_service import PDFService
from app.storage.storage import get_storage

router = APIRouter()

MAX_FILE_BYTES = 50 * 1024 * 1024  # 50 MB demo limit


@router.post("/upload-chunk")
async def upload_pdf(
    file: UploadFile = File(...),
    file_id: str = Form(...),
    filename: str = Form(...),
    chunk_index: int = Form(default=0),
    db: Session = Depends(get_db),
):
    if not filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Only PDF files are accepted.")

    data = await file.read()
    if len(data) == 0:
        raise HTTPException(400, "Empty file received.")
    if len(data) > MAX_FILE_BYTES:
        raise HTTPException(413, f"File exceeds {MAX_FILE_BYTES // 1024 // 1024} MB demo limit.")

    repo = SQLChunkRepository(db)
    service = PDFService(repo, get_storage())
    chunks = service.ingest_pdf(pdf_data=data, filename=filename, file_id=file_id)

    total_text = sum(len(c.content) for c in chunks)
    searchable = total_text > 0 and "[no extractable" not in chunks[0].content
    return {
        "status": "ok",
        "file_id": file_id,
        "text_chunks_created": len(chunks),
        "total_text_chars": total_text,
        "searchable": searchable,
    }


@router.delete("/clear")
def clear_index(db: Session = Depends(get_db)):
    """Wipe all indexed data so fresh uploads start clean."""
    service = PDFService(SQLChunkRepository(db), get_storage())
    deleted = service.clear_all()
    return {"status": "ok", "chunks_deleted": deleted}
