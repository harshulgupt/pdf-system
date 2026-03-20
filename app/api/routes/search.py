from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List

from app.db.database import get_db
from app.db.models import PDFChunk
from app.repositories.chunk_repository import SQLChunkRepository
from app.services.pdf_service import PDFService
from app.storage.storage import get_storage

router = APIRouter()


@router.get("/search")
def search(
    q: str = Query(..., min_length=1, max_length=500),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    if not q.strip():
        raise HTTPException(400, "Query cannot be blank.")

    repo = SQLChunkRepository(db)
    service = PDFService(repo, get_storage())
    results = service.search(q.strip(), limit)

    return {
        "query": q,
        "total": len(results),
        "results": [
            {
                "chunk_id": r.id,
                "file_id": r.file_id,
                "filename": r.filename,
                "chunk_index": r.chunk_index,
                "snippet": r.content[:400] + ("…" if len(r.content) > 400 else ""),
                "created_at": r.created_at,
            }
            for r in results
        ],
    }


@router.get("/debug")
def debug(db: Session = Depends(get_db)):
    """
    Shows what's currently stored — use this to verify uploads worked.
    Hit /api/debug in your browser after uploading a PDF.
    """
    rows = db.query(PDFChunk).all()
    return {
        "total_chunks": len(rows),
        "files": [
            {
                "file_id": r.file_id,
                "filename": r.filename,
                "chunk_index": r.chunk_index,
                "content_length": len(r.content),
                "content_preview": r.content[:200],
            }
            for r in rows
        ],
    }
