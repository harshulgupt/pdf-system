from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.db.models import PDFChunk
from app.repositories.chunk_repository import SQLChunkRepository
from app.services.pdf_service import PDFService
from app.storage.storage import get_storage

router = APIRouter()


@router.get("/search")
def search(
    q:     str = Query(..., min_length=1, max_length=500),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    results = PDFService(SQLChunkRepository(db), get_storage()).search(q.strip(), limit)
    return {
        "query":   q,
        "total":   len(results),
        "results": [
            {
                "chunk_id":    r.id,
                "upload_id":   r.upload_id,
                "filename":    r.filename,
                "chunk_index": r.chunk_index,
                "snippet":     r.content[:400] + ("…" if len(r.content) > 400 else ""),
            }
            for r in results
        ],
    }


@router.get("/debug")
def debug(db: Session = Depends(get_db)):
    rows = db.query(PDFChunk).all()
    return {
        "total_text_chunks": len(rows),
        "chunks": [
            {
                "filename":       r.filename,
                "chunk_index":    r.chunk_index,
                "content_length": len(r.content),
                "preview":        r.content[:150],
            }
            for r in rows
        ],
    }
