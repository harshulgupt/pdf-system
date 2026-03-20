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
                "passage_index": r.passage_index,
                "snippet": extract_snippet(r.content, q.strip(), 100),
            }
            for r in results
        ],
    }

def extract_snippet(content: str, query: str, window: int = 100) -> str:
    pos = content.lower().find(query.lower())
    if pos == -1:
        return content[:200] + ("…" if len(content) > 200 else "")
    start = max(0, pos - window)
    end = min(len(content), pos + len(query) + window)
    snippet = content[start:end]
    if start > 0:
        snippet = "…" + snippet
    if end < len(content):
        snippet = snippet + "…"
    return snippet

@router.get("/debug")
def debug(db: Session = Depends(get_db)):
    rows = db.query(PDFChunk).all()
    return {
        "total_text_chunks": len(rows),
        "chunks": [
            {
                "filename":       r.filename,
                "passage_index":    r.passage_index,
                "content_length": len(r.content),
                "preview":        r.content[:150],
            }
            for r in rows
        ],
    }
