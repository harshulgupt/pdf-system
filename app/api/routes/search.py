"""
Search route — query text across all ingested PDF chunks.

GET /api/search?q=invoice&limit=10

Caching note (for interview):
  Currently no cache. In production, add Redis with a short TTL (e.g. 60s)
  in front of this endpoint — search results for the same query are
  expensive (full-table LIKE scan) and usually stable.
  Cache key = hash(query + limit).
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import List

from app.db.database import get_db
from app.repositories.chunk_repository import SQLChunkRepository
from app.services.pdf_service import PDFService
from app.storage.storage import get_storage

router = APIRouter()


@router.get("/search")
def search(
    q: str = Query(..., min_length=1, max_length=500, description="Search query"),
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
                # Return a snippet, not the full chunk (could be large)
                "snippet": r.content[:400] + ("…" if len(r.content) > 400 else ""),
                "created_at": r.created_at,
            }
            for r in results
        ],
    }
