from sqlalchemy import Column, Integer, String, Text, DateTime
from sqlalchemy.sql import func
from app.db.database import Base

class PDFChunk(Base):
    """
    One row per text chunk extracted from a PDF.
    chunk_index: order within the file
    content: extracted text (searchable)
    storage_path: where the raw chunk bytes live (local fs or S3 key)
    """
    __tablename__ = "pdf_chunks"

    id            = Column(Integer, primary_key=True, index=True)
    file_id       = Column(String, index=True, nullable=False)
    filename      = Column(String, nullable=False)
    chunk_index   = Column(Integer, nullable=False)
    content       = Column(Text, nullable=False)       # extracted text
    storage_path  = Column(String, nullable=False)     # fs path or S3 key
    created_at    = Column(DateTime(timezone=True), server_default=func.now())
