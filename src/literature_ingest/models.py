from enum import Enum
from typing import Dict, List, Optional
from pydantic import BaseModel


class ArticleType(str, Enum):
    """Standardized article types used internally"""
    RESEARCH_ARTICLE = "Research Article"
    REVIEW = "Review"
    CASE_REPORT = "Case Report"
    EDITORIAL = "Editorial"
    LETTER = "Letter"
    COMMENT = "Comment"
    NEWS = "News"
    OTHER = "Other"


PMC_ARTICLE_TYPE_MAP = {
    "research-article": ArticleType.RESEARCH_ARTICLE,
    "review-article": ArticleType.REVIEW,
    "case-report": ArticleType.CASE_REPORT,
    "editorial": ArticleType.EDITORIAL,
    "letter": ArticleType.LETTER,
    "article-commentary": ArticleType.COMMENT,
    "news": ArticleType.NEWS,
    None: None,
}


class DocumentId(BaseModel):
    id: str
    type: str


class Author(BaseModel):
    """Represents an author with their name and affiliations"""
    name: str
    email: Optional[str] = None
    affiliations: List[str] = []
    is_corresponding: bool = False


class JournalMetadata(BaseModel):
    """Journal-specific metadata"""
    title: str
    issn: Optional[str] = None
    publisher: Optional[str] = None
    abbreviation: Optional[str] = None  # journal-id with type "nlm-ta" or "iso-abbrev"


class PublicationDates(BaseModel):
    """Various publication dates associated with the article"""
    received_date: Optional[str] = None
    accepted_date: Optional[str] = None
    epub_date: Optional[str] = None
    collection_date: Optional[str] = None


class Document(BaseModel):
    """Represents a PMC document with enhanced metadata"""
    # Core identifiers
    id: DocumentId
    other_ids: Dict[str, str] = {}  # Other IDs like pmid, doi, etc.

    # Basic metadata
    title: str
    type: Optional[ArticleType] = None

    # Journal information
    journal: Optional[JournalMetadata] = None

    # Dates
    year: Optional[int] = None
    publication_dates: PublicationDates = PublicationDates()

    # Content
    abstract: Optional[str] = None
    keywords: List[str] = []

    # Contributors
    authors: List[Author] = []

    # Article categorization
    subject_groups: List[str] = []  # e.g., "Original Article"

    # License information
    license_type: Optional[str] = None
    copyright_statement: Optional[str] = None
    copyright_year: Optional[str] = None
