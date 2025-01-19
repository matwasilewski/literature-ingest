from enum import Enum
from typing import Dict, List, Optional, Set
from pydantic import BaseModel
import datetime


class ArticleType(str, Enum):
    """Standardized article types used internally"""
    RESEARCH_ARTICLE = "Research Article"
    REVIEW = "Review"
    CASE_REPORT = "Case Report"
    EDITORIAL = "Editorial"
    LETTER = "Letter"
    COMMENT = "Comment"
    NEWS = "News"
    CLINICAL_TRIAL = "Clinical Trial"
    OTHER = "Other"
    CORRECTION = "Correction"
    RETRACTION = "Retraction"


PMC_ARTICLE_TYPE_MAP = {
    "research-article": ArticleType.RESEARCH_ARTICLE,
    "review-article": ArticleType.REVIEW,
    "case-report": ArticleType.CASE_REPORT,
    "editorial": ArticleType.EDITORIAL,
    "letter": ArticleType.LETTER,
    "article-commentary": ArticleType.COMMENT,
    "news": ArticleType.NEWS,
    "other": ArticleType.OTHER,
    "brief-report": ArticleType.CASE_REPORT,
    "reply": ArticleType.LETTER,
    "correction": ArticleType.CORRECTION,
    "protocol": ArticleType.OTHER,
    "discussion": ArticleType.OTHER,
    "in-brief": ArticleType.OTHER,
    "abstract": ArticleType.RESEARCH_ARTICLE,
    "book-review": ArticleType.REVIEW,
    "oration": ArticleType.OTHER,
    "obituary": ArticleType.OTHER,
    "meeting-report": ArticleType.OTHER,
    "retraction": ArticleType.RETRACTION,
    "report": ArticleType.OTHER,
    "calendar": ArticleType.OTHER,
    "announcement": ArticleType.OTHER,
    "collection": ArticleType.OTHER,
    "introduction": ArticleType.OTHER,
    "product-review": ArticleType.REVIEW,
    "addendum": ArticleType.OTHER,
    "rapid-communication": ArticleType.OTHER,
    "product-review": ArticleType.REVIEW,
    "expression-of-concern": ArticleType.OTHER,
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

    def items(self):
        """Returns an iterator of (date_type, date_value) tuples for non-None dates"""
        return [
            (name, value)
            for name, value in self.__dict__.items()
            if value is not None
        ]

class Section(BaseModel):
    """Represents a section in the document"""
    id: Optional[str] = None  # section id if present
    label: Optional[str] = None  # section number/label if present
    title: Optional[str] = None
    text: str
    subsections: List['Section'] = []  # recursive for nested sections


class Document(BaseModel):
    """Represents a PMC document with enhanced metadata"""
    # Core identifiers
    ids: List[DocumentId] = []

    # Basic metadata
    title: str
    raw_type: Optional[str] = None
    type: Optional[ArticleType] = None

    # Computed fields
    synthetic_id: str = ""

    # Journal information
    journal: Optional[JournalMetadata] = None

    # Dates
    year: Optional[int] = None
    publication_dates: PublicationDates = PublicationDates()

    # Content
    abstract: Optional[str] = None
    keywords: List[str] = []
    sections: List[Section] = []  # Main sections of the document

    # Contributors
    authors: List[Author] = []

    # Article categorization
    subject_groups: List[str] = []  # e.g., "Original Article"

    # License information
    license_type: Optional[str] = None
    copyright_statement: Optional[str] = None
    copyright_year: Optional[str] = None

    parsed_date: datetime.datetime = datetime.datetime.now(datetime.timezone.utc).isoformat()

    def __init__(self, **data):
        # Generate synthetic_id from ids before calling parent constructor
        if 'ids' in data and not data.get('synthetic_id'):
            # Filter out publisher-id and sort by type
            filtered_ids = [id for id in data['ids'] if isinstance(id, DocumentId) and id.type != "publisher-id"]
            data['synthetic_id'] = "&".join([f"type={id.type};id={id.id}" for id in filtered_ids])

        super().__init__(**data)

    def to_json(self, indent: int = 2) -> str:
        """Convert document to JSON string"""
        return self.model_dump_json(indent=indent)

    def to_raw_text(self) -> str:
        """Convert document to raw text format.
        Format: title, newline, abstract, newline, sections text
        """
        components = [self.title]

        if self.abstract:
            components.append(self.abstract)

        def process_section(section: Section) -> str:
            section_text = [section.text]
            for subsection in section.subsections:
                section_text.append(process_section(subsection))
            return " ".join(section_text)

        for section in self.sections:
            section_text = process_section(section)
            if section_text.strip():
                components.append(section_text)

        return "\n\n".join(components)
