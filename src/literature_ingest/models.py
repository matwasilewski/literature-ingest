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
    sections: List[Section] = []  # Main sections of the document

    # Contributors
    authors: List[Author] = []

    # Article categorization
    subject_groups: List[str] = []  # e.g., "Original Article"

    # License information
    license_type: Optional[str] = None
    copyright_statement: Optional[str] = None
    copyright_year: Optional[str] = None

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
