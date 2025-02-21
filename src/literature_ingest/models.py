from enum import Enum
from typing import Dict, List, Optional, Set
from pydantic import BaseModel
import datetime


class ArticleType(str, Enum):
    """Standardized article types used internally"""
    RESEARCH_ARTICLE = "Research Article"
    REVIEW = "Review"
    CASE_REPORT = "Case Report"
    CASE_STUDY = "Case Study"
    EDITORIAL = "Editorial"
    LETTER = "Letter"
    COMMENT = "Comment"
    NEWS = "News"
    CLINICAL_TRIAL = "Clinical Trial"
    OTHER = "Other"
    CORRECTION = "Correction"
    RETRACTION = "Retraction"
    DATA_PAPER = "Data Paper"
    METHODS_ARTICLE = "Methods Article"
    SYSTEMATIC_REVIEW = "Systematic Review"
    CHAPTER_ARTICLE = "Chapter Article"
    COMMUNITY_COMMENT = "Community Comment"

PMC_ARTICLE_TYPE_MAP = {
    "research-article": ArticleType.RESEARCH_ARTICLE,
    "review-article": ArticleType.REVIEW,
    "case-report": ArticleType.CASE_REPORT,
    "case-study": ArticleType.CASE_STUDY,
    "data-paper": ArticleType.DATA_PAPER,
    "methods-article": ArticleType.METHODS_ARTICLE,
    "systematic-review": ArticleType.SYSTEMATIC_REVIEW,
    "chapter-article": ArticleType.CHAPTER_ARTICLE,
    "community-comment": ArticleType.COMMUNITY_COMMENT,
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

PUBMED_PUBLICATION_TYPE_MAP = {
    "Journal Article": ArticleType.RESEARCH_ARTICLE,
    "Review": ArticleType.REVIEW,
    "Case Reports": ArticleType.CASE_REPORT,
    "Clinical Trial": ArticleType.CLINICAL_TRIAL,
    "Editorial": ArticleType.EDITORIAL,
    "Letter": ArticleType.LETTER,
    "Comment": ArticleType.COMMENT,
    "News": ArticleType.NEWS,
    "Systematic Review": ArticleType.SYSTEMATIC_REVIEW,
    "Meta-Analysis": ArticleType.SYSTEMATIC_REVIEW,
    "Practice Guideline": ArticleType.OTHER,
    "Guideline": ArticleType.OTHER,
    "Retraction of Publication": ArticleType.RETRACTION,
    "Published Erratum": ArticleType.CORRECTION,
    "Clinical Study": ArticleType.CLINICAL_TRIAL,
    "Observational Study": ArticleType.RESEARCH_ARTICLE,
    "Randomized Controlled Trial": ArticleType.CLINICAL_TRIAL,
    "Research Support, U.S. Gov't, P.H.S.": ArticleType.RESEARCH_ARTICLE,
    "Research Support, U.S. Gov't, Non-P.H.S.": ArticleType.RESEARCH_ARTICLE,
    "Research Support, N.I.H., Extramural": ArticleType.RESEARCH_ARTICLE,
    "Research Support, N.I.H., Intramural": ArticleType.RESEARCH_ARTICLE,
    "Research Support, Non-U.S. Gov't": ArticleType.RESEARCH_ARTICLE,
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


class AnnotationType(str, Enum):
    """Standardized annotation types used internally"""
    GENE = "Gene"
    DRUG = "Drug"
    DISEASE = "Disease"


class Annotation(BaseModel):
    """Represents an annotation in the document"""
    start: int
    end: int
    type: AnnotationType

class Section(BaseModel):
    """Represents a section in the document"""
    name: str
    text: str
    annotations: List[Annotation] = []

    class Config:
        extra = "forbid"


class Document(BaseModel):
    """Represents a PMC document with enhanced metadata"""
    # Core identifiers
    ids: List[DocumentId] = []

    # Basic metadata
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
        """
        return "\n\n".join([section.text for section in self.sections]) if self.sections else ""

    @property
    def title(self) -> str:
        """Returns the title of the document"""
        return self.sections[0].text if self.sections else ""
