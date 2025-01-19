import pytest
from pathlib import Path
from literature_ingest.pubmed import PubMedParser
from literature_ingest.models import ArticleType, Document, DocumentId, Author, JournalMetadata, PublicationDates

@pytest.mark.asyncio
async def test_parse_doc_basic_fields(pubmed_doc):
    """Test basic document field parsing"""
    parser = PubMedParser()
    doc = await parser.parse_doc(pubmed_doc, Path("test.xml"))

    # Test core identifiers
    assert isinstance(doc, Document)
    assert doc.ids == [
        DocumentId(id="1", type="pubmed"),
        DocumentId(id="10.1016/0006-2944(75)90147-7", type="doi"),
    ]
    assert doc.synthetic_id == "type=pubmed;id=1&type=doi;id=10.1016/0006-2944(75)90147-7"

    # Test basic metadata
    assert doc.title == "Formate assay in body fluids: application in methanol poisoning."
    assert doc.type == ArticleType.RESEARCH_ARTICLE

    # Test journal metadata
    assert isinstance(doc.journal, JournalMetadata)
    assert doc.journal.title == "Biochemical medicine"
    assert doc.journal.issn == "0006-2944"
    assert doc.journal.abbreviation == "Biochem Med"

@pytest.mark.asyncio
async def test_parse_doc_authors(pubmed_doc):
    """Test author information parsing"""
    parser = PubMedParser()
    doc = await parser.parse_doc(pubmed_doc, Path("test.xml"))

    # Test authors list
    assert len(doc.authors) == 4

    # Test author details
    authors = doc.authors
    assert authors[0].name == "Makar, A B"
    assert authors[1].name == "McMartin, K E"
    assert authors[2].name == "Palese, M"
    assert authors[3].name == "Tephly, T R"

@pytest.mark.asyncio
async def test_parse_doc_dates(pubmed_doc):
    """Test publication dates parsing"""
    parser = PubMedParser()
    doc = await parser.parse_doc(pubmed_doc, Path("test.xml"))

    assert isinstance(doc.publication_dates, PublicationDates)
    assert doc.year == 1975
    assert doc.publication_dates.collection_date == "1975-6"

@pytest.mark.asyncio
async def test_parse_doc_content(pubmed_doc):
    """Test document content parsing"""
    parser = PubMedParser()
    doc = await parser.parse_doc(pubmed_doc, Path("test.xml"))

    # Test keywords/MeSH terms
    assert "Formates" in doc.keywords
    assert "Carbon Dioxide" in doc.keywords
    assert "Methanol" in doc.keywords
    assert "Animals" in doc.keywords
    assert "Humans" in doc.keywords

    # Test subject groups
    assert "Journal Article" in doc.subject_groups
    assert "Research Support, U.S. Gov't, P.H.S." in doc.subject_groups
