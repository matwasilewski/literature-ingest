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

def test_parse_doc_content(pubmed_doc):
    """Test document content parsing"""
    parser = PubMedParser()
    doc = parser._parse_doc_sync(pubmed_doc, Path("test.xml"))

    # Test keywords/MeSH terms
    assert "Formates" in doc.keywords
    assert "Carbon Dioxide" in doc.keywords
    assert "Methanol" in doc.keywords
    assert "Animals" in doc.keywords
    assert "Humans" in doc.keywords

    # Test subject groups
    assert "Journal Article" in doc.subject_groups
    assert "Research Support, U.S. Gov't, P.H.S." in doc.subject_groups

@pytest.mark.asyncio
async def test_parse_doc_pmid_2(pubmed_doc):
    """Test parsing of PMID 2 article with its unique fields"""
    parser = PubMedParser()
    doc = await parser.parse_doc(pubmed_doc, Path("test.xml"))

    # Test core identifiers including PII
    assert doc.ids == [
        DocumentId(id="2", type="pubmed"),
        DocumentId(id="0006-291X(75)90482-9", type="pii"),
        DocumentId(id="10.1016/0006-291x(75)90482-9", type="doi"),
    ]

    # Test basic metadata
    assert doc.title == "Delineation of the intimate details of the backbone conformation of pyridine nucleotide coenzymes in aqueous solution."
    assert doc.type == ArticleType.RESEARCH_ARTICLE

    # Test journal metadata with electronic ISSN
    assert isinstance(doc.journal, JournalMetadata)
    assert doc.journal.title == "Biochemical and biophysical research communications"
    assert doc.journal.issn == "1090-2104"  # Electronic ISSN
    assert doc.journal.abbreviation == "Biochem Biophys Res Commun"

    # Test authors
    assert len(doc.authors) == 2
    assert doc.authors[0].name == "Bose, K S"
    assert doc.authors[1].name == "Sarma, R H"

    # Test keywords/MeSH terms specific to this article
    assert "NAD" in doc.keywords
    assert "NADP" in doc.keywords
    assert "Fourier Analysis" in doc.keywords
    assert "Molecular Conformation" in doc.keywords

    # Test subject groups including non-PHS support
    assert "Journal Article" in doc.subject_groups
    assert "Research Support, U.S. Gov't, Non-P.H.S." in doc.subject_groups
    assert "Research Support, U.S. Gov't, P.H.S." in doc.subject_groups

@pytest.mark.asyncio
async def test_parse_doc_pmid_3(pubmed_doc):
    """Test parsing of PMID 3 article with its unique fields"""
    parser = PubMedParser()
    doc = await parser.parse_doc(pubmed_doc, Path("test.xml"))

    # Test core identifiers
    assert doc.ids == [
        DocumentId(id="3", type="pubmed"),
        DocumentId(id="0006-291X(75)90498-2", type="pii"),
        DocumentId(id="10.1016/0006-291x(75)90498-2", type="doi"),
    ]

    # Test basic metadata
    assert doc.title == "Metal substitutions incarbonic anhydrase: a halide ion probe study."
    assert doc.type == ArticleType.RESEARCH_ARTICLE

    # Test journal metadata with print ISSN
    assert isinstance(doc.journal, JournalMetadata)
    assert doc.journal.title == "Biochemical and biophysical research communications"
    assert doc.journal.issn == "0006-291X"  # Print ISSN
    assert doc.journal.abbreviation == "Biochem Biophys Res Commun"

    # Test authors
    assert len(doc.authors) == 2
    assert doc.authors[0].name == "Smith, R J"
    assert doc.authors[1].name == "Bryant, R G"

    # Test keywords/MeSH terms specific to this article
    assert "Cadmium" in doc.keywords
    assert "Carbonic Anhydrases" in doc.keywords
    assert "Mercury" in doc.keywords
    assert "Zinc" in doc.keywords
    assert "Binding Sites" in doc.keywords
    assert "Protein Binding" in doc.keywords

    # Test subject groups
    assert "Journal Article" in doc.subject_groups
    assert "Research Support, U.S. Gov't, P.H.S." in doc.subject_groups
