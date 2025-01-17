from literature_ingest.models import ArticleType
import pytest
from unittest.mock import Mock
from literature_ingest.pmc import (
    PMCFTPClient, PMCParser, Document, DocumentId, Author,
    JournalMetadata, PublicationDates
)

def test_get_baseline_date_success():
    # Arrange
    client = PMCFTPClient()

    # Act
    result_date, result_files = client.extract_baseline_files([
        "oa_noncomm_xml.PMC000xxxxxx.baseline.2024-12-18.filelist.csv",
        "some_other_file.txt"
    ])

    # Assert
    assert result_date == "2024-12-18"
    assert result_files == ["oa_noncomm_xml.PMC000xxxxxx.baseline.2024-12-18.filelist.csv"]

@pytest.mark.parametrize("file_list", [
    ["oa_noncomm_xml.PMC000xxxxxx.baseline.invalid-date.filelist.csv"]  # invalid date format
])
def test_get_baseline_date_failure(file_list):
    # Arrange
    client = PMCFTPClient()

    # Act/Assert
    with pytest.raises(ValueError, match="Found a file with `baseline` string but no date"):
        client.extract_baseline_files(file_list)

def test_parse_doc_basic_fields(pmc_doc):
    """Test basic document field parsing"""
    parser = PMCParser()
    doc = parser.parse_doc(pmc_doc)

    # Test core identifiers
    assert isinstance(doc, Document)
    assert doc.id == DocumentId(id="PMC10335194", type="pmc")
    assert doc.other_ids["pmid"] == "37435574"
    assert doc.other_ids["doi"] == "10.1016/j.ijcchd.2022.100354"

    # Test basic metadata
    assert doc.title == "Maternal and fetal outcomes in pregnant women with pulmonary hypertension: The impact of left heart disease"
    assert doc.type == ArticleType.RESEARCH_ARTICLE

    # Test journal metadata
    assert isinstance(doc.journal, JournalMetadata)
    assert doc.journal.title == "International Journal of Cardiology Congenital Heart Disease"
    assert doc.journal.issn == "2666-6685"
    assert doc.journal.publisher == "Elsevier"
    assert doc.journal.abbreviation == "Int J Cardiol Congenit Heart Dis"

def test_parse_doc_authors(pmc_doc):
    """Test author information parsing"""
    parser = PMCParser()
    doc = parser.parse_doc(pmc_doc)

    # Test authors list
    assert len(doc.authors) == 9

    # Test first author details
    first_author = doc.authors[0]
    assert isinstance(first_author, Author)
    assert first_author.name == "Marshall V, William H."
    assert first_author.email == "William.marshall@osumc.edu"
    assert first_author.is_corresponding is True
    assert len(first_author.affiliations) == 2
    assert "The Ohio State University Wexner Medical Center" in first_author.affiliations[0]
    assert "Nationwide Children's Hospital" in first_author.affiliations[1]

def test_parse_doc_dates(pmc_doc):
    """Test publication dates parsing"""
    parser = PMCParser()
    doc = parser.parse_doc(pmc_doc)

    assert isinstance(doc.publication_dates, PublicationDates)
    assert doc.year == 2022
    assert doc.publication_dates.received_date == "2021-3-23"
    assert doc.publication_dates.accepted_date == "2022-2-21"
    assert doc.publication_dates.epub_date == "2022-2-24"
    assert doc.publication_dates.collection_date == "2022-6"

def test_parse_doc_content(pmc_doc):
    """Test document content parsing"""
    parser = PMCParser()
    doc = parser.parse_doc(pmc_doc)

    # Test abstract
    assert "Pulmonary hypertension (PH) due to left heart disease" in doc.abstract

    # Test keywords
    assert "Pregnancy" in doc.keywords
    assert "Pulmonary hypertension" in doc.keywords
    assert "Heart failure" in doc.keywords

    # Test subject groups
    assert "Original Article" in doc.subject_groups

def test_parse_doc_license(pmc_doc):
    """Test license and copyright information parsing"""
    parser = PMCParser()
    doc = parser.parse_doc(pmc_doc)

    assert doc.license_type == "https://creativecommons.org/licenses/by-nc-nd/4.0/"
    assert "© 2022 The Authors" in doc.copyright_statement
    assert doc.copyright_year == "2022"

def test_parse_doc_missing_fields():
    """Test parser handles missing fields gracefully"""
    minimal_xml = '''
    <?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE article PUBLIC "-//NLM//DTD JATS (Z39.96) Journal Archiving and Interchange DTD v1.2 20190208//EN" "JATS-archivearticle1.dtd">
    <article>
        <front>
            <journal-meta>
                <journal-title-group>
                    <journal-title>Test Journal</journal-title>
                </journal-title-group>
            </journal-meta>
            <article-meta>
                <article-id pub-id-type="pmc">PMC123456</article-id>
                <title-group>
                    <article-title>Test Title</article-title>
                </title-group>
                <pub-date pub-type="collection">
                    <year>2023</year>
                </pub-date>
            </article-meta>
        </front>
    </article>
    '''

    parser = PMCParser()
    doc = parser.parse_doc(minimal_xml)

    assert doc.id.id == "PMC123456"
    assert doc.title == "Test Title"
    assert doc.year == 2023
    assert doc.abstract == ""
    assert doc.keywords == []
    assert doc.authors == []
    assert doc.subject_groups == []
    assert doc.license_type is None
    assert doc.copyright_statement is None
    assert doc.copyright_year is None
