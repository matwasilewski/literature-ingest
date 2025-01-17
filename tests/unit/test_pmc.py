from literature_ingest.models import ArticleType, Section
import pytest
from unittest.mock import Mock
from literature_ingest.pmc import (
    PMCFTPClient, PMCParser, Document, DocumentId, Author,
    JournalMetadata, PublicationDates
)
import json

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

def test_parse_doc_sections(pmc_doc):
    """Test document section parsing"""
    parser = PMCParser()
    doc = parser.parse_doc(pmc_doc)

    # Test that sections were extracted
    assert len(doc.sections) > 0

    # Test first section (Introduction)
    intro = doc.sections[0]
    assert intro.label == "1"
    assert intro.title == "Introduction"
    assert intro.id == "sec1"
    assert "Pulmonary hypertension (PH) is defined as" in intro.text

    # Test Methods section and its subsections
    methods = doc.sections[1]
    assert methods.label == "2"
    assert methods.title == "Methods"
    assert methods.id == "sec2"
    assert len(methods.subsections) > 0

    # Test first methods subsection
    study_design = methods.subsections[0]
    assert study_design.label == "2.1"
    assert study_design.title == "Study design"
    assert "We performed a single center retrospective cohort study" in study_design.text

def test_parse_doc_section_hierarchy(pmc_doc):
    """Test that section hierarchy is correctly preserved"""
    parser = PMCParser()
    doc = parser.parse_doc(pmc_doc)

    # Find the Methods section
    methods = next(s for s in doc.sections if s.title == "Methods")

    # Test subsection structure
    assert len(methods.subsections) == 4  # Methods has 4 subsections
    subsection_titles = [s.title for s in methods.subsections]
    assert "Study design" in subsection_titles
    assert "Patient identification, classification and data collection" in subsection_titles
    assert "Outcomes studied" in subsection_titles
    assert "Statistical analysis" in subsection_titles

def test_parse_doc_section_content(pmc_doc):
    """Test that section content is correctly extracted"""
    parser = PMCParser()
    doc = parser.parse_doc(pmc_doc)

    # Find the Discussion section
    discussion = next(s for s in doc.sections if s.title == "Discussion")

    # Test section content
    assert discussion.label == "4"
    assert "In this large cohort of pregnant women with PH" in discussion.text

    # Test that subsection content is preserved
    limitations = next(s for s in discussion.subsections if s.title == "Limitations")
    assert "We recognize this data has limitations" in limitations.text

def test_document_to_raw_text(pmc_doc):
    """Test Document.to_raw_text() method"""
    parser = PMCParser()
    doc = parser.parse_doc(pmc_doc)

    raw_text = doc.to_raw_text()

    # Check that components are present in correct order
    lines = raw_text.split("\n\n")

    # Title should be first
    assert lines[0] == doc.title

    # Abstract should be second
    assert "Pulmonary hypertension (PH) due to left heart disease" in lines[1]

    # Main text should follow
    assert "Pulmonary hypertension (PH) is defined as" in raw_text  # From Introduction
    assert "We performed a single center retrospective cohort study" in raw_text  # From Methods
    assert "We recognize this data has limitations" in raw_text  # From Discussion

    # Check that section titles are not included in raw text
    assert "1. Introduction" not in raw_text
    assert "2. Methods" not in raw_text

def test_document_to_json(pmc_doc):
    """Test Document.to_json() method"""
    parser = PMCParser()
    doc = parser.parse_doc(pmc_doc)

    json_str = doc.to_json()

    # Verify it's valid JSON
    json_data = json.loads(json_str)

    # Check core fields
    assert json_data["id"]["id"] == "PMC10335194"
    assert json_data["id"]["type"] == "pmc"
    assert json_data["title"] == doc.title
    assert json_data["type"] == "Research Article"

    # Check nested structures
    assert len(json_data["authors"]) == 9
    assert json_data["authors"][0]["name"] == "Marshall V, William H."
    assert json_data["authors"][0]["is_corresponding"] is True

    # Check sections
    assert len(json_data["sections"]) > 0
    first_section = json_data["sections"][0]
    assert first_section["title"] == "Introduction"
    assert first_section["label"] == "1"

    # Check dates
    assert json_data["publication_dates"]["received_date"] == "2021-3-23"
    assert json_data["year"] == 2022

def test_document_to_raw_text_minimal():
    """Test Document.to_raw_text() with minimal document"""
    doc = Document(
        id=DocumentId(id="TEST123", type="test"),
        title="Test Title",
        abstract="Test abstract",
        sections=[
            Section(
                title="Section 1",
                text="Main text",
                subsections=[
                    Section(
                        title="Subsection 1.1",
                        text="Subsection text"
                    )
                ]
            )
        ]
    )

    raw_text = doc.to_raw_text()
    lines = raw_text.split("\n\n")

    assert lines[0] == "Test Title"
    assert lines[1] == "Test abstract"
    assert "Main text Subsection text" in lines[2]  # Section texts should be combined
    assert "Section 1" not in raw_text  # Section titles should not be included
    assert "Subsection 1.1" not in raw_text  # Subsection titles should not be included

def test_document_to_json_minimal():
    """Test Document.to_json() with minimal document"""
    doc = Document(
        id=DocumentId(id="TEST123", type="test"),
        title="Test Title"
    )

    json_str = doc.to_json()
    json_data = json.loads(json_str)

    assert json_data["id"]["id"] == "TEST123"
    assert json_data["id"]["type"] == "test"
    assert json_data["title"] == "Test Title"
    assert json_data["sections"] == []
    assert json_data["keywords"] == []
    assert json_data["authors"] == []
    assert json_data["abstract"] is None

def test_document_load_from_json():
    """Test Document.from_json() method"""
    # Create a minimal document
    original_doc = Document(
        id=DocumentId(id="TEST123", type="test"),
        title="Test Title",
        abstract="Test abstract",
        sections=[
            Section(
                title="Section 1",
                text="Main text",
                subsections=[
                    Section(
                        title="Subsection 1.1",
                        text="Subsection text"
                    )
                ]
            )
        ],
        authors=[
            Author(
                name="Smith, John",
                email="john@example.com",
                affiliations=["University A", "Institute B"],
                is_corresponding=True
            )
        ],
        journal=JournalMetadata(
            title="Test Journal",
            issn="1234-5678",
            publisher="Test Publisher",
            abbreviation="Test J"
        ),
        publication_dates=PublicationDates(
            received_date="2023-01-01",
            accepted_date="2023-02-01",
            epub_date="2023-03-01"
        )
    )

    # Convert to JSON
    json_str = original_doc.to_json()

    # Load back from JSON
    loaded_doc = Document.model_validate_json(json_str)

    # Verify core fields
    assert loaded_doc.id == original_doc.id
    assert loaded_doc.title == original_doc.title
    assert loaded_doc.abstract == original_doc.abstract

    # Verify nested structures
    assert len(loaded_doc.sections) == len(original_doc.sections)
    assert loaded_doc.sections[0].title == original_doc.sections[0].title
    assert loaded_doc.sections[0].text == original_doc.sections[0].text
    assert len(loaded_doc.sections[0].subsections) == len(original_doc.sections[0].subsections)

    # Verify authors
    assert len(loaded_doc.authors) == len(original_doc.authors)
    assert loaded_doc.authors[0].name == original_doc.authors[0].name
    assert loaded_doc.authors[0].email == original_doc.authors[0].email
    assert loaded_doc.authors[0].affiliations == original_doc.authors[0].affiliations
    assert loaded_doc.authors[0].is_corresponding == original_doc.authors[0].is_corresponding

    # Verify journal metadata
    assert loaded_doc.journal.title == original_doc.journal.title
    assert loaded_doc.journal.issn == original_doc.journal.issn
    assert loaded_doc.journal.publisher == original_doc.journal.publisher
    assert loaded_doc.journal.abbreviation == original_doc.journal.abbreviation

    # Verify dates
    assert loaded_doc.publication_dates.received_date == original_doc.publication_dates.received_date
    assert loaded_doc.publication_dates.accepted_date == original_doc.publication_dates.accepted_date
    assert loaded_doc.publication_dates.epub_date == original_doc.publication_dates.epub_date

@pytest.mark.parametrize("pmc_doc_fixture", [
    "pmc_doc_2",
    "pmc_doc_3",
    "pmc_doc_4",
    "pmc_doc_5",
])
def test_parse_doc_with_error(request, pmc_doc_fixture):
    parser = PMCParser()
    doc = parser.parse_doc(request.getfixturevalue(pmc_doc_fixture))
    assert doc
