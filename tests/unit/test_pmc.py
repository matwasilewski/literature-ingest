from pathlib import Path
from literature_ingest.models import ArticleType, Section
import pytest
from unittest.mock import Mock
from literature_ingest.pmc import (
    PMC_OPEN_ACCESS_NONCOMMERCIAL_XML_DIR, PMCFTPClient, PMCParser, Document, DocumentId, Author,
    JournalMetadata, PublicationDates
)
import json
from datetime import datetime, timezone

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
    doc = parser.parse_doc(pmc_doc, Path("test.xml"))

    # Test core identifiersSYT
    assert isinstance(doc, Document)
    assert doc.ids == [
        DocumentId(id="10.1016/j.ijcchd.2022.100354", type="doi"),
        DocumentId(id="PMC10335194", type="pmc"),
        DocumentId(id="37435574", type="pmid"),
        DocumentId(id="S2666-6685(22)00037-4", type="pii"),
        DocumentId(id="100354", type="publisher-id"),
    ]
    assert doc.synthetic_id == "type=doi;id=10.1016/j.ijcchd.2022.100354&type=pmc;id=PMC10335194&type=pmid;id=37435574&type=pii;id=S2666-6685(22)00037-4"

    # Test basic metadata
    assert doc.sections[0].name == "title"
    assert doc.sections[0].text == "Maternal and fetal outcomes in pregnant women with pulmonary hypertension: The impact of left heart disease"
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
    doc = parser.parse_doc(pmc_doc, Path("test.xml"))

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
    doc = parser.parse_doc(pmc_doc, Path("test.xml"))

    assert isinstance(doc.publication_dates, PublicationDates)
    assert doc.year == 2022
    assert doc.publication_dates.received_date == "2021-3-23"
    assert doc.publication_dates.accepted_date == "2022-2-21"
    assert doc.publication_dates.epub_date == "2022-2-24"
    assert doc.publication_dates.collection_date == "2022-6"

def test_parse_doc_content(pmc_doc):
    """Test document content parsing"""
    parser = PMCParser()
    doc = parser.parse_doc(pmc_doc, Path("test.xml"))

    # Test abstract
    assert "Pulmonary hypertension (PH) due to left heart disease" in doc.sections[1].text

    # Test keywords
    assert "Pregnancy" in doc.keywords
    assert "Pulmonary hypertension" in doc.keywords
    assert "Heart failure" in doc.keywords

    # Test subject groups
    assert "Original Article" in doc.subject_groups

def test_parse_doc_license(pmc_doc):
    """Test license and copyright information parsing"""
    parser = PMCParser()
    doc = parser.parse_doc(pmc_doc, Path("test.xml"))

    assert doc.license_type == "https://creativecommons.org/licenses/by-nc-nd/4.0/"
    assert "© 2022 The Authors" in doc.copyright_statement
    assert doc.copyright_year == "2022"

def test_parse_doc_sections(pmc_doc):
    """Test document section parsing"""
    parser = PMCParser()
    doc = parser.parse_doc(pmc_doc, Path("test.xml"))

    # Test that sections were extracted
    assert len(doc.sections) > 0

    # Test first section (Introduction)
    intro = doc.sections[2]
    assert intro.name == "Introduction"
    assert "Introduction\nPulmonary hypertension (PH) is defined as" in intro.text

    # Test Methods section and its subsections
    methods = doc.sections[3]
    assert methods.name == "Methods"

    assert "Methods\nWe performed a single center retrospective cohort study" in methods.text
    assert "Study design" in methods.text
    assert "Patient identification, classification and data collection" in methods.text

def test_parse_doc_section_hierarchy(pmc_doc):
    """Test that section hierarchy is correctly preserved"""
    parser = PMCParser()
    doc = parser.parse_doc(pmc_doc, Path("test.xml"))

    # Find the Methods section
    methods = next(s for s in doc.sections if s.name == "Methods")

    # Test subsection structure
    assert "Study design" in methods.text
    assert "Patient identification, classification and data collection" in methods.text
    assert "Outcomes studied" in methods.text
    assert "Statistical analysis" in methods.text

def test_parse_doc_section_content(pmc_doc):
    """Test that section content is correctly extracted"""
    parser = PMCParser()
    doc = parser.parse_doc(pmc_doc, Path("test.xml"))

    # Find the Discussion section
    discussion = next(s for s in doc.sections if s.name == "Discussion")

    # Test section content
    assert discussion.name == "Discussion"
    assert "In this large cohort of pregnant women with PH" in discussion.text

    # Test that subsection content is preserved
    assert "We recognize this data has limitations" in discussion.text

def test_document_to_raw_text(pmc_doc):
    """Test Document.to_raw_text() method"""
    parser = PMCParser()
    doc = parser.parse_doc(pmc_doc, Path("test.xml"))

    raw_text = doc.to_raw_text()

    # Check that components are present in correct order
    lines = raw_text.split("\n\n")

    # Title should be first
    assert lines[0] == doc.sections[0].text

    # Abstract should be second
    assert "Pulmonary hypertension (PH) due to left heart disease" in lines[1]

    # Main text should follow
    assert "Introduction\nPulmonary hypertension (PH) is defined as" in raw_text  # From Introduction
    assert "Methods\nWe performed a single center retrospective cohort study" in raw_text  # From Methods
    assert "Discussion\nIn this large cohort of pregnant women with PH" in raw_text  # From Discussion

    # Check that section titles are not included in raw text
    assert "1. Introduction" not in raw_text
    assert "2. Methods" not in raw_text

def test_document_to_json(pmc_doc):
    """Test Document.to_json() method"""
    parser = PMCParser()
    doc = parser.parse_doc(pmc_doc, Path("test.xml"))

    json_str = doc.to_json()

    # Verify it's valid JSON
    json_data = json.loads(json_str)

    # Check core fields
    assert json_data["synthetic_id"] == doc.synthetic_id
    assert json_data["ids"] == [
        {"id": "10.1016/j.ijcchd.2022.100354", "type": "doi"},
        {"id": "PMC10335194", "type": "pmc"},
        {"id": "37435574", "type": "pmid"},
        {"id": "S2666-6685(22)00037-4", "type": "pii"},
        {"id": "100354", "type": "publisher-id"},
    ]
    assert json_data["sections"][0]["name"] == "title"
    assert json_data["sections"][0]["text"] == "Maternal and fetal outcomes in pregnant women with pulmonary hypertension: The impact of left heart disease"
    assert json_data["type"] == "Research Article"

    # Check nested structures
    assert len(json_data["authors"]) == 9
    assert json_data["authors"][0]["name"] == "Marshall V, William H."
    assert json_data["authors"][0]["is_corresponding"] is True

    # Check sections
    assert len(json_data["sections"]) > 0
    first_section = json_data["sections"][2]
    assert first_section["name"] == "Introduction"

    # Check dates
    assert json_data["publication_dates"]["received_date"] == "2021-3-23"
    assert json_data["year"] == 2022

def test_doc_2(pmc_doc_2):
    parser = PMCParser()
    doc = parser.parse_doc(pmc_doc_2, Path("test.xml"))
    assert doc

    assert doc.sections[0].name == "title"
    assert doc.sections[0].text == "Co-regulation of intragenic microRNA miR-153 and its host gene Ia-2β: identification of miR-153 target genes with functions related to IA-2β in pancreas and brain"
    assert doc.sections[1].name == "abstract"
    assert doc.sections[1].text.startswith("We analysed the genomic organisation of miR-153")
    assert doc.keywords == ["Diabetes", "Glucose stimulation", "IA-2β", "MicroRNA", "miR-153", "Neurodegeneration"]
    assert doc.raw_type == "research-article"
    assert doc.type == "Research Article"
    assert doc.journal.title == "Diabetologia"
    assert doc.journal.issn == "0012-186X"
    assert doc.journal.publisher == "Springer-Verlag"
    assert doc.journal.abbreviation == "Diabetologia"
    assert doc.year == 2013
    assert doc.publication_dates.received_date == "2013-1-23"
    assert doc.publication_dates.accepted_date == "2013-2-26"
    assert doc.publication_dates.epub_date == "2013-4-18"
    assert doc.sections[2].name == "Introduction"
    assert doc.sections[2].text.startswith("Introduction\nIslet-associated protein (IA) 2 and IA-2β are major autoantigens in type 1 diabetes [1].")

def test_document_to_raw_text_minimal():
    """Test Document.to_raw_text() with minimal document"""
    doc = Document(
        ids=[DocumentId(id="TEST123", type="test")],
        sections=[
            Section(
                name="title",
                text="Test Title",
            ),
            Section(
                name="abstract",
                text="Test abstract",
            ),
            Section(
                name="Section 1",
                text="Section 1\nMain text\n\nSubsection 1.1\nSubsection text",
            )
        ]
    )

    raw_text = doc.to_raw_text()
    lines = raw_text.split("\n\n")

    assert lines[0] == "Test Title"
    assert lines[1] == "Test abstract"
    assert lines[2] == "Section 1\nMain text"
    assert lines[3] == "Subsection 1.1\nSubsection text"

def test_document_to_json_minimal():
    """Test Document.to_json() with minimal document"""
    doc = Document(
        ids=[DocumentId(id="TEST123", type="test")],
        sections=[
            Section(
                name="title",
                text="Test Title",
            ),
            Section(
                name="abstract",
                text="Test abstract",
            ),
        ]
    )

    json_str = doc.to_json()
    json_data = json.loads(json_str)

    assert json_data["synthetic_id"] == doc.synthetic_id
    assert json_data["ids"] == [
        {"id": "TEST123", "type": "test"},
    ]
    assert json_data["sections"][0]["name"] == "title"
    assert json_data["sections"][0]["text"] == "Test Title"
    assert json_data["keywords"] == []
    assert json_data["authors"] == []
    assert json_data["sections"][1]["name"] == "abstract"
    assert json_data["sections"][1]["text"] == "Test abstract"

def test_document_load_from_json():
    """Test that a document can be loaded from JSON and maintains all properties"""
    # Create a sample document with all fields populated
    original_doc = Document(
        ids=[
            DocumentId(id="123", type="pmc"),
            DocumentId(id="10.1234/abc", type="doi")
        ],
        raw_type="research-article",
        type=ArticleType.RESEARCH_ARTICLE,
        journal=JournalMetadata(
            title="Test Journal",
            issn="1234-5678",
            publisher="Test Publisher",
            abbreviation="Test J"
        ),
        year=2023,
        publication_dates=PublicationDates(
            received_date="2023-01-01",
            accepted_date="2023-02-01",
            epub_date="2023-03-01"
        ),
        keywords=["test", "document"],
        authors=[
            Author(
                name="Smith, John",
                email="john@test.com",
                affiliations=["University of Test"],
                is_corresponding=True
            )
        ],
        subject_groups=["Test Group"],
        sections=[
            Section(
                name="title",
                text="Test Title",
            ),
            Section(
                name="abstract",
                text="Test abstract",
            ),
            Section(
                name="Introduction",
                text="Test introduction text",
            )
        ],
        license_type="CC-BY-4.0",
        copyright_statement="Copyright 2023",
        copyright_year="2023",
        parsed_date=datetime.now(timezone.utc)
    )

    # Convert to JSON and back
    json_str = original_doc.model_dump_json()
    loaded_doc = Document.model_validate_json(json_str)

    # Verify all fields are preserved
    assert loaded_doc.ids == original_doc.ids
    assert loaded_doc.sections[0].name == "title"
    assert loaded_doc.sections[0].text == "Test Title"
    assert loaded_doc.sections[1].name == "abstract"
    assert loaded_doc.sections[1].text == "Test abstract"
    assert loaded_doc.sections[2].name == "Introduction"
    assert loaded_doc.sections[2].text == "Test introduction text"
    assert loaded_doc.raw_type == original_doc.raw_type
    assert loaded_doc.type == original_doc.type

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
    "pmc_doc_6",
    "pmc_doc_7",
    "pmc_doc_8",
    "pmc_doc_9",
    "pmc_doc_10",
])
def test_parse_doc_with_error(request, pmc_doc_fixture):
    parser = PMCParser()
    doc = parser.parse_doc(request.getfixturevalue(pmc_doc_fixture), Path(f"{pmc_doc_fixture}.xml"))
    # Create output filename based on fixture name
    output_filename = f"{pmc_doc_fixture}.json"
    with open(f"tests/resources/json_versions/{output_filename}", "w") as f:
        f.write(doc.to_json())
    assert doc
