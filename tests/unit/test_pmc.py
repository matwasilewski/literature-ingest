import pytest
from unittest.mock import Mock
from literature_ingest.pmc import PMCFTPClient, PMCParser, Document, DocumentId

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
    parser = PMCParser()
    doc = parser.parse_doc(pmc_doc)

    assert isinstance(doc, Document)
    assert doc.id == DocumentId(id="PMC10335194", type="pmc")
    assert doc.title == "Maternal and fetal outcomes in pregnant women with pulmonary hypertension: The impact of left heart disease"
    assert doc.journal == "International Journal of Cardiology Congenital Heart Disease"
    assert doc.year == 2022
    assert doc.type == "research-article"
    assert len(doc.authors) == 9
    assert "Marshall V, William H." in doc.authors
    assert "Bradley, Elisa A." in doc.authors

    # Check that abstract contains expected text
    assert "Pulmonary hypertension (PH) due to left heart disease" in doc.abstract
