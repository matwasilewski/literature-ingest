import pytest
from pathlib import Path
from literature_ingest.pubmed import PubMedParser
from literature_ingest.models import ArticleType, Document, DocumentId, Author, JournalMetadata, PublicationDates
from datetime import datetime, timezone

def test_parse_doc_basic_fields(pubmed_doc):
    """Test basic document field parsing"""
    parser = PubMedParser()
    docs = parser.parse_doc(pubmed_doc, Path("test.xml"))
    docs = sorted(docs, key=lambda x: x.ids[0].id)
    doc = docs[0]

    # Test core identifiers
    assert isinstance(doc, Document)
    assert doc.ids == [
        DocumentId(id="1", type="pubmed"),
        DocumentId(id="10.1016/0006-2944(75)90147-7", type="doi"),
    ]
    assert doc.synthetic_id == "type=pubmed;id=1&type=doi;id=10.1016/0006-2944(75)90147-7"

    # Test basic metadata
    assert doc.sections[0].name == "title"
    assert doc.sections[0].text == "Formate assay in body fluids: application in methanol poisoning."
    assert doc.type == ArticleType.RESEARCH_ARTICLE

    assert doc.publication_dates.collection_date == "1975-6"

    assert doc.sections[1].name == "abstract"
    assert doc.sections[1].text == "Formate assay in body fluids: application in methanol poisoning."

    # Test journal metadata
    assert isinstance(doc.journal, JournalMetadata)
    assert doc.journal.title == "Biochemical medicine"
    assert doc.journal.issn == "0006-2944"
    assert doc.journal.abbreviation == "Biochem Med"

def test_parse_doc_authors(pubmed_doc):
    """Test author information parsing"""
    parser = PubMedParser()
    doc = parser.parse_doc(pubmed_doc, Path("test.xml"))
    doc = sorted(doc, key=lambda x: x.ids[0].id)
    doc = doc[0]

    # Test authors list
    assert len(doc.authors) == 4

    # Test author details
    authors = doc.authors
    assert authors[0].name == "Makar, A B"
    assert authors[1].name == "McMartin, K E"
    assert authors[2].name == "Palese, M"
    assert authors[3].name == "Tephly, T R"

def test_parse_doc_dates(pubmed_doc):
    """Test publication dates parsing"""
    parser = PubMedParser()
    docs = parser.parse_doc(pubmed_doc, Path("test.xml"))
    docs = sorted(docs, key=lambda x: x.ids[0].id)
    doc = docs[0]

    assert isinstance(doc.publication_dates, PublicationDates)
    assert doc.year == 1975
    assert doc.publication_dates.collection_date == "1975-6"

def test_parse_doc_content(pubmed_doc):
    """Test document content parsing"""
    parser = PubMedParser()
    docs = parser.parse_doc(pubmed_doc, Path("test.xml"))
    docs = sorted(docs, key=lambda x: x.ids[0].id)
    doc = docs[0]

    # Test keywords/MeSH terms
    assert "Formates" in doc.keywords
    assert "Carbon Dioxide" in doc.keywords
    assert "Methanol" in doc.keywords
    assert "Animals" in doc.keywords
    assert "Humans" in doc.keywords

    # Test subject groups
    assert "Journal Article" in doc.subject_groups
    assert "Research Support, U.S. Gov't, P.H.S." in doc.subject_groups

def test_parse_doc_pmid_2(pubmed_doc):
    """Test document field parsing for PMID 2"""
    parser = PubMedParser()
    docs = parser.parse_doc(pubmed_doc, Path("test.xml"))
    docs = sorted(docs, key=lambda x: x.ids[0].id)
    doc = docs[1]

    # Test core identifiers
    assert isinstance(doc, Document)
    assert doc.ids == [
        DocumentId(id="2", type="pubmed"),
        DocumentId(id="0006-291X(75)90482-9", type="pii"),
        DocumentId(id="10.1016/0006-291x(75)90482-9", type="doi"),
    ]

    # Test basic metadata
    assert doc.title == "Delineation of the intimate details of the backbone conformation of pyridine nucleotide coenzymes in aqueous solution."
    assert doc.type == ArticleType.RESEARCH_ARTICLE  # Default type for Journal Article

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

def test_parse_doc_pmid_3(pubmed_doc):
    """Test document field parsing for PMID 3"""
    parser = PubMedParser()
    docs = parser.parse_doc(pubmed_doc, Path("test.xml"))
    docs = sorted(docs, key=lambda x: x.ids[0].id)
    doc = docs[2]

    # Test core identifiers
    assert doc.ids == [
        DocumentId(id="3", type="pubmed"),
        DocumentId(id="0006-291X(75)90498-2", type="pii"),
        DocumentId(id="10.1016/0006-291x(75)90498-2", type="doi"),
    ]

    # Test basic metadata
    assert doc.title == "Metal substitutions incarbonic anhydrase: a halide ion probe study."
    assert doc.type == ArticleType.RESEARCH_ARTICLE  # Default type for Journal Article

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

def test_parse_doc_pmid_30934(pubmed_doc):
    """Test document field parsing for PMID 30934"""
    parser = PubMedParser()
    docs = parser.parse_doc(pubmed_doc, Path("test.xml"))
    docs = sorted(docs, key=lambda x: x.ids[0].id)
    doc = docs[3]

    # Test core identifiers (only PMID)
    assert doc.ids == [
        DocumentId(id="30934", type="pubmed"),
    ]
    assert doc.synthetic_id == "type=pubmed;id=30934"

    # Test basic metadata
    assert doc.title == "pH and Eh relationships in the body."
    assert doc.type == ArticleType.RESEARCH_ARTICLE  # Default type for Journal Article
    assert doc.year == 1976  # From PubDate MedlineDate

    # Test journal metadata
    assert isinstance(doc.journal, JournalMetadata)
    assert doc.journal.title == "PDM: Physicians' drug manual"
    assert doc.journal.issn == "0031-9058"  # Print ISSN
    assert doc.journal.abbreviation == "PDM"

    # Test authors
    assert len(doc.authors) == 1
    assert doc.authors[0].name == "Chapman, G H"

    # Test publication dates
    assert doc.publication_dates.collection_date == "1976"  # From MedlineDate

    # Test abstract
    expected_abstract = "This report concerns application of the graphical method for representing pH and Eh relationships in macromolecular systems (see previous paper) to in vivo studies. The author presents reasons for concluding that controlled measurements of urine are satisfactory indicators of changes in pH and Eh in the body whereas blood studies remain relatively constant. The original concept had to be modified because of two little known \"reversing phenomena\". One is well known to physicians as the \"acid rebound\" because of the acid reaction of urine when an excess of a base is administered. This is a paradox because it would be expected to be more alkaline. The second phenomenon occurs following hyperoxidation, such as in narcotic addiction, and results in reduction. Both hyperalkalinity and hyperoxidation result in an acid reaction. The author concludes that they are phases of a single phenomenon. It is the basis for \"Chapman's law\": Unfavorable effects on the body cause the urine pH and Eh to shift away from normal whereas favorable effects cause them to shift toward normal."
    assert doc.abstract == expected_abstract

    # Test keywords/MeSH terms
    assert len(doc.keywords) == 5
    assert "Acid-Base Equilibrium" in doc.keywords
    assert "Humans" in doc.keywords
    assert "Hydrogen-Ion Concentration" in doc.keywords
    assert "Models, Biological" in doc.keywords
    assert "Oxidation-Reduction" in doc.keywords

    # Test subject groups
    assert len(doc.subject_groups) == 1
    assert "Journal Article" in doc.subject_groups

    # Test parsed_date is present and is a datetime
    assert isinstance(doc.parsed_date, datetime)
    assert doc.parsed_date.tzinfo == timezone.utc  # Verify timezone is UTC
