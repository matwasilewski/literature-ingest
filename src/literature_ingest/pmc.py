#!/usr/bin/env python3

from collections import defaultdict
import ftplib
import sys
from literature_ingest.normalization import normalize_document
from literature_ingest.utils.logging import log
from typing import Dict, List, Optional, Tuple
import re
from cloudpathlib import CloudPath
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
import backoff

from click import Path
from literature_ingest.models import PMC_ARTICLE_TYPE_MAP, ArticleType, Author, Document, DocumentId, JournalMetadata, PublicationDates, Section
from pydantic import BaseModel
import multiprocessing

PMC_FTP_HOST = "ftp.ncbi.nlm.nih.gov"
PMC_OPEN_ACCESS_NONCOMMERCIAL_XML_DIR = '/pub/pmc/oa_bulk/oa_noncomm/xml'
PUBMED_OPEN_ACCESS_DIR = '/pubmed/baseline'

class GenericFTPClient:
    def __init__(self):
        self.host = "FILL_ME_IN"
        self.dir = "FILL_ME_IN"
        self.ftp = None
        self.connect()

    def connect(self) -> None:
        """Establish connection to the PMC FTP server"""
        try:
            self.ftp = ftplib.FTP(self.host)
            self.ftp.login()  # anonymous login
            self.ftp.cwd(self.dir)
            print(f"Connected to {self.host} and changed to {self.dir}")
        except Exception as e:
            print(f"Failed to connect: {str(e)}")
            raise

    def close(self) -> None:
        """Close the FTP connection"""
        if self.ftp:
            self.ftp.quit()
            self.ftp = None
            print("Connection closed")


    def list_directory(self, path: str = '.') -> List[str]:
        """List contents of the specified directory"""
        if not self.ftp:
            raise ConnectionError("Not connected to FTP server")

        files = []
        self.ftp.dir(path, files.append)
        files = [f.split()[-1] for f in files]
        return files


    @backoff.on_exception(backoff.expo, Exception, max_time=120, max_tries=10)
    def download_file(self, remote_file: str, target_path: Path) -> None:
        """Download a file from REMOTE_FILE to TARGET_PATH, from PMC FTP server defined in __init__"""
        if not self.ftp:
            raise ConnectionError("Not connected to FTP server")

        try:
            with target_path.open(mode='wb') as f:
                self.ftp.retrbinary(f'RETR {remote_file}', f.write)
            print(f"Successfully downloaded {remote_file} to {target_path}")
        except Exception as e:
            print(f"Failed to download {remote_file}: {str(e)}")
            raise

    def _download_files(self, files: List[str], base_dir: Path, dry_run: bool = False, overwrite: bool = False) -> List[Path]:
        """Download all files that don't exist locally."""
        target_file_paths = []

        for remote_file in files:
            target_file_path = base_dir / remote_file
            if not target_file_path.exists() or overwrite:
                if not dry_run:
                    print(f"Downloading {remote_file}...")
                    self.download_file(remote_file, target_file_path)
                else:
                    print(f"Would download {remote_file} to {target_file_path}")
                target_file_paths.append(target_file_path)
            else:
                print(f"Skipping {remote_file}")
        return target_file_paths

    def extract_baseline_files(self, files: List[str]) -> Tuple[str, List[str]]:
        """Extract the date from baseline files in the current directory"""
        baseline_dates = set()
        baseline_files = []

        for file in files:
            if 'baseline' in file:
                # Use regex to find date pattern YYYY-MM-DD between dots
                match = re.search(r'\.(\d{4}-\d{2}-\d{2})\.' , file)
                if not match:
                    raise ValueError(f"Found a file with `baseline` string but no date: {file}")

                baseline_dates.add(match.group(1))
                baseline_files.append(file)

        if len(baseline_dates) != 1:
            raise ValueError(f"Exactly one baseline date is expected, found {len(baseline_dates)}: {baseline_dates}")

        return baseline_dates.pop(), baseline_files


    def extract_incremental_files(self, files: List[str]) -> List[str]:
        """Extract the date from incremental files in the current directory"""
        baseline_files = []

        for file in files:
            if '.incr.' in file:
                baseline_files.append(file)

        return baseline_files

class PMCFTPClient(GenericFTPClient):
    def __init__(self):
        self.host = PMC_FTP_HOST
        self.dir = PMC_OPEN_ACCESS_NONCOMMERCIAL_XML_DIR
        self.ftp = None
        self.connect()


    @backoff.on_exception(backoff.expo, Exception, max_time=60, max_tries=5)
    def _download_pmc_incremental(self, base_dir: Path = Path('data/pmc/incremental'), dry_run: bool = False, overwrite: bool = False) -> List[Path]:
        """Download all incremental files that don't exist locally."""
        if not self.ftp:
            raise ConnectionError("Not connected to FTP server")

        # Create base directory if it doesn't exist
        base_dir.mkdir(parents=True, exist_ok=True)

        # Get list of remote files
        raw_file_names = self.list_directory()

        # Extract baseline date
        baseline_date, _ = self.extract_baseline_files(raw_file_names)

        # Create dated directory - it will be data/pmc/incremental/YYYY-MM-DD/*
        dated_dir = base_dir / baseline_date
        dated_dir.mkdir(parents=True, exist_ok=True)

        incremental_files = self.extract_incremental_files(raw_file_names)
        downloaded_files = self._download_files(incremental_files, dated_dir, dry_run=dry_run, overwrite=overwrite)

        return downloaded_files

    @backoff.on_exception(backoff.expo, Exception, max_time=60, max_tries=5)
    def _download_pmc_baselines(self, base_dir: Path = Path('data/pmc/baselines'), dry_run: bool = False, overwrite: bool = False) -> List[Path]:
        """Download all baseline files that don't exist locally.

        Baseline files is a batch of PMC documents that should contain all PMC documents released up to a certain date.
        Baseline is then updated with new PMC documents released after the date in DAILY incremental updates.

        Fundamental logic is that we download whatever baseline file is available, and then we download the daily incremental updates. If keeping up to date with latest PMC documents is important.

        The date is extracted from the baseline file name.
        """
        if not self.ftp:
            raise ConnectionError("Not connected to FTP server")

        # Create base directory if it doesn't exist
        base_dir.mkdir(parents=True, exist_ok=True)

        # Get list of remote files
        raw_file_names = self.list_directory()

        # Extract baseline date and files
        baseline_date, baseline_files = self.extract_baseline_files(raw_file_names)

        # Create dated directory
        dated_dir = base_dir / baseline_date
        dated_dir.mkdir(parents=True, exist_ok=True)

        downloaded_files = self._download_files(baseline_files, dated_dir, dry_run=dry_run, overwrite=overwrite)
        return downloaded_files


    def _download_pmc_baselines_sample(self, base_dir: Path, file_names: List[str], dry_run: bool = False, overwrite: bool = False) -> List[Path]:
        if not self.ftp:
            raise ConnectionError("Not connected to FTP server")

        # Create base directory if it doesn't exist
        base_dir.mkdir(parents=True, exist_ok=True)

        # Get list of remote files
        raw_file_names = file_names

        # Extract baseline date and files
        baseline_date, baseline_files = self.extract_baseline_files(raw_file_names)

        # Create dated directory
        dated_dir = base_dir / baseline_date
        dated_dir.mkdir(parents=True, exist_ok=True)

        downloaded_files = self._download_files(baseline_files, dated_dir, dry_run=dry_run, overwrite=overwrite)
        return downloaded_files




class PubMedFTPClient(GenericFTPClient):
    def __init__(self):
        self.host = PMC_FTP_HOST
        self.dir = PUBMED_OPEN_ACCESS_DIR
        self.ftp = None
        self.connect()

    def extract_pubmed_files(self, files: List[str]) -> Tuple[str, List[str]]:
        """Extract baseline files from PubMed directory"""
        baseline_numbers = set()
        baseline_files = []

        for file in files:
            # Match pattern like 'pubmed25n0001.xml.gz' but not '.gz.md5'
            if file.endswith('.gz') and not file.endswith('.gz.md5'):
                match = re.search(r'pubmed(\d+)n\d+\.xml\.gz$', file)
                if match:
                    baseline_numbers.add(match.group(1))
                    baseline_files.append(file)

        if len(baseline_numbers) != 1:
            raise ValueError(f"Exactly one baseline number is expected, found {len(baseline_numbers)}: {baseline_numbers}")

        # Convert baseline number (e.g., "25") to a date string (e.g., "2025-01-01")
        baseline_year = f"20{baseline_numbers.pop()}"

        return baseline_year, baseline_files


    def _download_pubmed_baselines_sample(self, base_dir: Path, file_names: List[str], dry_run: bool = False, overwrite: bool = False) -> List[Path]:
        if not self.ftp:
            raise ConnectionError("Not connected to FTP server")

        # Create base directory if it doesn't exist
        base_dir.mkdir(parents=True, exist_ok=True)

        # Get list of remote files
        raw_file_names = file_names

        # Extract baseline date and files
        baseline_date, baseline_files = self.extract_pubmed_files(raw_file_names)

        # Create dated directory
        dated_dir = base_dir / baseline_date
        dated_dir.mkdir(parents=True, exist_ok=True)

        downloaded_files = self._download_files(baseline_files, dated_dir, dry_run=dry_run, overwrite=overwrite)
        return downloaded_files

    def _download_pubmed_baselines(self, base_dir: Path, dry_run: bool = False, overwrite: bool = False) -> List[Path]:
        if not self.ftp:
            raise ConnectionError("Not connected to FTP server")

        # Create base directory if it doesn't exist
        base_dir.mkdir(parents=True, exist_ok=True)


        # Get list of remote files
        raw_file_names = self.list_directory()

        # Extract baseline date and files
        baseline_date, baseline_files = self.extract_pubmed_files(raw_file_names)

        # Create dated directory
        dated_dir = base_dir / baseline_date
        dated_dir.mkdir(parents=True, exist_ok=True)

        downloaded_files = self._download_files(baseline_files, dated_dir, dry_run=dry_run, overwrite=overwrite)
        return downloaded_files, baseline_date

class PMCParser:
    def __init__(self):
        self.unique_article_types = defaultdict(int)
        # Use number of CPU cores for ThreadPoolExecutor
        self._cpu_count = multiprocessing.cpu_count()

    def print_article_type_distribution(self):
        total_docs = sum(self.unique_article_types.values())
        print("Article type distribution:")
        # Convert to list of tuples and sort by percentage (count/total) in descending order
        sorted_types = sorted(
            self.unique_article_types.items(),
            key=lambda x: (x[1] / total_docs if total_docs > 0 else 0),
            reverse=True
        )
        for article_type, count in sorted_types:
            percentage = (count / total_docs * 100) if total_docs > 0 else 0
            print(f"  {article_type}: {count} ({percentage:.1f}%)")

    def _extract_authors(self, contrib_group) -> List[Author]:
        """Extract author information from contrib-group element"""
        authors = []
        # Get all affiliations first
        affiliations = {}
        if contrib_group is None:
            return authors

        for aff in contrib_group.findall(".//aff"):
            aff_id = aff.get("id")
            if aff_id:
                # Get full text content of affiliation including nested elements
                aff_text = ''.join(aff.itertext()).strip()
                # Remove the label if it exists
                label_elem = aff.find('label')
                if label_elem is not None and label_elem.text and aff_text.startswith(label_elem.text):
                    aff_text = aff_text[len(label_elem.text):].strip()
                affiliations[aff_id] = aff_text

        for contrib in contrib_group.findall(".//contrib[@contrib-type='author']"):
            name = contrib.find(".//name")
            if name is not None:
                surname = name.find("surname").text if name.find("surname") is not None else ""
                given_names = name.find("given-names").text if name.find("given-names") is not None else ""
                full_name = f"{surname}, {given_names}"

                # Get email
                email = contrib.find(".//email")
                email_text = email.text if email is not None else None

                # Get affiliations
                author_affiliations = []
                for xref in contrib.findall(".//xref[@ref-type='aff']"):
                    aff_id = xref.get("rid")
                    if aff_id in affiliations:
                        author_affiliations.append(affiliations[aff_id])

                # Check if corresponding author
                is_corresponding = contrib.find(".//xref[@ref-type='corresp']") is not None

                authors.append(Author(
                    name=full_name,
                    email=email_text,
                    affiliations=author_affiliations,
                    is_corresponding=is_corresponding
                ))
        return authors

    def _extract_dates(self, article_meta) -> PublicationDates:
        """Extract publication dates from article-meta element"""
        dates = {}

        # Handle dates in history element
        history_date_types = {
            "received": "received_date",
            "accepted": "accepted_date",
        }

        for history_date in article_meta.findall(".//history/date"):
            date_type = history_date.get("date-type")
            if date_type in history_date_types:
                year = history_date.find("year")
                month = history_date.find("month")
                day = history_date.find("day")

                if year is not None:
                    date_str = f"{year.text}"
                    if month is not None:
                        date_str = f"{date_str}-{month.text}"
                        if day is not None:
                            date_str = f"{date_str}-{day.text}"
                    dates[history_date_types[date_type]] = date_str

        # Handle pub-dates
        pub_date_types = {
            "epub": "epub_date",
            "collection": "collection_date"
        }

        for pub_date in article_meta.findall(".//pub-date"):
            pub_type = pub_date.get("pub-type")
            if pub_type in pub_date_types:
                year = pub_date.find("year")
                month = pub_date.find("month")
                day = pub_date.find("day")

                if year is not None:
                    date_str = f"{year.text}"
                    if month is not None:
                        date_str = f"{date_str}-{month.text}"
                        if day is not None:
                            date_str = f"{date_str}-{day.text}"
                    dates[pub_date_types[pub_type]] = date_str

        return PublicationDates(**dates)

    def _extract_publication_year(self, publication_dates: PublicationDates) -> int:
        """Extract publication year from publication dates in priority order.

        Priority order:
        1. Collection date (final publication)
        2. EPub date (electronic publication)
        3. Accepted date
        4. Received date
        """
        date_priority = [
            'collection_date',
            'epub_date',
            'accepted_date',
            'received_date'
        ]

        for date_type in date_priority:
            date_value = getattr(publication_dates, date_type)
            if date_value:
                return int(date_value.split("-")[0])

        return None

    def _extract_journal_metadata(self, journal_meta) -> JournalMetadata:
        """Extract journal metadata from journal-meta element"""
        # Get journal title
        journal_title = journal_meta.find(".//journal-title")
        title = journal_title.text if journal_title is not None else ""

        # Get ISSN
        issn = journal_meta.find(".//issn")
        issn_text = issn.text if issn is not None else None

        # Get publisher
        publisher = journal_meta.find(".//publisher/publisher-name")
        publisher_text = publisher.text if publisher is not None else None

        # Get abbreviation
        journal_id = journal_meta.find(".//journal-id[@journal-id-type='nlm-ta']")
        if journal_id is None:
            journal_id = journal_meta.find(".//journal-id[@journal-id-type='iso-abbrev']")
        abbreviation = journal_id.text if journal_id is not None else None

        return JournalMetadata(
            title=title,
            issn=issn_text,
            publisher=publisher_text,
            abbreviation=abbreviation
        )

    def _extract_abstract(self, front) -> str:
        """Extract abstract text from front element"""
        abstract = front.find(".//abstract")
        if abstract is None:
            return ""

        # Combine all paragraph texts
        paragraphs = []
        for p in abstract.findall(".//p"):
            if p.text:
                paragraphs.append(p.text.strip())
        return " ".join(paragraphs)

    def _extract_section_text(self, section_elem) -> str:
        """Extract all text content from a section, including nested paragraphs"""
        texts = []
        for p in section_elem.findall(".//p"):
            # Get all text content including from nested elements
            text = ''.join(p.itertext()).strip()
            if text:
                texts.append(text)
        return " ".join(texts)

    def _extract_sections(self, body_elem, parent_section=None) -> List[Section]:
        """Recursively extract sections from the document body"""
        sections = []

        # If we're parsing the body element itself, look for direct sec elements
        xpath = "./sec" if parent_section is None else ".//sec"

        for sec in body_elem.findall(xpath):
            # Get section ID if present
            section_id = sec.get("id")

            # Get section label if present
            label_elem = sec.find("label")
            label = label_elem.text if label_elem is not None else None

            # Get section title
            title_elem = sec.find("title")
            title = title_elem.text if title_elem is not None else "Untitled Section"

            # Get section text content
            text = self._extract_section_text(sec)

            if text is None:
                continue


            # Create section object
            section = Section(
                id=section_id,
                label=label,
                title=title,
                text=text,
                subsections=[]
            )

            # Recursively process subsections
            subsections = self._extract_sections(sec, parent_section=section)
            if subsections:
                section.subsections = subsections

            sections.append(section)

        return sections

    def _reorder_ids(self, ids: List[DocumentId]) -> List[DocumentId]:
        """Reorder the IDs by type (doi, pmc, pmid, pii, *, publisher-id)"""
        explicit_id_types_order = ["doi", "pmc", "pmid", "pii", "publisher-id"]
        # Return the IDs in the order of explicit_id_types_order
        ids_list = []
        id2type2id = {}
        for id in ids:
            id2type2id[id.type] = id.id

        for id_type in explicit_id_types_order:
            if id_type in id2type2id:
                ids_list.append(DocumentId(id=id2type2id[id_type], type=id_type))

        # Add any remaining IDs that are not in the explicit_id_types_order
        for id in ids:
            if id.type not in explicit_id_types_order:
                ids_list.append(id)

        return ids_list

    def parse_doc(self, file_contents: str, file_name: Path) -> Document:
        """Parse PMC XML document and extract relevant information"""
        # Normalize the document
        normalized_content = normalize_document(file_contents)

        root = ET.fromstring(file_contents)

        # Extract front matter which contains metadata
        front = root.find(".//front")

        self.unique_article_types[root.get("article-type", None)] += 1
        # Get article type
        if root.get("article-type", None) is None or root.get("article-type", None).strip() not in PMC_ARTICLE_TYPE_MAP:
            log.warn(f"File: {file_name.name} - Article type: {root.get('article-type', None)} not known!")
            article_type = None
        else:
            article_type = PMC_ARTICLE_TYPE_MAP.get(root.get("article-type").strip(), ArticleType.OTHER)

        # Get article meta section
        article_meta = front.find(".//article-meta")

        # Get all article IDs
        ids = []
        for article_id in article_meta.findall(".//article-id"):
            id_type = article_id.get("pub-id-type")
            if article_id.text:
                ids.append(DocumentId(id=article_id.text, type=id_type))

        # reorder the IDs by type
        ids = self._reorder_ids(ids)

        # Get title
        title_elem = article_meta.find(".//article-title")
        title = None
        if title_elem is not None:
            title = ''.join(title_elem.itertext()).strip()
        else:
            # For correction articles, the title is often in the first paragraph
            if root.get("article-type") == "correction":
                first_p = root.find(".//body/p")
                if first_p is not None:
                    title = ''.join(first_p.itertext()).strip()

        if title is None:
            title = "Untitled Article"  # Provide a default title to satisfy validation

        # Get journal metadata
        journal_meta = front.find(".//journal-meta")
        journal = self._extract_journal_metadata(journal_meta)

        # Get publication year
        pub_date = article_meta.find(".//pub-date[@pub-type='collection']")
        journal = self._extract_journal_metadata(journal_meta) if journal_meta is not None else None

        # Get publication dates
        publication_dates = self._extract_dates(article_meta)
        publication_year = self._extract_publication_year(publication_dates)

        # Get authors
        contrib_group = article_meta.find(".//contrib-group")
        authors = self._extract_authors(contrib_group)

        # Get abstract
        abstract = self._extract_abstract(front)

        # Get keywords
        keywords = []
        kwd_group = article_meta.find(".//kwd-group")
        if kwd_group is not None:
            for kwd in kwd_group.findall(".//kwd"):
                if kwd.text:
                    keywords.append(kwd.text)

        # Get subject groups
        subject_groups = []
        for subj in article_meta.findall(".//subj-group/subject"):
            if subj.text:
                subject_groups.append(subj.text)

        # Get license information
        license_elem = article_meta.find(".//license")
        license_type = None
        if license_elem is not None:
            license_ref = license_elem.find(".//{http://www.niso.org/schemas/ali/1.0/}license_ref")
            if license_ref is not None:
                license_type = license_ref.text

        # Get copyright information
        copyright_statement = None
        copyright_year = None
        copyright_elem = article_meta.find(".//copyright-statement")
        if copyright_elem is not None:
            copyright_statement = copyright_elem.text
        copyright_year_elem = article_meta.find(".//copyright-year")
        if copyright_year_elem is not None:
            copyright_year = copyright_year_elem.text

        # Get the document body and extract sections
        body = root.find(".//body")
        sections = []
        if body is not None:
            sections = self._extract_sections(body)

        return Document(
            ids=ids,
            title=title,
            raw_type=root.get("article-type", None),
            type=article_type,
            journal=journal,
            year=publication_year,
            publication_dates=publication_dates,
            abstract=abstract,
            keywords=keywords,
            authors=authors,
            subject_groups=subject_groups,
            sections=sections,
            license_type=license_type,
            copyright_statement=copyright_statement,
            copyright_year=copyright_year,
            parsed_date=datetime.now(timezone.utc)
        )

    def parse_docs(self, files: List[Path], output_dir: Path) -> List[Path]:
        """Parse a list of PMC XML files and save to output_dir"""
        documents = []
        counter = 0
        timestamp = datetime.now(timezone.utc)

        for file in files:
            file_name = file.stem + '.json'
            try:
                with file.open(mode='r') as f:
                    doc = self.parse_doc(f.read(), file)
                    counter += 1
                    if counter % 10000 == 0:
                        elapsed_seconds = (datetime.now(timezone.utc) - timestamp).total_seconds()
                        log.info(f"Parsed {counter} files in {elapsed_seconds:.1f} seconds")
                        timestamp = datetime.now(timezone.utc)

                output_path = output_dir / file_name
                with open(output_path, 'w') as f:
                    f.write(doc.model_dump_json(indent=2))
                documents.append(output_path)
            except Exception as e:
                log.error(f"Error parsing {file.name}: {str(e)}")

        return documents
