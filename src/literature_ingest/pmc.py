#!/usr/bin/env python3

import ftplib
from typing import Dict, List, Optional, Tuple
import re
from cloudpathlib import CloudPath
import xml.etree.ElementTree as ET


from click import Path
from pydantic import BaseModel

PMC_FTP_HOST = "ftp.ncbi.nlm.nih.gov"
PMC_OPEN_ACCESS_NONCOMMERCIAL_XML_DIR = '/pub/pmc/oa_bulk/oa_noncomm/xml'

class PMCFTPClient:
    def __init__(self):
        self.host = PMC_FTP_HOST
        self.pmc_open_access_noncommercial_xml_dir = PMC_OPEN_ACCESS_NONCOMMERCIAL_XML_DIR
        self.ftp = None

    def connect(self) -> None:
        """Establish connection to the PMC FTP server"""
        try:
            self.ftp = ftplib.FTP(self.host)
            self.ftp.login()  # anonymous login
            self.ftp.cwd(self.pmc_open_access_noncommercial_xml_dir)
            print(f"Connected to {self.host} and changed to {self.pmc_open_access_noncommercial_xml_dir}")
        except Exception as e:
            print(f"Failed to connect: {str(e)}")
            raise

    def list_directory(self, path: str = '.') -> List[str]:
        """List contents of the specified directory"""
        if not self.ftp:
            raise ConnectionError("Not connected to FTP server")

        files = []
        self.ftp.dir(path, files.append)
        files = [f.split()[-1] for f in files]
        return files

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

    def close(self) -> None:
        """Close the FTP connection"""
        if self.ftp:
            self.ftp.quit()
            self.ftp = None
            print("Connection closed")

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

    def download_files(self, files: List[str], base_dir: Path, dry_run: bool = False, overwrite: bool = False) -> List[Path]:
        """Download all files that don't exist locally."""
        target_file_paths = []

        for remote_file in files:
            target_file_path = base_dir / remote_file
            if not target_file_path.exists() or overwrite:
                if not dry_run:
                    print(f"Downloading {remote_file}...")
                    self.download_file(remote_file, target_file_path)
                    target_file_paths.append(target_file_path)
                else:
                    print(f"Would download {remote_file} to {target_file_path}")
            else:
                print(f"Skipping {remote_file}")
        return target_file_paths

    def download_incremental(self, base_dir: Path = Path('data/incremental'), dry_run: bool = False, overwrite: bool = False) -> List[Path]:
        """Download all incremental files that don't exist locally."""
        if not self.ftp:
            raise ConnectionError("Not connected to FTP server")

        # Create base directory if it doesn't exist
        base_dir.mkdir(parents=True, exist_ok=True)

        # Get list of remote files
        raw_file_names = self.list_directory()

        # Extract baseline date
        baseline_date, _ = self.extract_baseline_files(raw_file_names)

        # Create dated directory - it will be data/incremental/YYYY-MM-DD/*
        dated_dir = base_dir / baseline_date
        dated_dir.mkdir(parents=True, exist_ok=True)

        incremental_files = self.extract_incremental_files(raw_file_names)
        downloaded_files = self.download_files(incremental_files, dated_dir, dry_run=dry_run, overwrite=overwrite)

        return downloaded_files

    def download_baselines(self, base_dir: Path = Path('data/baselines'), dry_run: bool = False, overwrite: bool = False) -> List[Path]:
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

        downloaded_files = self.download_files(baseline_files, dated_dir, dry_run=dry_run, overwrite=overwrite)
        return downloaded_files


class DocumentId(BaseModel):
    id: str
    type: str

class Document(BaseModel):
    id: DocumentId
    title: str
    authors: List[str]
    abstract: str
    type: str
    journal: str
    year: int

class PMCParser:
    def __init__(self):
        pass

    def _extract_authors(self, contrib_group) -> List[str]:
        """Extract author names from contrib-group element"""
        authors = []
        for contrib in contrib_group.findall(".//contrib[@contrib-type='author']"):
            name = contrib.find(".//name")
            if name is not None:
                surname = name.find("surname").text if name.find("surname") is not None else ""
                given_names = name.find("given-names").text if name.find("given-names") is not None else ""
                authors.append(f"{surname}, {given_names}")
        return authors

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

    def parse_doc(self, file_contents: str) -> Document:
        """Parse PMC XML document and extract relevant information"""
        root = ET.fromstring(file_contents)

        # Extract front matter which contains metadata
        front = root.find(".//front")

        # Get article type
        article_type = root.get("article-type", "")

        # Get PMC ID
        article_meta = front.find(".//article-meta")
        pmc_id = None
        for article_id in article_meta.findall(".//article-id"):
            if article_id.get("pub-id-type") == "pmc":
                pmc_id = article_id.text
                break

        # Get title
        title = article_meta.find(".//article-title").text

        # Get journal title
        journal_meta = front.find(".//journal-meta")
        journal = journal_meta.find(".//journal-title").text

        # Get publication year
        pub_date = article_meta.find(".//pub-date[@pub-type='collection']")
        year = int(pub_date.find("year").text)

        # Get authors
        contrib_group = article_meta.find(".//contrib-group")
        authors = self._extract_authors(contrib_group)

        # Get abstract
        abstract = self._extract_abstract(front)

        return Document(
            id=DocumentId(id=pmc_id, type="pmc"),
            title=title,
            authors=authors,
            abstract=abstract,
            type=article_type,
            journal=journal,
            year=year
        )
