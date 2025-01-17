#!/usr/bin/env python3

import ftplib
import os
from typing import Dict, List, Optional, Tuple
import re
from cloudpathlib import CloudPath


from click import Path

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

    def download_incremental(self, base_dir: Path = Path('data/incremental'), dry_run: bool = False) -> List[Path]:
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
        downloaded_files = self.download_files(incremental_files, dated_dir, dry_run=dry_run, overwrite=False)

        return downloaded_files

    def download_baselines(self, base_dir: Path = Path('data/baselines'), dry_run: bool = False) -> List[Path]:
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

        downloaded_files = self.download_files(baseline_files, dated_dir, dry_run=dry_run, overwrite=False)
        return downloaded_files

def main():
    client = PMCFTPClient()
    try:
        client.connect()
        client.download_baselines()
    finally:
        client.close()

if __name__ == "__main__":
    main()
