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

    def download_file(self, remote_file: str, local_path: Optional[str] = None) -> None:
        """Download a file from the FTP server"""
        if not self.ftp:
            raise ConnectionError("Not connected to FTP server")

        if local_path is None:
            local_path = os.path.basename(remote_file)

        try:
            with open(local_path, 'wb') as f:
                self.ftp.retrbinary(f'RETR {remote_file}', f.write)
            print(f"Successfully downloaded {remote_file} to {local_path}")
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

    def download_baselines(self, base_dir: Path = Path('data/baselines'), dry_run: bool = False) -> None:
        """Download all baseline files that don't exist locally.

        Baseline files is a batch of PMC documents that should contain all PMC documents released up to a certain date.
        Baseline is then updated with new PMC documents released after the date in DAILY incremental updates.

        Fundamental logic is that we download whatever baseline file is available, and then we download the daily incremental updates. If keeping up to date with latest PMC documents is important.

        The date is extracted from the baseline file name.
        """
        if not self.ftp:
            raise ConnectionError("Not connected to FTP server")

        os.makedirs(base_dir, exist_ok=True)

        raw_file_names = self.list_directory()
        baseline_date, baseline_files = self.extract_baseline_files(raw_file_names)
        dated_dir = os.path.join(base_dir, baseline_date)
        os.makedirs(dated_dir, exist_ok=True)

        # Download missing files
        for remote_file in baseline_files:
            target_file_path = os.path.join(dated_dir, remote_file)
            if not os.path.exists(target_file_path):
                if not dry_run:
                    print(f"Downloading {remote_file}...")
                    self.download_file(remote_file, target_file_path)
                else:
                    print(f"Would download {remote_file} to {target_file_path}")
            else:
                print(f"Skipping {remote_file} - already exists")

def main():
    client = PMCFTPClient()
    try:
        client.connect()
        client.download_baselines()
    finally:
        client.close()

if __name__ == "__main__":
    main()
