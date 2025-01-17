#!/usr/bin/env python3

import ftplib
import os
from typing import List, Optional

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

    def get_baseline_date(self) -> str:
        """Extract the date from baseline files in the current directory"""
        files = self.list_directory()
        for file in files:
            if 'baseline' in file:
                # Extract date using string split on whitespace and take the date field
                date_str = file.split()[0].split('.')[-3]
                return date_str
        raise ValueError("No baseline files found in directory")

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

        baseline_date = self.get_baseline_date()
        dated_dir = os.path.join(base_dir, baseline_date)
        os.makedirs(dated_dir, exist_ok=True)

        # Get list of remote files
        remote_files = self.list_directory()
        baseline_files = [f.split()[0] for f in remote_files if 'baseline' in f]

        # Download missing files
        for remote_file in baseline_files:
            local_path = os.path.join(dated_dir, remote_file)
            if not os.path.exists(local_path):
                if not dry_run:
                    print(f"Downloading {remote_file}...")
                    self.download_file(remote_file, local_path)
                else:
                    print(f"Would download {remote_file} to {local_path}")
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
