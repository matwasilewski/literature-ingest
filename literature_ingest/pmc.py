#!/usr/bin/env python3

import ftplib
import os
from typing import List, Optional

class PMCFTPClient:
    def __init__(self):
        self.host = "ftp.ncbi.nlm.nih.gov"
        self.ftp = None

    def connect(self) -> None:
        """Establish connection to the PMC FTP server"""
        try:
            self.ftp = ftplib.FTP(self.host)
            self.ftp.login()  # anonymous login
            self.ftp.cwd('/pub/pmc/oa_bulk/oa_noncomm/xml/')  # Change to PMC directory
            print(f"Connected to {self.host}")
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

    def download_baselines(self, base_dir: str = 'data/baselines') -> None:
        """Download all baseline files that don't exist locally"""
        if not self.ftp:
            raise ConnectionError("Not connected to FTP server")

        # Create base directory if it doesn't exist
        os.makedirs(base_dir, exist_ok=True)

        # Get baseline date and create dated directory
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
                print(f"Downloading {remote_file}...")
                self.download_file(remote_file, local_path)
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
