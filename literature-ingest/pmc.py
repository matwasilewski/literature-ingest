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
            self.ftp.cwd('/pub/pmc')  # Change to PMC directory
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

def main():
    # Example usage
    client = PMCFTPClient()
    try:
        client.connect()
        print("\nDirectory listing:")
        files = client.list_directory()
        for file in files:
            print(file)
            
        # Example: Download the journal list file
        # client.download_file('jlist.csv')
        
    finally:
        client.close()

if __name__ == "__main__":
    main() 