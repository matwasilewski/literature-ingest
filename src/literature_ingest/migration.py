#!/usr/bin/env python3

import json
from pathlib import Path
from typing import Dict, List, Optional
from literature_ingest.models import Document, Section

def migrate_document(old_doc: Dict) -> Dict:
    """Migrate an old document format to the new one."""
    # Create a copy to avoid modifying the input
    doc = old_doc.copy()

    # Handle title migration
    title = doc.pop("title", "")
    sections = doc.get("sections", [])

    # Add title as first section if it exists
    if title:
        sections.insert(0, {"name": "title", "text": title})

    # Update sections field
    doc["sections"] = sections

    return doc

def migrate_documents_in_directory(directory_path: str) -> None:
    """
    Read all JSON files in the given directory, migrate them to the new format,
    and overwrite them in place if they need migration.

    Args:
        directory_path: Path to directory containing JSON files to migrate
    """
    dir_path = Path(directory_path)
    if not dir_path.exists():
        raise ValueError(f"Directory {directory_path} does not exist")

    for json_file in dir_path.glob("*.json"):
        try:
            # Read the JSON file
            with open(json_file, 'r') as f:
                doc_dict = json.load(f)

            # Check if migration is needed by looking for title field
            if "title" not in doc_dict:
                print(f"Skipping {json_file} - already in new format")
                continue

            # Migrate the document
            migrated_doc = migrate_document(doc_dict)

            # Validate by attempting to load into Document model
            Document(**migrated_doc)

            # Write back to file
            with open(json_file, 'w') as f:
                json.dump(migrated_doc, f, indent=2)

            print(f"Successfully migrated {json_file}")

        except Exception as e:
            print(f"Error processing {json_file}: {str(e)}")

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python migration.py <directory_path>")
        sys.exit(1)

    directory_path = sys.argv[1]
    migrate_documents_in_directory(directory_path)
