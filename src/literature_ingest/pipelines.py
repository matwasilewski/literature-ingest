from collections import defaultdict
from pathlib import Path
from typing import List

from literature_ingest.data_engineering import unzip_and_filter
from literature_ingest.models import ArticleType, Document
from literature_ingest.pmc import PMCParser

# def find_clinical_trials(parsed_dir: Path) -> List[Path]:
#     clinical_trials = []
#     article_type_statistics = defaultdict(int)
#     for doc in parsed_dir.glob("*.json"):
#         document = Document.model_validate_json(doc.read_text())
#         article_type_statistics[document.article_type] += 1

#         if document.article_type == ArticleType.CLINICAL_TRIAL:
#             if document.clinical_trials:
#                 clinical_trials.append(doc)
#     return clinical_trials

def parse_pmc_docs_sample(start_from_parsed: bool = False):
    source_dir = Path("data/pipelines/raw/")
    unzipped_dir = Path("data/pipelines/unzipped/")
    parsed_dir = Path("data/pipelines/parsed/")

    parsed_dir.mkdir(parents=True, exist_ok=True)

    parser = PMCParser()

    if not start_from_parsed:
        print(f"Unzipping {source_dir}...")
        for file in source_dir.glob("*.tar.gz"):
            print(f"Unzipping {file}...")
            unzip_and_filter(file, unzipped_dir, extension=".xml", use_gsutil=False, overwrite=True)
    print(f"Unzipped {unzipped_dir}...")

    print(f"Parsing {unzipped_dir}...")
    unzipped_files = [Path(x) for x in list(unzipped_dir.glob("*.xml"))]
    parsed_files = parser.parse_docs(unzipped_files, parsed_dir)

    print(f"Parsed {len(parsed_files)} files...")
    print(f"Unique article types: {parser.unique_article_types}")

if __name__ == "__main__":
    parse_pmc_docs_sample()
