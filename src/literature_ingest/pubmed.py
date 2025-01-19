#!/usr/bin/env python3

from collections import defaultdict
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
import asyncio
from datetime import datetime, timezone
from pathlib import Path
import xml.etree.ElementTree as ET

from literature_ingest.models import ArticleType, Author, Document, DocumentId, JournalMetadata, PublicationDates
from literature_ingest.normalization import normalize_document
from literature_ingest.utils.logging import log

class PubMedParser:
    def __init__(self):
        self.unique_article_types = defaultdict(int)
        # Use number of CPU cores for ThreadPoolExecutor
        self._cpu_count = multiprocessing.cpu_count()
        self._executor = ThreadPoolExecutor(max_workers=self._cpu_count)
        # Match semaphore to CPU count to avoid oversubscription
        self._semaphore = asyncio.Semaphore(self._cpu_count)

    def _extract_journal_metadata(self, journal_elem) -> JournalMetadata:
        """Extract journal metadata from Journal element"""
        # Get journal title
        journal_title = journal_elem.find(".//Title")
        title = journal_title.text if journal_title is not None else ""

        # Get ISSN
        issn = journal_elem.find(".//ISSN")
        issn_text = issn.text if issn is not None else None

        # Get abbreviation
        journal_abbrev = journal_elem.find(".//ISOAbbreviation")
        abbreviation = journal_abbrev.text if journal_abbrev is not None else None

        return JournalMetadata(
            title=title,
            issn=issn_text,
            abbreviation=abbreviation
        )

    def _extract_dates(self, article_elem) -> PublicationDates:
        """Extract publication dates from article element"""
        dates = {}

        # Month name to number mapping
        month_map = {
            'Jan': '1', 'Feb': '2', 'Mar': '3', 'Apr': '4',
            'May': '5', 'Jun': '6', 'Jul': '7', 'Aug': '8',
            'Sep': '9', 'Oct': '10', 'Nov': '11', 'Dec': '12'
        }

        # Handle PubDate in Journal/JournalIssue
        pub_date = article_elem.find(".//Journal/JournalIssue/PubDate")
        if pub_date is not None:
            year = pub_date.find("Year")
            month = pub_date.find("Month")
            day = pub_date.find("Day")

            if year is not None:
                date_str = f"{year.text}"
                if month is not None:
                    # Convert month name to number if it's a name
                    month_text = month.text.strip()
                    month_num = month_map.get(month_text[:3], month_text)
                    date_str = f"{date_str}-{month_num}"
                    if day is not None:
                        date_str = f"{date_str}-{day.text}"
                dates["collection_date"] = date_str

        # Handle History dates
        history = article_elem.find("../PubmedData/History")
        if history is not None:
            for date in history.findall("PubMedPubDate"):
                pub_status = date.get("PubStatus")
                if pub_status == "pubmed":
                    year = date.find("Year")
                    month = date.find("Month")
                    day = date.find("Day")

                    if year is not None:
                        date_str = f"{year.text}"
                        if month is not None:
                            # Convert month name to number if it's a name
                            month_text = month.text.strip()
                            month_num = month_map.get(month_text[:3], month_text)
                            date_str = f"{date_str}-{month_num}"
                            if day is not None:
                                date_str = f"{date_str}-{day.text}"
                        dates["epub_date"] = date_str

        return PublicationDates(**dates)

    def _extract_publication_year(self, publication_dates: PublicationDates) -> int:
        """Extract publication year from publication dates"""
        if publication_dates.collection_date:
            return int(publication_dates.collection_date.split("-")[0])
        elif publication_dates.epub_date:
            return int(publication_dates.epub_date.split("-")[0])
        return None

    def _extract_authors(self, article_elem) -> list[Author]:
        """Extract author information from article element"""
        authors = []
        author_list = article_elem.find(".//AuthorList")

        if author_list is not None:
            for author_elem in author_list.findall("Author"):
                last_name = author_elem.find("LastName")
                fore_name = author_elem.find("ForeName")

                if last_name is not None:
                    name_parts = []
                    name_parts.append(last_name.text)
                    if fore_name is not None:
                        name_parts.append(fore_name.text)

                    name = ", ".join(name_parts)
                    authors.append(Author(name=name))

        return authors

    def _extract_keywords(self, article_elem) -> list[str]:
        """Extract keywords from MeSH headings"""
        keywords = set()

        # Find MedlineCitation first
        medline_citation = article_elem.find("../..")
        if medline_citation is None:
            return list(keywords)

        # Extract from ChemicalList
        chemical_list = medline_citation.find("ChemicalList")
        if chemical_list is not None:
            for chemical in chemical_list.findall(".//NameOfSubstance"):
                if chemical.text:
                    keywords.add(chemical.text)

        # Extract from MeshHeadingList
        mesh_list = medline_citation.find("MeshHeadingList")
        if mesh_list is not None:
            for mesh in mesh_list.findall(".//DescriptorName"):
                if mesh.text:
                    keywords.add(mesh.text)

        return list(keywords)

    def _extract_subject_groups(self, article_elem) -> list[str]:
        """Extract subject groups from PublicationTypeList"""
        subject_groups = []
        pub_types = article_elem.find(".//PublicationTypeList")

        if pub_types is not None:
            for pub_type in pub_types.findall("PublicationType"):
                if pub_type.text:
                    subject_groups.append(pub_type.text)

        return subject_groups

    def _reorder_ids(self, ids: list[DocumentId]) -> list[DocumentId]:
        """Reorder the IDs by type (pubmed, doi, *)"""
        explicit_id_types_order = ["pubmed", "doi"]
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

    async def parse_doc(self, file_contents: str, file_name: Path) -> Document:
        """Parse PubMed XML document and extract relevant information asynchronously"""
        # Since XML parsing is CPU-bound, we'll use a thread pool
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self._parse_doc_sync, file_contents, file_name)

    def _parse_doc_sync(self, file_contents: str, file_name: Path) -> Document:
        """Synchronous version of parse_doc for running in thread pool"""
        # Normalize the document
        normalized_content = normalize_document(file_contents)

        root = ET.fromstring(file_contents)
        article = root.find(".//PubmedArticle/MedlineCitation/Article")

        if article is None:
            raise ValueError("No Article element found in PubMed XML")

        # Get article type
        article_type = ArticleType.RESEARCH_ARTICLE  # Default to research article for PubMed

        # Get article IDs
        ids = []
        # Add PMID
        pmid = root.find(".//PMID")
        if pmid is not None and pmid.text:
            ids.append(DocumentId(id=pmid.text, type="pubmed"))

        # Add DOI if present
        article_ids = root.findall(".//PubmedData/ArticleIdList/ArticleId")
        for article_id in article_ids:
            id_type = article_id.get("IdType")
            if id_type == "doi" and article_id.text:
                ids.append(DocumentId(id=article_id.text, type="doi"))

        # Reorder IDs
        ids = self._reorder_ids(ids)

        # Get title
        title_elem = article.find(".//ArticleTitle")
        title = title_elem.text if title_elem is not None else "Untitled Article"

        # Get journal metadata
        journal = article.find("Journal")
        journal_metadata = self._extract_journal_metadata(journal) if journal is not None else None

        # Get publication dates
        publication_dates = self._extract_dates(article)
        publication_year = self._extract_publication_year(publication_dates)

        # Get authors
        authors = self._extract_authors(article)

        # Get keywords from MeSH terms
        keywords = self._extract_keywords(article)

        # Get subject groups
        subject_groups = self._extract_subject_groups(article)

        return Document(
            ids=ids,
            title=title,
            type=article_type,
            journal=journal_metadata,
            year=publication_year,
            publication_dates=publication_dates,
            keywords=keywords,
            authors=authors,
            subject_groups=subject_groups,
            parsed_date=datetime.now(timezone.utc)
        )

    async def parse_docs(self, files: list[Path], output_dir: Path) -> list[Path]:
        """Parse a list of PubMed XML files and save to output_dir asynchronously"""
        documents = []
        counter = 0
        timestamp = datetime.now(timezone.utc)
        tasks = []

        async def process_file(file: Path) -> Path | None:
            nonlocal counter
            nonlocal timestamp
            file_name = file.stem + '.json'
            try:
                async with self._semaphore:  # Use semaphore to limit concurrent operations
                    with file.open(mode='r') as f:
                        doc = await self.parse_doc(f.read(), file)
                        counter += 1
                        if counter % 10000 == 0:
                            elapsed_seconds = (datetime.now(timezone.utc) - timestamp).total_seconds()
                            log.info(f"Parsed {counter} files in {elapsed_seconds:.1f} seconds")
                            timestamp = datetime.now(timezone.utc)

                    output_path = output_dir / file_name
                    with open(output_path, 'w') as f:
                        f.write(doc.model_dump_json(indent=2))
                    return output_path
            except Exception as e:
                log.error(f"Error parsing {file.name}: {str(e)}")
                return None

        # Create tasks for all files
        for file in files:
            tasks.append(process_file(file))

        # Process all files concurrently
        results = await asyncio.gather(*tasks)

        # Filter out None results (failed files)
        documents = [doc for doc in results if doc is not None]
        return documents
