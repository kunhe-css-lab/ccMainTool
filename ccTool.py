import pyarrow.parquet as pq  # not used yet
import pandas as pd
import requests
from io import BytesIO
from warcio.archiveiterator import ArchiveIterator
import gzip
import os
from bs4 import BeautifulSoup
from tqdm import tqdm
import duckdb

# ============================================================
# Step 1: Get the list of parquet files from cc-index-table.paths.gz
# ============================================================
def get_warc_parquet_paths(crawl: str):
    """
    Download cc-index-table.paths.gz and return parquet shard paths for subset=warc.
    """
    paths_url = f"https://data.commoncrawl.org/crawl-data/{crawl}/cc-index-table.paths.gz"
    print(f"Fetching parquet paths from: {paths_url}")
    response = requests.get(paths_url)
    with gzip.open(BytesIO(response.content), "rt") as f:
        parquet_paths = [line.strip() for line in f]

    warc_parquet_paths = [p for p in parquet_paths if "/subset=warc/" in p]
    print(f"Found {len(warc_parquet_paths)} parquet files for warc subset")
    return warc_parquet_paths


def resolve_num_parquet_files(
    warc_parquet_paths,
    scan_all_parquet: bool,
    num_parquet_files: int,
    keywords: list,
):
    """
    Decide how many parquet shards to scan (all vs first N)
    and print summary information.
    """

    keyword_str = ", ".join(keywords)

    if scan_all_parquet:
        num_parquet_files = len(warc_parquet_paths)
        print(f"Will scan ALL {num_parquet_files} files for keywords: [{keyword_str}]")
    else:
        print(f"Will scan first {num_parquet_files} files for keywords: [{keyword_str}]")

    return num_parquet_files

# ============================================================
# Step 2 + 3: Read parquet files and filter
# ============================================================
def scan_and_filter_index_duckdb(
    warc_parquet_paths,
    *,
    keywords: list,
    num_parquet_files: int,
    columns_to_read,
):
    """
    Scan selected Common Crawl Parquet shards using DuckDB
    with per-shard progress reporting.
    """

    cols_sql = ", ".join(columns_to_read)

    # Build OR condition for keywords
    # If you want AND logic instead, replace " OR " with " AND ".
    keyword_conditions = " OR ".join(
        [f"url ILIKE '%{k}%'" for k in keywords]
    )

    all_filtered = []

    # Move connection outside loop
    con = duckdb.connect(database=":memory:")

    try:
        for i, parquet_path in enumerate(
            tqdm(warc_parquet_paths[:num_parquet_files], desc="Scanning parquet files")
        ):

            https_url = f"https://data.commoncrawl.org/{parquet_path}"

            query = f"""
            SELECT {cols_sql}
            FROM read_parquet('{https_url}')
            WHERE ({keyword_conditions})
              AND content_languages ILIKE '%eng%'
              AND content_mime_type = 'text/html'
            """

            try:
                arrow_tbl = con.execute(query).arrow()
                df_part = arrow_tbl.to_pandas()

                if len(df_part) > 0:
                    all_filtered.append(df_part)
                    tqdm.write(
                        f"  [{i+1}/{num_parquet_files}] Found {len(df_part)} matches"
                    )

            except Exception as e:
                tqdm.write(
                    f"  [{i+1}/{num_parquet_files}] Error: {e}"
                )

    finally:
        con.close()

    # Combine results
    if all_filtered:
        filtered_df = pd.concat(all_filtered, ignore_index=True)
    else:
        filtered_df = pd.DataFrame(columns=columns_to_read)

    print(f"\n{'='*60}")
    print(
        f"TOTAL: {len(filtered_df)} matching records across {num_parquet_files} parquet files (DuckDB)"
    )

    return filtered_df

# ============================================================
# Step 4: Check size and save if < 1GB
# ============================================================
def save_filtered_index_if_small(
    filtered_df: pd.DataFrame,
    *,
    output_path: str,
    max_gb: float = 1.0,
):
    """
    Save filtered_df to parquet at output_path if it is smaller than max_gb.
    Returns the output_path if saved, else None.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    memory_size_bytes = filtered_df.memory_usage(deep=True).sum()
    memory_size_gb = memory_size_bytes / (1024**3)

    print(f"\nFiltered dataset size: {memory_size_bytes:,} bytes ({memory_size_gb:.4f} GB)")

    if memory_size_gb < max_gb:
        filtered_df.to_parquet(output_path, index=False)
        print(f"✓ Saved filtered index to: {output_path}")
        return output_path
    else:
        print(f"✗ Dataset too large ({memory_size_gb:.2f} GB > {max_gb:.2f} GB). Not saving.")
        return None

# ============================================================
# Load previously saved filtered index
# ============================================================
def load_filtered_index(path: str):
    """
    Load a previously saved filtered parquet index file.

    Parameters
    ----------
    path : str
        Path to the saved parquet file.

    Returns
    -------
    pd.DataFrame
        Loaded filtered metadata DataFrame.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Filtered index file not found: {path}")

    print(f"Loading filtered index from: {path}")
    df = pd.read_parquet(path, engine="pyarrow")
    print(f"Loaded {len(df)} records.")
    return df

# ============================================================
# Step 5: Fetch + extract text from WARC
# ============================================================
def fetch_text_from_warc(warc_filename, offset, length):
    """
    Fetch a single WARC record using byte-range request and extract clean text.
    This is MORE EFFICIENT than streaming WET files.
    """
    url = f"https://data.commoncrawl.org/{warc_filename}"
    headers = {"Range": f"bytes={offset}-{offset + length - 1}"}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        decompressed = gzip.decompress(response.content)
        for record in ArchiveIterator(BytesIO(decompressed)):
            html = record.content_stream().read().decode('utf-8', errors='ignore')

            soup = BeautifulSoup(html, 'html.parser')

            for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 'noscript']):
                tag.decompose()

            text = soup.get_text(separator='\n', strip=True)
            return text

    except Exception as e:
        return f"Error: {e}"

    return None

def save_text_content(filtered_df, output_dir, max_records=None):
    """
    Fetch and save extracted text from WARC records using byte-range requests.
    Much faster than WET streaming!
    """
    os.makedirs(output_dir, exist_ok=True)

    if max_records is None:
        max_records = len(filtered_df)

    total_to_process = min(max_records, len(filtered_df))

    print(f"\n{'='*60}")
    print(f"Extracting text from {total_to_process} WARC records...")
    print("(Using byte-range requests - efficient random access)\n")

    saved_count = 0

    for idx, row in tqdm(filtered_df.head(total_to_process).iterrows(),
                         total=total_to_process,
                         desc="Fetching WARC records"):
        text = fetch_text_from_warc(
            row['warc_filename'],
            row['warc_record_offset'],
            row['warc_record_length']
        )

        if text and not text.startswith("Error:"):
            safe_filename = f"doc_{saved_count:04d}.txt"
            filepath = os.path.join(output_dir, safe_filename)

            with open(filepath, "w", encoding="utf-8") as f:
                f.write(f"URL: {row['url']}\n")
                f.write(f"Domain: {row['url_host_registered_domain']}\n")
                f.write(f"Language: {row['content_languages']}\n")
                f.write(f"Fetch Time: {row['fetch_time']}\n")
                f.write(f"{'='*60}\n\n")
                f.write(text)

            saved_count += 1

    print(f"\n{'='*60}")
    print(f"Saved {saved_count} text files to: {output_dir}")
    return saved_count