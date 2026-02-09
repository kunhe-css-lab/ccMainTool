import pyarrow.parquet as pq
import pandas as pd
import requests
from io import BytesIO
from warcio.archiveiterator import ArchiveIterator
import gzip
import os
from bs4 import BeautifulSoup
from tqdm import tqdm

# ============================================================
# Step 1: Get the list of parquet files from cc-index-table.paths.gz
# ============================================================
CRAWL = "CC-MAIN-2024-10"  # Change to your desired crawl
KEYWORD = r"immigrant|immigration"  # Keyword to search for in URLs, use Regular Expression to allow combination
SCAN_ALL_PARQUET = False  # Set to True to scan ALL parquet files
NUM_PARQUET_FILES = 2 # Number of parquet to scan if SCAN_ALL_PARQUET = False

# Download the parquet file paths listing
paths_url = f"https://data.commoncrawl.org/crawl-data/{CRAWL}/cc-index-table.paths.gz"
print(f"Fetching parquet paths from: {paths_url}")
response = requests.get(paths_url)
with gzip.open(BytesIO(response.content), "rt") as f:
    parquet_paths = [line.strip() for line in f]

# Filter for 'warc' subset only (exclude robotstxt/crawldiagnostics)
warc_parquet_paths = [p for p in parquet_paths if "/subset=warc/" in p]
print(f"Found {len(warc_parquet_paths)} parquet files for warc subset")

if SCAN_ALL_PARQUET:
    NUM_PARQUET_FILES = len(warc_parquet_paths)
    print(f"Will scan ALL {NUM_PARQUET_FILES} files for keyword '{KEYWORD}'")
else:
    print(f"Will scan first {NUM_PARQUET_FILES} files for keyword '{KEYWORD}'")

# ============================================================
# Step 2: Read parquet files with filters
# ============================================================
# Columns needed for filtering
COLUMNS_TO_READ = [
    'url',
    'url_host_tld',
    'url_host_registered_domain',
    'warc_filename',
    'warc_record_offset',
    'warc_record_length',
    'content_mime_type',
    'content_languages',  # For language filtering (ISO-639-3 codes)
    'fetch_time',  # For date filtering
]

all_filtered = []

for i, parquet_path in enumerate(tqdm(warc_parquet_paths[:NUM_PARQUET_FILES], desc="Scanning parquet files")):
    https_url = f"https://data.commoncrawl.org/{parquet_path}"
    
    try:
        # Read with pandas
        df = pd.read_parquet(
            https_url, 
            columns=COLUMNS_TO_READ,
            engine='pyarrow'
        )
        
        # ============================================================
        # Step 3: Filter by keyword, language (English), and content type
        # ============================================================
        # Filter: URL contains keyword (case-insensitive)
        mask_keyword = df['url'].str.contains(KEYWORD, case=False, na=False, regex=True)
        
        # Filter: English content (content_languages contains 'eng')
        mask_english = df['content_languages'].str.contains('eng', case=False, na=False)
        
        # Filter: HTML content only
        # Type of content can be: text/html, application/pdf, text/plain, image/jpeg, application/json
        mask_html = df['content_mime_type'] == 'text/html'
        
        # Combine filters
        filtered = df[mask_keyword & mask_english & mask_html]
        
        if len(filtered) > 0:
            all_filtered.append(filtered)
            tqdm.write(f"  [{i+1}/{NUM_PARQUET_FILES}] Found {len(filtered)} matches")
            
    except Exception as e:
        tqdm.write(f"  [{i+1}/{NUM_PARQUET_FILES}] Error: {e}")
        continue

# Combine all filtered results
if all_filtered:
    filtered_df = pd.concat(all_filtered, ignore_index=True)
else:
    filtered_df = pd.DataFrame(columns=COLUMNS_TO_READ)

print(f"\n{'='*60}")
print(f"TOTAL: {len(filtered_df)} matching records across {NUM_PARQUET_FILES} parquet files")

# ============================================================
# Step 4: Check size and save if < 1GB
# ============================================================
OUTPUT_DIR = "filtered_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Estimate size in memory (rough approximation)
memory_size_bytes = filtered_df.memory_usage(deep=True).sum()
memory_size_gb = memory_size_bytes / (1024**3)

print(f"\nFiltered dataset size: {memory_size_bytes:,} bytes ({memory_size_gb:.4f} GB)")

if memory_size_gb < 1.0:
    output_path = os.path.join(OUTPUT_DIR, f"filtered_{KEYWORD}_{CRAWL}.parquet")
    filtered_df.to_parquet(output_path, index=False)
    print(f"✓ Saved filtered index to: {output_path}")
else:
    print(f"✗ Dataset too large ({memory_size_gb:.2f} GB > 1 GB). Not saving.")

# ============================================================
# Step 5: Function to extract text from WARC using BeautifulSoup
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
        
        # Decompress and parse the WARC record
        decompressed = gzip.decompress(response.content)
        for record in ArchiveIterator(BytesIO(decompressed)):
            # Get raw HTML content
            html = record.content_stream().read().decode('utf-8', errors='ignore')
            
            # Extract text using BeautifulSoup
            soup = BeautifulSoup(html, 'html.parser')
            
            # Remove non-content elements
            for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 'noscript']):
                tag.decompose()
            
            # Get clean text
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
    text_output_dir = os.path.join(output_dir, "text_content")
    os.makedirs(text_output_dir, exist_ok=True)
    
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
            # Save to file
            safe_filename = f"doc_{saved_count:04d}.txt"
            filepath = os.path.join(text_output_dir, safe_filename)
            
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(f"URL: {row['url']}\n")
                f.write(f"Domain: {row['url_host_registered_domain']}\n")
                f.write(f"Language: {row['content_languages']}\n")
                f.write(f"Fetch Time: {row['fetch_time']}\n")
                f.write(f"{'='*60}\n\n")
                f.write(text)
            
            saved_count += 1
    
    print(f"\n{'='*60}")
    print(f"Saved {saved_count} text files to: {text_output_dir}")
    return saved_count

# ============================================================
# Step 6: Demo - fetch text content for matching records
# ============================================================
if len(filtered_df) > 0:
    print("\nSample matching URLs:")
    print(filtered_df[['url', 'content_languages']].head(5).to_string())
    
    # Uncomment below to fetch and save text content from WARC:
    save_text_content(filtered_df, OUTPUT_DIR, max_records=10)
else:
    print("\nNo matching records found. Try adjusting filters or scanning more parquet files.")
