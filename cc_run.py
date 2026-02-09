from ccTool import (
    get_warc_parquet_paths,
    resolve_num_parquet_files,
    scan_and_filter_index_duckdb,
    save_filtered_index_if_small,
    load_filtered_index,
    save_text_content
)

# -----------------------------
# Configuration
# -----------------------------
CRAWL = "CC-MAIN-2024-10"  # Change to your desired crawl
KEYWORDS = ["immigrant", "immigration"]  # List of keywords to match in URL (case-insensitive)
SCAN_ALL_PARQUET = False  # Set to True to scan ALL parquet files
NUM_PARQUET_FILES = 1     # Number of parquet to scan if SCAN_ALL_PARQUET = False

output_path_parquet = f"filtered_data/test_CC-MAIN-2024-10.parquet"
output_dir_text = "filtered_data"

COLUMNS_TO_READ = [
    'url',
    'url_host_tld',
    'url_host_registered_domain',
    'warc_filename',
    'warc_record_offset',
    'warc_record_length',
    'content_mime_type',
    'content_languages',
    'fetch_time',
]

# -----------------------------
# Steps
# -----------------------------
warc_paths = get_warc_parquet_paths(CRAWL)

NUM_PARQUET_FILES = resolve_num_parquet_files(
    warc_paths,
    SCAN_ALL_PARQUET,
    NUM_PARQUET_FILES,
    KEYWORDS
)

filtered_df = scan_and_filter_index_duckdb(
    warc_paths,
    keywords=KEYWORDS,
    num_parquet_files=NUM_PARQUET_FILES,
    columns_to_read=COLUMNS_TO_READ,
)

save_filtered_index_if_small(
    filtered_df,
    output_path=output_path_parquet
)

### Load previously saved filtered index
#filtered_df = load_filtered_index(output_path_parquet)

if len(filtered_df) > 0:
    print("\nSample matching URLs:")
    print(filtered_df[['url', 'content_languages']].head(5).to_string())

    save_text_content(filtered_df, output_dir_text, max_records=3)
else:
    print("\nNo matching records found.")
