# Common Crawl Parquet Index Query

Query Common Crawl data efficiently using the columnar (Parquet) index instead of downloading entire WET files.
---

> ## Related CC-News Datasets on Hugging Face
>
> Before building a custom CC-MAIN pipeline, you may also consider existing curated datasets:
>
> - [vblagoje/cc_news](https://huggingface.co/datasets/vblagoje/cc_news) – A cleaned and structured subset of CC News articles.
> - [intfloat/multilingual_cc_news](https://huggingface.co/datasets/intfloat/multilingual_cc_news) – A multilingual version of CC News.
> - [stanford-oval/ccnews](https://huggingface.co/datasets/stanford-oval/ccnews) – Stanford OVAL’s processed CC News dataset.
>
> These datasets provide ready-to-use news corpora, while this repository focuses on building custom corpora directly from raw CC-MAIN index + WARC files.

## Why Use the Parquet Index?

| Old Approach | New Approach (Parquet Index) |
|-------------|------------------------------|
| Download entire WET files (~GB each) | Query parquet index (~50-100 MB per partition) |
| Process everything sequentially | Filter by domain/TLD/URL first |
| No way to target specific URLs | Fetch only specific WARC records using byte-range |

## How It Works

| Step | What it does | Data size |
|------|-------------|-----------|
| 1. Get paths | Download `cc-index-table.paths.gz` and extract all Parquet index shard paths for the selected crawl (keep `subset=warc/`). | ~1 KB |
| 2. Read parquet | Read selected columns from one Parquet index shard (each row = one crawled URL record). | ~200–400 MB per shard (remote) |
| 3. Filter | Apply Boolean filters on URL (regex), language (`eng`), and MIME type (`text/html`). | In memory (metadata only) |
| 4. Aggregate | Combine matches across scanned shards and optionally save filtered metadata as a Parquet file. | Depends on match size |
| 5. Fetch WARC | Download only specific WARC byte ranges for matched records (no full WARC download). | Only requested bytes |
| 6. Extract text | Parse HTML, remove non-content tags, and save clean text as individual `.txt` files. | Few KB–MB per page |

## Installation

```bash
pip install pyarrow pandas requests warcio fsspec
```

## Usage

Run the script:

```bash
python test.py
```

## Key Features

- **Column projection**: Only downloads the columns you need from parquet files
- **HTTP range requests**: Fetches specific WARC records without downloading full files
- **Partition-based**: Each parquet file contains ~15M records organized by URL

## Available Columns

The parquet index includes:
- `url` - Full URL
- `url_host_tld` - Top-level domain (com, org, nl, etc.)
- `url_host_registered_domain` - Domain name (example.com)
- `warc_filename` - Path to WARC file
- `warc_record_offset` - Byte offset in WARC file
- `warc_record_length` - Length of record
- `content_mime_type` - MIME type (text/html, etc.)

## Resources

- [Common Crawl Columnar Index Blog Post](https://commoncrawl.org/blog/index-to-warc-files-and-urls-in-columnar-format)
- [cc-index-table GitHub](https://github.com/commoncrawl/cc-index-table)
- [Browse crawl data](https://data.commoncrawl.org/crawl-data/index.html)

## Output of an example run
```text
Fetching parquet paths from: https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-10/cc-index-table.paths.gz
Found 300 parquet files for warc subset
Will scan first 2 files for keyword 'immigrant'
  [1/2] Found 90 matches 
  [2/2] Found 69 matches 
Scanning parquet files: 100%|████████████████████████████████████████████████████████████████████████████████████████████| 2/2 [09:42<00:00, 291.37s/it]
============================================================
TOTAL: 159 matching records across 2 parquet files

Filtered dataset size: 95,933 bytes (0.0001 GB)
✓ Saved filtered index to: filtered_data/filtered_immigrant_CC-MAIN-2024-10.parquet

Sample matching URLs:
url content_languages
0  https://governor.wa.gov/news/2021/washington-covid-19-immigrant-relief-fund-opens-new-applications               eng
1  https://www.hum.wa.gov/news/2022/08/update-wenatchee-immigrant-justice           eng,spa
2  https://ofm.wa.gov/pubs-reports/washington-states-immigrant-population-2010-17               eng
3  https://www.whitehouse.gov/briefing-room/presidential-actions/2021/01/25/proclamation-on-the-suspension-of-entry-as-immigrants-and-non-immigrants-of-certain-additional-persons-who-pose-a-risk-of-transmitting-coronavirus-disease/               eng
4  https://www.whitehouse.gov/briefing-room/presidential-actions/2021/04/30/a-proclamation-on-the-suspension-of-entry-as-nonimmigrants-of-certain-additional-persons-who-pose-a-risk-of-transmitting-coronavirus-disease-2019/               eng
============================================================
Extracting text from 10 WARC records...
(Using byte-range requests - efficient random access)

Fetching WARC records: 100%|████████████████████████████████████████████████████████████████████████████████████████████| 10/10 [00:12<00:00,  1.23s/it]
============================================================
Saved 10 text files to: filtered_data/text_content
```

## Example of one text_content
```text
URL: https://dpi.wi.gov/english-learners/immigrants-and-refugees/immigrant-children
Domain: wi.gov
Language: eng
Fetch Time: 2024-02-22 22:11:19+00:00
============================================================

Immigrant Children and Youth | Wisconsin Department of Public Instruction
Skip to main content
You are here
Bilingual/ESL Program
Immigrants and Refugees
Immigrant Children and Youth
Immigrant Children and Youth
Title III, Part A, Immigrant Children and Youth Discretionary Grant
Purpose, Background, and Eligibility Requirements
The Wisconsin Department of Public Instruction (DPI) reserves approximately five percent of its Title III to provide enhanced instructional opportunities to Immigrant Children and Youth. Local Education Agencies (LEAs) that have seen a significant increase in immigrant children and youth may apply for the discretionary grant. Qualifying LEAs must have demonstrated a significant increase in their Immigrant and Youth population[i]. For Immigrant and Youth eligibility, a significant increase is defined as at least a 25 percent increase in the current year for the number of Immigrant and Youth over the average of the previous two years.
Safeguarding Immigrant children and Youth Data Collection
LEA eligibility is determined by Immigrant and Youth data reported for the Third Friday Count in WISEdata via the LEA’s Student Information System to WISEdata. The data requested is defined by Section 3301(6) of Title III. Immigrant children and youth are ages 3 through 21, were not born in any state, and have not been attending one or more schools in any one or more states for more than three full academic years[ii]. States are defined as the 50 states, the District of Columbia, and the Commonwealth of Puerto Rico. Immigrants may be adoptees, foreign exchange students, and refugee students, born in the US military, among others. Immigrants may or may not be English Learners (ELs) as defined by ESSA.[iii] Immigrant and Youth Discretionary Grant funds may be used for activities that provide enhanced instructional opportunities such as[iv]:
A) Family literacy, parent and family outreach, and the training activities designed to assist parents and families to become active participants in their children’s education.
B) Recruit and support personnel, including teachers and paraprofessionals who have been specifically trained, or are being trained, to provide services to immigrant children and youth.
C) Provide tutorials, mentoring, and academic and career counseling.
D) Identify, develop, and acquire curricular materials, educational software, and technologies to be used in the program to carry out with these grant funds.
E) Basic instructional services that are directly attributable to the presence of immigrant children and youth in the LEA, including payment of costs for added classroom supplies, transportation costs, etc.
F) Other instructional services that are designed to assist immigrant children and youth to achieve in U.S. elementary and secondary schools, such as school orientation or civics education.
G) Activities in coordination with community-based organizations, institutions of higher education, private section entities, or other entities with expertise in working with immigrants, to assist families by offering comprehensive services.
Application and Eligibility
Eligible LEAs and Title III Consortia may apply. Within the grant application, the application should describe how any activities undertaken will explicitly support immigrant children and youth. Additional pages may be added to the application. Districts should anticipate being able to serve immigrants during the grant year. The grant year runs from July 1, 2021, through June 30, 2022. The grants must be expended during the fiscal year awarded. Title III Immigrant and Youth funding do not carry over. Grant requests have traditionally ranged from $10,000 to $40,000 for eligible grant activities.
2024-2025 Title III- Immigrant Children and Youth Discretionary Grant Information
2024-2025 Competion will open March 1st,  Check back for more information!
The 2023-2024 competition list of awarded LEAS.
2024-2025 Immigrant Children and Youth Grant Qualifying LEAs
List of
Qualifying LEAs
List of
Qualifying LEAs (CESA Region Sort)
2024-2025  Immigrant Children and Youth Grant Application
NOTE:  When completing and submitting an application, data should be kept
confidential. No confidential student data should be submitted.
List of Eligible Activities for Immigrant Children and Youth
2024-2025   Consortium Verification Form
NOTE:  This document is required for those applying as a consortium and must be
submitted with the grant application.
2024-2025 Immigrant Children and Youth Grant Rubric
Inquiries about the Immigrant Children and Youth grant can be directed to:
Amy.Maciolek@dpi.wi.gov
or (608) 266-1570.
[i] ESEA 20 U.S.C. § 3114(d)(1)
[ii] Academic Year need not be consecutive.
[iii] ESSA definition of English Learner plus citation.
[iv] ESEA 20 U.S.C. § 3114(e)(1)
```