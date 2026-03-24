# SpiderSearch 🕷️

A full-stack web crawler and search engine. Enter any URL, SpiderSearch crawls the domain, builds an inverted index, computes PageRank, and lets you search the results — all from a clean web interface.

**Live demo:** [spider-search.net](https://spider-search.net)

---

## Features

- **Web Crawler** — multi-threaded crawler with politeness delays, trap detection, duplicate filtering, and domain scoping
- **Inverted Index** — positional postings with field weighting (title, headings, bold, body) and anchor text
- **Ranked Retrieval** — TF-IDF with cosine normalization, bigram proximity boost, positional phrase matching, and PageRank signal
- **Duplicate Detection** — exact duplicate detection via SHA-1 hashing and near-duplicate detection via SimHash
- **Web Interface** — live crawl progress feed, pipeline stage indicator, and ranked clickable search results

---

## How It Works

```plaintext
URL Input → Crawler → Indexer → Merger → Link Graph → PageRank → Search
```

1. **Crawler** fetches pages within the target domain, saves raw HTML to disk
2. **Indexer** extracts text fields, tokenizes and stems tokens, builds positional postings
3. **Merger** performs a k-way heap merge of partial index files into a single postings file + dictionary
4. **Link Graph** builds a directed hyperlink graph over indexed documents
5. **PageRank** runs iterative damped PageRank over the link graph
6. **Searcher** scores documents using TF-IDF + length normalization + PageRank + phrase/bigram boosts

---

## Project Structure

```plaintext
spidersearch/
├── app.py                        # Flask web app
├── templates/
│   └── index.html                # Single-page UI
├── config/
│   └── crawler.ini               # Crawler configuration
├── src/
│   ├── crawler/                  # Web crawler
│   │   ├── worker.py             # Threaded crawler workers
│   │   ├── frontier.py           # Thread-safe crawl frontier
│   │   ├── scraper.py            # Link extraction and URL filtering
│   │   └── url_filters.py        # Trap detection and URL normalization
│   ├── indexing/                 # Index construction
│   │   ├── build_index.py        # Main indexing pipeline
│   │   ├── accumulator.py        # In-memory postings accumulator
│   │   └── merger.py             # k-way merge of partial index files
│   ├── ranking/                  # Link graph and PageRank
│   │   ├── link_graph_builder.py
│   │   └── pagerank.py
│   ├── search/                   # Retrieval
│   │   ├── searcher.py           # Ranked retrieval pipeline
│   │   ├── dictionary_reader.py
│   │   ├── postings_reader.py
│   │   └── pagerank_reader.py
│   ├── text/                     # Text processing
│   │   ├── tokstem.py            # Tokenization and stemming
│   │   └── html_extractor.py     # Field extraction from HTML
│   ├── quality/                  # Duplicate detection
│   │   ├── duplicate_detector.py
│   │   └── simhash_utils.py
│   └── common/                   # Shared constants and paths
│       ├── paths.py
│       └── search_constants.py
├── requirements.txt
├── Procfile
└── runtime.txt
```

---

## Running Locally

**1. Clone the repo and install dependencies:**

```bash
git clone https://github.com/yourusername/spidersearch.git
cd spidersearch
pip install -r requirements.txt
```

**2. Run the full pipeline:**

```bash
python -m src.main crawl https://books.toscrape.com
python -m src.main build-index
python -m src.main merge
python -m src.main graph
python -m src.main pagerank
```

**3. Start the web interface:**

```bash
python app.py
```

Then open `http://127.0.0.1:8080` in your browser.

**Or run everything via CLI search:**

```bash
python -m src.main search
```

---

## CLI Commands

| Command | Description |
| --- | --- |
| `python -m src.main crawl [url]` | Crawl a domain |
| `python -m src.main build-index` | Build inverted index from crawled pages |
| `python -m src.main merge` | Merge partial index files |
| `python -m src.main graph` | Build hyperlink graph |
| `python -m src.main pagerank` | Compute PageRank scores |
| `python -m src.main search` | Interactive CLI search |
| `python -m src.main pipeline` | Run index → graph → pagerank → search |

---

## Configuration

Edit `config/crawler.ini` to change crawl behavior:

```ini
[IDENTIFICATION]
USERAGENT = SearchBot

[LOCAL PROPERTIES]
THREADCOUNT = 2
SAVE = data/crawl_state/frontier_state

[CRAWLER]
SEEDURL = https://books.toscrape.com
POLITENESS = 0.5
```

Key ranking constants in `src/common/search_constants.py`:

```python
FIELD_WEIGHTS = {
    "title":    3.0,
    "headings": 2.0,
    "bold":     1.5,
    "body":     1.0,
}

PAGERANK_SCORE_WEIGHT = 0.2
PAGERANK_ALPHA        = 0.85
PAGERANK_ITERATIONS   = 20
```

---

## Ranking Model

Documents are scored using a combination of signals:

- **TF-IDF** — log-scaled term frequency × inverse document frequency
- **Cosine normalization** — scores divided by precomputed document vector length
- **Field weighting** — title terms weighted 3×, headings 2×, bold 1.5×, body 1×
- **Soft conjunction bonus** — small reward for documents matching more query terms
- **Bigram boost** — boost for documents containing adjacent query terms
- **Positional phrase boost** — boost when query terms appear consecutively in the document
- **PageRank** — query-independent authority signal blended into final score

---

## Tech Stack

- **Python 3.11**
- **Flask** — web interface
- **BeautifulSoup + lxml** — HTML parsing
- **NLTK Snowball Stemmer** — token stemming
- **Requests** — HTTP fetching

---

## Deployment

Deployed on [Render](https://render.com) with a pre-built index committed to the repository. To update the index, run the pipeline locally and push:

```bash
git add data/
git commit -m "update index"
git push
```

Render auto-redeploys on push.
