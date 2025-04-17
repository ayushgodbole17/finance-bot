# Finance News Summarizer & Q&A

## Structure
- ingest/: RSS → S3
- index/: S3 → embeddings → OpenSearch
- api/: FastAPI endpoints
- web/: HTML/JS client