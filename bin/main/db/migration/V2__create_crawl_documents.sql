-- Crawl Documents Table (for orphan detection)
CREATE TABLE crawl_documents (
  crawl_session_id VARCHAR(100) NOT NULL,
  source_id VARCHAR(500) NOT NULL,
  document_id VARCHAR(100) NOT NULL,
  processed_at TIMESTAMP NOT NULL,

  PRIMARY KEY (crawl_session_id, source_id),
  FOREIGN KEY (crawl_session_id) REFERENCES crawl_sessions(id) ON DELETE CASCADE
);

CREATE INDEX idx_crawl_documents_document_id ON crawl_documents(document_id);
