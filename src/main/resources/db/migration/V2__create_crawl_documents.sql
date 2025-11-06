-- Crawl Documents Table (for orphan detection)
CREATE TABLE crawl_documents (
  crawl_session_id VARCHAR(100) NOT NULL,
  source_id VARCHAR(500) NOT NULL,
  document_id VARCHAR(100) NOT NULL,
  processed_at TIMESTAMP NOT NULL,
  
  PRIMARY KEY (crawl_session_id, source_id),
  INDEX idx_document_id (document_id),
  
  FOREIGN KEY (crawl_session_id) REFERENCES crawl_sessions(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
