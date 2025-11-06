-- Crawl Sessions Table
CREATE TABLE crawl_sessions (
  id VARCHAR(100) PRIMARY KEY,
  connector_id VARCHAR(100) NOT NULL,
  crawl_id VARCHAR(100) NOT NULL,
  account_id VARCHAR(100) NOT NULL,
  state VARCHAR(50) NOT NULL,
  started_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP,
  last_heartbeat TIMESTAMP,
  
  -- Statistics
  documents_found INT DEFAULT 0,
  documents_processed INT DEFAULT 0,
  documents_failed INT DEFAULT 0,
  documents_skipped INT DEFAULT 0,
  bytes_processed BIGINT DEFAULT 0,
  
  -- Configuration
  track_documents BOOLEAN DEFAULT FALSE,
  delete_orphans BOOLEAN DEFAULT FALSE,
  
  -- Metadata
  connector_type VARCHAR(50),
  source_system VARCHAR(500),
  metadata JSON,
  
  UNIQUE KEY unique_connector_crawl (connector_id, crawl_id),
  INDEX idx_account_id (account_id),
  INDEX idx_state (state),
  INDEX idx_started_at (started_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
