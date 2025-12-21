-- Crawl Sessions Table
CREATE TABLE crawl_sessions (
  id VARCHAR(100) PRIMARY KEY,
  datasource_id VARCHAR(100) NOT NULL,
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
  metadata JSONB,

  CONSTRAINT unique_datasource_crawl UNIQUE (datasource_id, crawl_id)
);

CREATE INDEX idx_crawl_sessions_account_id ON crawl_sessions(account_id);
CREATE INDEX idx_crawl_sessions_state ON crawl_sessions(state);
CREATE INDEX idx_crawl_sessions_started_at ON crawl_sessions(started_at);
