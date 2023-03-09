CREATE TABLE IF NOT EXISTS pr (
  id INT AUTO_INCREMENT PRIMARY KEY,
  pr_id INT,
  recordTimestamp TIMESTAMP,
  additions INT,
  deletions INT,
  author VARCHAR(64),
  state VARCHAR(64),
  createdAt TIMESTAMP,
  updatedAt TIMESTAMP,
  closedAt TIMESTAMP,
  title TEXT,
  url TEXT,
  body TEXT,
  owner VARCHAR(64),
  repo VARCHAR(64),
  reviews JSON
);
