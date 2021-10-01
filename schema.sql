DROP TABLE IF EXISTS file_info;
DROP TABLE IF EXISTS file_store;
DROP TABLE IF EXISTS file_chunks;

CREATE TABLE file_info (
  name TEXT PRIMARY KEY,
  contentHash TEXT NOT NULL,
  channels INT,
  framerate INT,
  frames BIGINT,
  duration DOUBLE,
  comptype TEXT,
  uploaded TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE file_store (
  contentHash TEXT PRIMARY KEY,
  content LONGBLOB NOT NULL
);

CREATE TABLE file_chunks (
  name TEXT PRIMARY KEY,
  start INT,
  end INT,
  content BLOB NOT NULL
);