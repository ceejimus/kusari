CREATE TABLE IF NOT EXISTS file_state (
  path TEXT
 ,hash NVARCHAR(32)
 ,size INTEGER
 ,timestamp INTEGER
 ,modtime INTEGER
 ,PRIMARY KEY(path, hash)
);
