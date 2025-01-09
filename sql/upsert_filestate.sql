INSERT INTO file_state
  (path, hash, size, timestamp, modtime)
VALUES
  (:path, :hash, :size, :timestamp, :modtime)
ON CONFLICT
  (path, hash)
DO UPDATE SET
  size=excluded.size
 ,timestamp=excluded.timestamp
 ,modtime=excluded.modtime
;
