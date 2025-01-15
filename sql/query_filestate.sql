SELECT
  path
 ,hash
 ,size
 ,timestamp
 ,modtime
FROM
  file_state
WHERE
  path = :path
;
