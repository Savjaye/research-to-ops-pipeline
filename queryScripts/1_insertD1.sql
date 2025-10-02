\copy public.tbl_d1_staging FROM 'docker-entrypoint-initdb.d/d1.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '', QUOTE '"');
