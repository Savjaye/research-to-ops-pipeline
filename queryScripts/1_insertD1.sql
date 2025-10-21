\copy public.tbl_d1_staging FROM '/home/adrc-admin/adrc/deliverables/sjhScriptsQueries/tables/d1.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '', QUOTE '"');
