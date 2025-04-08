SELECT 
    s.adrc_long_id,
    s.demographic_language_caregiver AS subject_language_caregiver, 
    v.demographic_language_caregiver AS version_language_caregiver,
    s.demographic_language_testing AS subject_language_testing, 
    v.demographic_language_testing AS version_language_testing,
    s.demographic_marital_status_combo AS subject_marital_status, 
    v.demographic_marital_status_combo AS version_marital_status,
    s.demographic_sex_at_birth AS subject_sex_at_birth, 
    v.demographic_sex_at_birth AS version_sex_at_birth,
    s.dob AS subject_dob, 
    v.dob AS version_dob,
    s.education_highest AS subject_education_highest, 
    v.education_highest AS version_education_highest,
    s.demographic_race AS subject_race, 
    v.demographic_race AS version_race,
    s.subject_occupation AS subject_occupation, 
    v.subject_occupation AS version_occupation,
    s.veteran AS subject_veteran, 
    v.veteran AS version_veteran,
    s.demographic_gender AS subject_gender, 
    v.demographic_gender AS version_gender,
    s.education_level AS subject_education_level, 
    v.education_level AS version_education_level,
    s.mmse AS subject_mmse, 
    v.mmse AS version_mmse,
    s.lst_moca AS subject_lst_moca, 
    v.lst_moca AS version_lst_moca,
    s.lst_drs AS subject_lst_drs, 
    v.lst_drs AS version_lst_drs
FROM adrc.tbl_subject AS s
JOIN public.version_control AS v 
    ON s.adrc_long_id = v.adrc_long_id
    AND v.migration_id = (SELECT MAX(migration_id) FROM public.version_control)
WHERE 
    s.demographic_language_caregiver IS DISTINCT FROM v.demographic_language_caregiver
    OR s.demographic_language_testing IS DISTINCT FROM v.demographic_language_testing
    OR s.demographic_marital_status_combo IS DISTINCT FROM v.demographic_marital_status_combo
    OR s.demographic_sex_at_birth IS DISTINCT FROM v.demographic_sex_at_birth
    OR s.dob IS DISTINCT FROM v.dob
    OR s.education_highest IS DISTINCT FROM v.education_highest
    OR s.demographic_race IS DISTINCT FROM v.demographic_race
    OR s.subject_occupation IS DISTINCT FROM v.subject_occupation
    OR s.veteran IS DISTINCT FROM v.veteran
    OR s.demographic_gender IS DISTINCT FROM v.demographic_gender
    OR s.education_level IS DISTINCT FROM v.education_level
    OR s.mmse IS DISTINCT FROM v.mmse
    OR s.lst_moca IS DISTINCT FROM v.lst_moca
    OR s.lst_drs IS DISTINCT FROM v.lst_drs;