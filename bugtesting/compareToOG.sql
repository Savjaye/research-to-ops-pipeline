SELECT 
    s.adrc_long_id,
    
    -- tbl_subject comparisons
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
    v.lst_drs AS version_lst_drs,
    s.subject_status_adrc_xfer_name AS subject_subject_status_adrc_xfer_name,
    v.subject_status_adrc_xfer_name AS version_subject_status_adrc_xfer_name,
    s.subject_status_adrc_xfer AS subject_subject_status_adrc_xfer,
    v.subject_status_adrc_xfer AS version_subject_status_adrc_xfer,
    s.cdrglob AS subject_cdrglob,
    v.cdrglob AS version_cdrglob,
    s.naccid AS subject_naccid,
    v.naccid AS version_naccid,


    
    -- tbl_subject_screen comparisons
    sc.ad_disease_modifying AS subject_screen_ad_disease_modifying,
    v.ad_disease_modifying AS version_screen_ad_disease_modifying,
    sc.ad_symptomatic AS subject_screen_ad_symptomatic,
    v.ad_symptomatic AS version_screen_ad_symptomatic,
    sc.ms_cancer AS subject_screen_ms_cancer,
    v.ms_cancer AS version_screen_ms_cancer,
    sc.ms_diabetes_insulindep AS subject_screen_ms_diabetes_insulindep,
    v.ms_diabetes_insulindep AS version_screen_ms_diabetes_insulindep,
    sc.ms_headtraum_wloss AS subject_screen_ms_headtraum_wloss,
    v.ms_headtraum_wloss AS version_screen_ms_headtraum_wloss,
    sc.ms_psychiatric_dx AS subject_screen_ms_psychiatric_dx,
    v.ms_psychiatric_dx AS version_screen_ms_psychiatric_dx,
    sc.ms_stroke AS subject_screen_ms_stroke,
    v.ms_stroke AS version_screen_ms_stroke,
    sc.contact_memory_prob AS subject_screen_contact_memory_prob,
    v.screen_contact_memory_prob AS version_screen_contact_memory_prob,

    -- tbl_recruitment comparisons
    r.referral_source_combo AS subject_recruitment_referral_source,
    v.referral_source_combo AS version_recruitment_referral_source,
    r.referral_comments AS subject_recruitment_referral_comments,
    v.referral_comments AS version_recruitment_referral_comments,

    -- tbl_subject_contacts comparisons
    c.relationship_with_subject AS subject_contact_relationship,
    v.relationship_with_subject AS version_contact_relationship,

    -- tbl_visits comparisons
    vts.contact_memory_prob AS subject_visits_contact_memory_prob,
    v.contact_memory_prob AS version_visits_contact_memory_prob,
    vts.contact_memory_prob_onset_yr AS subject_visits_contact_memory_prob_onset_yr,
    v.contact_memory_prob_onset_yr AS version_visits_contact_memory_prob_onset_yr,

    vts.mmse AS subject_visits_mmse,
    v.visits_mmse AS version_visits_mmse,
    vts.mmse_date AS subject_visits_mmse_date,
    v.visits_mmse_date AS version_visits_mmse_date,
    vts.moca AS subject_visits_moca,
    v.visits_moca AS version_visits_moca,
    vts.moca_date AS subject_visits_moca_date,
    v.visits_moca_date AS version_visits_moca_date,
    vts.drs AS subject_visits_drs,
    v.visits_drs AS version_visits_drs,
    vts.drs_date AS subject_visits_drs_date,
    v.visits_drs_date AS version_visits_drs_date

FROM adrc.tbl_subject AS s
JOIN public.version_control AS v 
    ON s.adrc_long_id = v.adrc_long_id
    AND v.migration_id = 1
LEFT JOIN adrc.tbl_subject_screen AS sc 
    ON s.id = sc.subject_id
LEFT JOIN adrc.tbl_recruitment AS r 
    ON s.id = r.subject_id
LEFT JOIN adrc.tbl_subject_contacts AS c 
    ON s.id = c.subject_id AND c.contact_type = 'Informant'
LEFT JOIN adrc.tbl_visits AS vts 
    ON s.id = vts.subject_id
    AND vts.yr_in_study = (SELECT MAX(yr_in_study) FROM adrc.tbl_visits WHERE subject_id = s.id)

WHERE 
    -- tbl_subject mismatches
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
    OR s.lst_drs IS DISTINCT FROM v.lst_drs
    OR s.subject_status_adrc_xfer_name IS DISTINCT FROM v.subject_status_adrc_xfer_name
    OR s.subject_status_adrc_xfer IS DISTINCT FROM v.subject_status_adrc_xfer
    OR s.cdrglob IS DISTINCT FROM v.cdrglob
    OR s.naccid IS DISTINCT FROM v.naccid
    
    -- tbl_subject_screen mismatches
    OR sc.ad_disease_modifying IS DISTINCT FROM v.ad_disease_modifying
    OR sc.ad_symptomatic IS DISTINCT FROM v.ad_symptomatic
    OR sc.ms_cancer IS DISTINCT FROM v.ms_cancer
    OR sc.ms_diabetes_insulindep IS DISTINCT FROM v.ms_diabetes_insulindep
    OR sc.ms_headtraum_wloss IS DISTINCT FROM v.ms_headtraum_wloss
    OR sc.ms_psychiatric_dx IS DISTINCT FROM v.ms_psychiatric_dx
    OR sc.ms_stroke IS DISTINCT FROM v.ms_stroke
    OR sc.contact_memory_prob IS DISTINCT FROM v.screen_contact_memory_prob

    -- tbl_recruitment mismatches
    OR r.referral_source_combo IS DISTINCT FROM v.referral_source_combo
    OR r.referral_comments IS DISTINCT FROM v.referral_comments

    -- tbl_subject_contacts mismatches
    OR c.relationship_with_subject IS DISTINCT FROM v.relationship_with_subject

    -- tbl_visits mismatches
    OR vts.contact_memory_prob IS DISTINCT FROM v.contact_memory_prob
    OR vts.contact_memory_prob_onset_yr IS DISTINCT FROM v.contact_memory_prob_onset_yr
    OR vts.mmse IS DISTINCT FROM v.visits_mmse
    OR vts.mmse_date IS DISTINCT FROM v.visits_mmse_date
    OR vts.moca IS DISTINCT FROM v.visits_moca
    OR vts.moca_date IS DISTINCT FROM v.visits_moca_date
    OR vts.drs IS DISTINCT FROM v.visits_drs
    OR vts.drs_date IS DISTINCT FROM v.visits_drs_date;
	