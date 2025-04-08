CREATE OR REPLACE PROCEDURE revert_data_to_OG()
LANGUAGE plpgsql
AS $$
BEGIN    

    -- Revert tbl_subject
    UPDATE adrc.tbl_subject AS s
    SET 
        demographic_language_caregiver = v.demographic_language_caregiver,
        demographic_language_testing = v.demographic_language_testing,
        demographic_marital_status_combo = v.demographic_marital_status_combo,
        demographic_sex_at_birth = v.demographic_sex_at_birth,
        dob = v.dob,
        education_highest = v.education_highest,
        demographic_race = v.demographic_race,
        subject_occupation = v.subject_occupation,
        veteran = v.veteran,
        demographic_gender = v.demographic_gender,
        education_level = v.education_level,
        mmse = v.mmse, 
        lst_moca = v.lst_moca,
        lst_drs = v.lst_drs,
        naccid = v.naccid,
        subject_status_adrc_xfer_name = v.subject_status_adrc_xfer_name,
        subject_status_adrc_xfer = v.subject_status_adrc_xfer,
        cdrglob = v.cdrglob
    FROM public.version_control AS v
    WHERE s.adrc_long_id = v.adrc_long_id
    AND v.migration_id = 1;

    -- Revert tbl_subject_screen
    UPDATE adrc.tbl_subject_screen AS sc
    SET 
        ad_disease_modifying = v.ad_disease_modifying,
        ad_symptomatic = v.ad_symptomatic,
        ms_cancer = v.ms_cancer,
        ms_diabetes_insulindep = v.ms_diabetes_insulindep,
        ms_headtraum_wloss = v.ms_headtraum_wloss,
        ms_psychiatric_dx = v.ms_psychiatric_dx,
        ms_stroke = v.ms_stroke,
        contact_memory_prob = v.screen_contact_memory_prob  -- Alias for field
    FROM public.version_control AS v
    JOIN adrc.tbl_subject AS s ON v.adrc_long_id = s.adrc_long_id
    WHERE sc.subject_id = s.id
    AND v.migration_id = 1;

    -- Revert tbl_recruitment
    UPDATE adrc.tbl_recruitment AS r
    SET 
        referral_source_combo = v.referral_source_combo,
        referral_comments = v.referral_comments
    FROM public.version_control AS v
    JOIN adrc.tbl_subject AS s ON v.adrc_long_id = s.adrc_long_id
    WHERE r.subject_id = s.id
    AND v.migration_id = 1;

    -- Revert tbl_subject_contacts
    UPDATE adrc.tbl_subject_contacts AS c
    SET 
        relationship_with_subject = v.relationship_with_subject
    FROM public.version_control AS v
    JOIN adrc.tbl_subject AS s ON v.adrc_long_id = s.adrc_long_id
    WHERE c.subject_id = s.id 
    AND v.migration_id = 1 
    AND c.contact_type = 'Informant'
    AND c.id = (SELECT MAX(id) FROM adrc.tbl_subject_contacts WHERE adrc.tbl_subject_contacts.subject_id = adrc.subject.id AND adrc.tbl_subject_contacts.contact_type='Informant');

    -- Revert tbl_visits
    UPDATE adrc.tbl_visits AS vis
    SET 
        contact_memory_prob = v.contact_memory_prob,
        contact_memory_prob_onset_yr = v.contact_memory_prob_onset_yr,
        mmse = v.visits_mmse,
        mmse_date = v.visits_mmse_date,
        moca = v.visits_moca,
        moca_date = v.visits_moca_date,
        drs = v.visits_drs,
        drs_date = v.visits_drs_date
    FROM public.version_control AS v
    JOIN adrc.tbl_subject AS s ON v.adrc_long_id = s.adrc_long_id
    WHERE vis.subject_id = s.id 
    AND vis.yr_in_study = v.yr_in_study
    AND v.migration_id = 1;

END $$;
CALL revert_data_to_OG();