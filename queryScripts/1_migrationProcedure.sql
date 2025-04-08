CREATE OR REPLACE PROCEDURE migrate_research_to_ops()
LANGUAGE plpgsql
AS $$
BEGIN    
    -- move old data from tbl_subject to the tbl_version_control
    INSERT INTO public.version_control(migration_id, subject_id, adrc_long_id, ad_disease_modifying, ad_symptomatic, demographic_language_caregiver,
        demographic_language_testing, demographic_marital_status_combo, demographic_sex_at_birth, dob, education_highest,
        demographic_race, ms_cancer, ms_diabetes_insulindep, ms_headtraum_wloss, ms_psychiatric_dx, ms_stroke,
        subject_occupation, veteran, demographic_gender, education_level, mmse,lst_moca, lst_drs, 
        naccid, subject_status_adrc_xfer, subject_status_adrc_xfer_name, cdrglob,
        screen_contact_memory_prob, contact_memory_prob,
        referral_source_combo, referral_comments, relationship_with_subject,contact_memory_prob_onset_yr, yr_in_study, visits_mmse, visits_mmse_date, visits_moca, visits_moca_date, visits_drs, visits_drs_date, 
        demographic_language_1, demographic_language_1_degree, demographic_language_2, demographic_language_2_degree, demographic_language_3, demographic_language_3_degree, moca_mis
    ) 
    SELECT 
            ( SELECT COALESCE(MAX(migration_id), 0) + 1 FROM public.version_control ), tbl_subject.id AS subject_id, tbl_subject.adrc_long_id, tbl_subject_screen.ad_disease_modifying, 
            tbl_subject_screen.ad_symptomatic, tbl_subject.demographic_language_caregiver, 
            tbl_subject.demographic_language_testing, 
            tbl_subject.demographic_marital_status_combo, tbl_subject.demographic_sex_at_birth, tbl_subject.dob, tbl_subject.education_highest, tbl_subject.demographic_race, 
            tbl_subject_screen.ms_cancer, tbl_subject_screen.ms_diabetes_insulindep, 
            tbl_subject_screen.ms_headtraum_wloss, tbl_subject_screen.ms_psychiatric_dx, tbl_subject_screen.ms_stroke,
            tbl_subject.subject_occupation, tbl_subject.veteran, tbl_subject.demographic_gender, tbl_subject.education_level, 
            tbl_subject.mmse, tbl_subject.lst_moca, tbl_subject.lst_drs, 
            tbl_subject.naccid, tbl_subject.subject_status_adrc_xfer_name, tbl_subject.subject_status_adrc_xfer, tbl_subject.cdrglob,
            tbl_subject_screen.contact_memory_prob AS screen_contact_memory_prob, tbl_visits.contact_memory_prob, tbl_recruitment.referral_source_combo,
            tbl_recruitment.referral_comments, tbl_subject_contacts.relationship_with_subject, tbl_visits.contact_memory_prob_onset_yr, tbl_visits.yr_in_study, tbl_visits.mmse AS visits_mmse, tbl_visits.mmse_date AS visits_mmse_date,
            tbl_visits.moca AS visits_moca, tbl_visits.moca_date AS visits_moca_date, tbl_visits.drs AS visits_drs, tbl_visits.drs_date AS visits_drs_date,
            tbl_subject.demographic_language_1, tbl_subject.demographic_language_1_degree, tbl_subject.demographic_language_2, tbl_subject.demographic_language_2_degree, tbl_subject.demographic_language_3, tbl_subject.demographic_language_3_degree, adrc.tbl_visits.moca_mis AS moca_mis
    FROM adrc.tbl_subject LEFT JOIN adrc.tbl_subject_screen ON adrc.tbl_subject.id = adrc.tbl_subject_screen.subject_id
    LEFT JOIN adrc.tbl_recruitment ON adrc.tbl_subject.id = adrc.tbl_recruitment.subject_id
    LEFT JOIN adrc.tbl_subject_contacts ON adrc.tbl_subject.id = adrc.tbl_subject_contacts.subject_id
        AND adrc.tbl_subject_contacts.contact_type='Informant' AND adrc.tbl_subject_contacts.id  = (SELECT MAX(id) FROM adrc.tbl_subject_contacts WHERE adrc.tbl_subject_contacts.subject_id = adrc.tbl_subject.id AND adrc.tbl_subject_contacts.contact_type='Informant')
    LEFT JOIN adrc.tbl_visits ON adrc.tbl_subject.id = adrc.tbl_visits.subject_id
		AND adrc.tbl_visits.yr_in_study = (SELECT MAX(yr_in_study) FROM adrc.tbl_visits WHERE adrc.tbl_visits.subject_id = adrc.tbl_subject.id)

    WHERE adrc.tbl_subject.adrc_long_id IN (SELECT adrc_long_id FROM public.test);

    UPDATE adrc.tbl_subject AS s
        SET 
        demographic_language_caregiver = COALESCE(t.demographic_language_caregiver, s.demographic_language_caregiver),
        demographic_language_testing = COALESCE(t.demographic_language_testing, s.demographic_language_testing),
        demographic_marital_status_combo = COALESCE(t.demographic_marital_status_combo, s.demographic_marital_status_combo),
        demographic_sex_at_birth = COALESCE(t.demographic_sex_at_birth, s.demographic_sex_at_birth),
        dob = COALESCE(t.dob, s.dob),
        education_highest = COALESCE(t.education_highest, s.education_highest),
        demographic_race = COALESCE(t.demographic_race, s.demographic_race),
        subject_occupation = COALESCE(t.subject_occupation, s.subject_occupation),
        veteran = COALESCE(t.veteran, s.veteran),
        demographic_gender = COALESCE(t.demographic_gender, s.demographic_gender),
        education_level = COALESCE(t.education_level, s.education_level),
        mmse = COALESCE(t.mmse, s.mmse),
        lst_moca = COALESCE(t.lst_moca, s.lst_moca),
        lst_drs = COALESCE(t.lst_drs, s.lst_drs), 
        naccid = COALESCE(t.naccid, s.naccid),
        subject_status_adrc_xfer_name = COALESCE(t.subject_status_adrc_xfer_name, s.subject_status_adrc_xfer_name),
        subject_status_adrc_xfer = COALESCE(t.subject_status_adrc_xfer, s.subject_status_adrc_xfer),
        cdrglob = COALESCE(t.cdrglob, s.cdrglob),
        demographic_language_1 = COALESCE(t.demographic_language_1, s.demographic_language_1),
        demographic_language_1_degree = COALESCE(t.demographic_language_1_degree, s.demographic_language_1_degree), 
        demographic_language_2 = COALESCE(t.demographic_language_2, s.demographic_language_2),
        demographic_language_2_degree = COALESCE(t.demographic_language_2_degree, s.demographic_language_2_degree), 
        demographic_language_3 = COALESCE(t.demographic_language_3, s.demographic_language_3),
        demographic_language_3_degree = COALESCE(t.demographic_language_3_degree, s.demographic_language_3_degree)
    FROM public.test AS t
    WHERE s.adrc_long_id = t.adrc_long_id;

    UPDATE adrc.tbl_subject_screen AS sc
        SET 
        ad_disease_modifying = COALESCE(t.ad_disease_modifying, sc.ad_disease_modifying),
        ad_symptomatic = COALESCE(t.ad_symptomatic, sc.ad_symptomatic),
        ms_cancer = COALESCE(t.ms_cancer, sc.ms_cancer),
        ms_diabetes_insulindep = COALESCE(t.ms_diabetes_insulindep, sc.ms_diabetes_insulindep),
        ms_headtraum_wloss = COALESCE(t.ms_headtraum_wloss, sc.ms_headtraum_wloss),
        ms_psychiatric_dx = COALESCE(t.ms_psychiatric_dx, sc.ms_psychiatric_dx),
        ms_stroke = COALESCE(t.ms_stroke, sc.ms_stroke),
        contact_memory_prob = COALESCE(t.contact_memory_prob, sc.contact_memory_prob)
    FROM public.test AS t
    LEFT JOIN adrc.tbl_subject AS s ON t.adrc_long_id = s.adrc_long_id
    WHERE sc.subject_id = s.id;

    UPDATE adrc.tbl_recruitment AS rt
        SET 
        referral_source_combo = COALESCE(t.referral_source_combo, rt.referral_source_combo),
        referral_comments = COALESCE(t.referral_comments, rt.referral_comments)
    FROM public.test AS t
    LEFT JOIN adrc.tbl_subject AS s ON t.adrc_long_id = s.adrc_long_id
    WHERE rt.subject_id = s.id;

    UPDATE adrc.tbl_subject_contacts AS c
        SET 
        relationship_with_subject = COALESCE(t.relationship_with_subject, c.relationship_with_subject)
    FROM public.test AS t
    LEFT JOIN adrc.tbl_subject AS s ON t.adrc_long_id = s.adrc_long_id
    WHERE c.subject_id = s.id AND c.contact_type = 'Informant';

    UPDATE adrc.tbl_visits AS v
        SET 
        contact_memory_prob = COALESCE(t.contact_memory_prob, v.contact_memory_prob),
        contact_memory_prob_onset_yr = COALESCE(t.visit_date, v.contact_memory_prob_onset_yr),
        mmse = COALESCE(t.mmse, v.mmse),
        mmse_date = COALESCE(t.c1_visit_date, v.mmse_date),
        moca_date = COALESCE(t.c1_visit_date, v.moca_date),
        moca =COALESCE(NULLIF(t.lst_moca, '') :: INTEGER, v.moca), -- lst_moca is stored as a VARCHAR in tbl_subject so we need to convert it to an integer
        drs = COALESCE(t.lst_drs, v.drs),
        drs_date = COALESCE(t.c1_visit_date, v.drs_date),
        moca_mis = COALESCE(t.moca_mis, v.moca_mis)
    FROM public.test AS t
    LEFT JOIN adrc.tbl_subject AS s ON t.adrc_long_id = s.adrc_long_id
    WHERE v.subject_id = s.id AND v.yr_in_study=t.yr_in_study;

    --DELETE FROM public.test;

END $$;
