import subprocess
from pathlib import Path
import pandas as pd
from functools import reduce
import combo_mappings 

# --- Notes for Adding New Fields ---
# 1. Update the template file (tables/template/template.csv).
# 2. Update public.test schema if needed.
# 3. Add the field to 'trac_var_name' (variable in this script that ensure the output (outv1.csv matches the schema of the read-in table)).
# 4. If pulling from a new SDSC table, update readSourceData().
# 5. If inserting into a new TRAC table, update SQL procedures.


# --- this functions flips through each of the queries in './queryScripts' and executes them locally on the server using ./operationResearchMigration ---
def executeQueryLocaly(queryPath, scriptPath, outPath=None):

    queries_dir = Path(queryPath)
    script_path = scriptPath

    if outPath:
        out_dir = outPath

    query_files = sorted(queries_dir.rglob("*.sql"), key=lambda f: f.name)

    for query in query_files:
        
        # if there is an output driectory specifies, pass it to the script 
        if outPath:
            cmd = [str(script_path), str(query), out_dir]
        else:
                cmd = [str(script_path), str(query)]
        print(f"Enter Password To  Run {query} Command Locally")
        subprocess.run(cmd, check=True)

# ---- Reads raw CSV's, preprocesses (is necessary), merges into single dataframe, selects data associated with most recent year ----
def readSourceData(Template):
     
     # save cross walk to variable
     tmp = Template
     
     # read tables*
     roster = pd.read_csv("./tables/sourceTables/uds_roster.csv", low_memory=False, na_values=-4)
     a1 = pd.read_csv("./tables/sourceTables/uds_a1subdemo.csv", low_memory=False, na_values=-4) 
     a4a = pd.read_csv("./tables/sourceTables/uds_a4ard.csv", low_memory=False, na_values=-4)
     c1 = pd.read_csv("./tables/sourceTables/uds_c1npsyb.csv", low_memory=False, na_values=-4)
     
     a3 = pd.read_csv("./tables/sourceTables/uds_a3xfhs.csv", low_memory=False, na_values=-4)
     # pre-processing of the A3 is necessary because the information within a row is not sufficient to calculate ms_famhx
     # we need information across rows because an entery in made for each family member 

     a3 = a3[["RID", "FHS01ETPR", "FHS01ETSEC"]]

     # ---- preprocessing of A3: we need to create an indicator by looking across rows before we send it to be transformed. Transformation function looks across columns only ---
     # don't group by 'VISCODE'... if someone has reported family history at any point, it should be 'ms_famhx' should be 'Yes' irrespective of the year
     a3 = a3.groupby(["RID"]).agg(lambda x: 1 if bool(x.isin([1, 2, 3, 5, 6, 11]).any()) else 0 if x.notna().any() else 9).reset_index()
     a3.to_csv("./tables/output/a3TEST.csv", index= False)


     a5 = pd.read_csv("./tables/sourceTables/uds_a5subhst.csv", low_memory=False, na_values=-4)
     # --- pre-process the A5 because it used to be collected at year one only ----
     a5["VISIT"] = a5["VISCODE"].str.extract(r"y(\d+)", expand=False).astype(int)
     a5 = a5.loc[a5.groupby("RID")["VISIT"].idxmax()]
     a5 = a5.drop("VISCODE", axis=1)

     a2 = pd.read_csv("./tables/sourceTables/uds_a2infdemo.csv", low_memory=False, na_values=-4)
     registry = pd.read_csv("./tables/sourceTables/uds_registry.csv", low_memory=False, na_values=-4)
     b4 = pd.read_csv("./tables/sourceTables/uds_b4cdr.csv", low_memory=False, na_values=-4)
     enroll = pd.read_csv("./tables/sourceTables/uds_naccenroll.csv", low_memory=False, na_values=-4)
     d1 = pd.read_csv("./tables/sourceTables/uds_d1clindx.csv", low_memory=False, na_values=-4)
     lhq = pd.read_csv("./tables/sourceTables/uds_lhqrev.csv", low_memory=False, na_values=-4)
    
    # --- organize all the loaded dataframes ---
     df_dict = {
                "roster" : roster, 
                "a1" : a1, 
                "a4a" : a4a, 
                "c1" : c1, 
                "a5" : a5, 
                "a2" : a2,
                "a3" : a3,
                "registry" : registry,
                "b4" : b4,
                "enroll" : enroll, 
                "lhq" : lhq
     }
    # --- subset each table to extract only the required fields (as defined in template.csv)
     for name, df in df_dict.items(): # for each sdsc_table
          df_fields = tmp[tmp["sdsc_table"].str.lower() == name]["sdsc_var_name"] # grab columns (as defined in tmp) associated with each sdsc_table
          df = df_dict[name][df_fields] # subset each sdsc_table  to get only the needed rows
          df_dict[name] = df # save the subsetted data to the dict 
    
    # Now perform individual merges:
     print("!! GETTING ERROR ON MERGE? - Did you add the columns you are joining ON to the template (tmp)?")
     df_temp1 = pd.merge(df_dict["a1"], df_dict["c1"], on=["RID", "VISCODE"], how="outer")
     df_temp2 = pd.merge(df_temp1, df_dict["a4a"], on=["RID", "VISCODE"], how="outer")
     df_temp3 = pd.merge(df_temp2, df_dict["a5"], on=["RID"], how="outer")
     df_temp4 = pd.merge(df_temp3, df_dict["a2"], on=["RID", "VISCODE"], how="outer")
     df_temp5 = pd.merge(df_temp4, df_dict["registry"], on=["RID", "VISCODE"], how="outer")
     df_temp6 = pd.merge(df_temp5, df_dict["b4"], on=["RID", "VISCODE"], how="outer")
     df_temp7 = pd.merge(df_temp6, df_dict["enroll"], on=["RID"], how="outer")
     df_temp8 = pd.merge(df_temp7, df_dict["lhq"], on=["RID", "VISCODE"], how="outer")
     df_temp9 = pd.merge(df_temp8, df_dict["a3"], on=["RID"], how="outer")
     source_df = pd.merge(df_temp9, df_dict["roster"], on="RID", how="outer")

     # grab out the data associated with the most recent year
     source_df = source_df.dropna(subset=["VISCODE"]) # remove anyone with a missing year
     source_df["VISIT"] = source_df["VISCODE"].str.extract(r"y(\d+)", expand=False).astype(int) # extract their visit year 
     most_recent_vis_source_df = source_df.loc[source_df.groupby("RID")["VISIT"].idxmax()] # grad row associated with most recent visit year
     most_recent_vis_source_df.dropna(subset="REGTRYID", inplace = True)

     # export for bug testing 
     most_recent_vis_source_df.to_csv("./tables/output/sdsc.csv", index= False)
     print("SDSC Data Accessed")
     return most_recent_vis_source_df
     
# --- Applies transformation mappings to match TRAC schema ---
def transformSourceData(Template, source_df):

    tmp = Template

    # rename columns
    valid_renames = tmp[tmp["trac_var_name"].notna() & tmp["sdsc_var_name"].notna()]
    rename_dict = dict(zip(valid_renames["sdsc_var_name"], valid_renames["trac_var_name"]))
    source_df.rename(columns=rename_dict, inplace=True)


    # add calculated variables
    calculated_vars = [field for field in tmp["trac_var_name"].unique() if tmp.loc[tmp["trac_var_name"] == field, "transformation"].notna().all() and pd.notnull(field)]
    print(f"calculated variables: {calculated_vars}")
    for field in calculated_vars:
         print(field)
         transformation_code = tmp[tmp["trac_var_name"] == field]["transformation"].iloc[0]

         combo_context = {
            "pd": pd,
            "row": None,
            "MARITAL_STATUS_MAP": combo_mappings.MARITAL_STATUS_MAP,
            "BIRTHSEX_MAP": combo_mappings.BIRTHSEX_MAP,
            "LVLEDUC_MAP": combo_mappings.LVLEDUC_MAP,
            "VETERAN_MAP": combo_mappings.VETERAN_MAP,
            "MEMWORS_MAP": combo_mappings.MEMWORS_MAP,
            "REFERSC_MAP": combo_mappings.REFERSC_MAP,
            "LANG_MAP": combo_mappings.LANG_MAP,
            "INRELTO_MAP": combo_mappings.INRELTO_MAP,
            "YES_NO_MAP": combo_mappings.YES_NO_MAP,
            "CANCER_DIABETES_STATUS_MAP": combo_mappings.CANCER_DIABETES_STATUS_MAP,
            "LANGUAGE_PROFICIENCY_MAP" : combo_mappings.LANGUAGE_PROFICIENCY_MAP
            }
         source_df[field] = source_df.apply(lambda row: eval(transformation_code, {**combo_context, "row": row}),axis=1)
         #for index, row in source_df.iterrows():
            #source_df.at[index, field] = eval(transformation_code)

    # remove the fields that dont have a direct mapping ex: RID, VISCODE, any fields that are combined with others 
    sdsc_fields_without_trac_mapping = [field for field in tmp["sdsc_var_name"] if tmp[tmp["sdsc_var_name"]==field]["trac_var_name"].isnull().all() and pd.notnull(field) and field != "VISITDATE"] # VISITDATE will be converted to VISITDATE_x and VISITDATE_y because we need to different ones
    print(f"MISSING MAP {sdsc_fields_without_trac_mapping}")
    #remove RID column 
    source_df.drop(columns=sdsc_fields_without_trac_mapping, inplace=True)



    # select out proper field names and order correctly -- **note: this is not dynamically sourced to ensure proper order and compatibility with SQL tbl_test... its picky
    trac_var_names = [
    "adrc_long_id",
    "ad_disease_modifying",
    "ad_symptomatic",
    "demographic_language_caregiver",
    "demographic_language_testing",
    "demographic_marital_status_combo",
    "demographic_sex_at_birth",
    "dob",
    "education_highest",
    "demographic_race",
    "ms_cancer",
    "ms_diabetes_insulindep",
    "ms_headtraum_wloss",
    "ms_psychiatric_dx",
    "ms_stroke",
    "subject_occupation",
    "veteran",
    "demographic_gender",
    "education_level", 
    "mmse",
    "lst_drs", 
    "lst_moca",
    "contact_memory_prob", 
    "referral_source_combo",
    "referral_comments",
    "relationship_with_subject",
    "visit_date",
    "yr_in_study",
    "c1_visit_date",
    "cdrglob",
    "naccid",
    "subject_status_adrc_xfer_name",
    "subject_status_adrc_xfer",
    "demographic_language_1",
    "demographic_language_1_degree",
    "demographic_language_2",
    "demographic_language_2_degree",
    "demographic_language_3",
    "demographic_language_3_degree",
    "moca_mis",
    "ms_famhxad"
]
    source_df = source_df[trac_var_names]
    #print(source_df["demographic_marital_status_combo"])

    # Convert all float64 columns that have only whole numbers to Int64
    source_df = source_df.convert_dtypes()
    for col in source_df.select_dtypes(include=["float64"]).columns:
        if (source_df[col].dropna() % 1 == 0).all():  # Check if all non-null values are whole numbers
            source_df[col] = source_df[col].astype("Int64")  # Convert to Int64

    source_df.to_csv("./tables/output/outv1.csv", index=False, date_format="%Y-%m-%d")

    #test for missing or extra columns - this is a not an essential step to maintain but is very helpful for bug testing when updating 
    expected_cols = [
    "demographic_gender",
    "demographic_marital_status_combo",
    "demographic_sex_at_birth",
    "education_highest",
    "education_level",
    "demographic_race",
    "subject_occupation",
    "veteran",
    "contact_memory_prob",
    "referral_source_combo",
    "referral_comments",
    "demographic_language_caregiver",
    "relationship_with_subject",
    "ad_disease_modifying",
    "ad_symptomatic",
    "ms_cancer",
    "ms_diabetes_insulindep",
    "ms_headtraum_wloss",
    "ms_psychiatric_dx",
    "ms_stroke",
    "cdrglob",
    "demographic_language_testing",
    "mmse",
    "lst_moca",
    "lst_drs",
    "subject_status_adrc_xfer",
    "subject_status_adrc_xfer_name",
    "yr_in_study",
    "adrc_long_id",
    "naccid",
    "dob",
    "visit_date",
    "c1_visit_date",
    "cdrglob",
    "naccid",
    "subject_status_adrc_xfer_name",
    "subject_status_adrc_xfer",
    "demographic_language_1",
    "demographic_language_1_degree",
    "demographic_language_2",
    "demographic_language_2_degree",
    "demographic_language_3",
    "demographic_language_3_degree",
    "ms_famhxad"
]
    actual_cols = source_df.columns.tolist()

    missing = [col for col in expected_cols if col not in actual_cols]
    extra = [col for col in actual_cols if col not in expected_cols]

    print("Missing columns:", missing)
    print("Unexpected columns:", extra)




# --- Securely copies transformed CSV to server ---
def copyTransfomedDataToServer():
     cmd = ["scp", "-i", "~/.ssh/id_adrc_rsa", "-P", "9221",
            "./tables/output/outv1.csv",
            "adrc-admin@adrc-trac.ucsd.edu:/home/adrc-admin/adrc/deliverables/sjhScriptsQueries/tables"]
     print("Enter Password To Copy Data To Server")
     subprocess.run(cmd, check=True)

# --- MAIN SCRIPT EXECUTION ---
transformed_table_name = "outv1.csv"
template = pd.read_csv("./tables/template/template.csv")
sdsc_df = readSourceData(template)
transformSourceData(template, sdsc_df)
copyTransfomedDataToServer()

# before you run this, your 'test' table has to have all the columns listed on outv1.csv
executeQueryLocaly("./queryScripts", "./operationResearchMigration.sh")
