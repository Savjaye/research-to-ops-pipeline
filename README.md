# Research-to-Ops Data Pipeline

This repository contains a Python-based ETL pipeline that facilitates the transformation and migration of research data from SDSC (San Diego Supercomputer Center) PostgreSQL database into the TRAC operations PostgreSQL database. It automates the complex, multi-table merging, transformation, and loading processes needed to keep research and operational datasets in sync.

> **Full Instructions Available**  
For detailed setup instructions and documentation, [see this Google Doc](https://docs.google.com/document/d/1RbJdK05GV0i78IYPc521CvKf5H9oPlrgtkRVXOUEhYc/edit?tab=t.0).

---

## Overview

The pipeline performs the following:
- Reads in and merges relevant CSV exports from the research database.
- Transforms and standardizes the data to match operational schema requirements.
- Loads the transformed dataset into a staging table on the TRAC operations server.
- Executes SQL scripts to insert data into production tables, tracking changes and supporting rollback.

---

##  Quick Start

###  Step 1: Ensure SSH Access
Make sure you can SSH into the operations server that houses the TRAC PostgreSQL database.

 See the [Appendix in the guide](https://docs.google.com/document/d/1RbJdK05GV0i78IYPc521CvKf5H9oPlrgtkRVXOUEhYc/edit?tab=t.0) for help setting up access.

---

###  Step 2: Clone the Repository

```bash
git clone https://github.com/your-username/research-to-ops-pipeline.git
cd research-to-ops-pipeline
```
###  Step 3: Download Required Tables

Download the following CSV files from the SDSC research server:

- `path_path.csv` (under the PATH project)
- `uds_a1subdemo.csv` (under the UDS project)
- `uds_a2infdemo.csv` (under the UDS project)
- `uds_a4ard.csv` (under the UDS project)
- `uds_a5subhst.csv` (under the UDS project)
- `uds_b4cdr.csv` (under the UDS project)
- `uds_c1npsyb.csv` (under the UDS project)
- `uds_d1clindx.csv` (under the UDS project)
- `uds_lhqrev.csv` (under the UDS project)
- `uds_naccenroll.csv` (under the UDS project)
- `uds_registry.csv` (under the UDS project)
- `uds_roster.csv` (under the UDS project)

 Place all downloaded files into the following directory:
 ./tables/sourceTables/

 ###  Step 4: Run the Pipeline
 ```bash
 python3.12 research_to_ops_pipeline.py
 ```

 ## Directory Tour

### `research_to_ops_pipeline.py`
**Purpose:**  
This is the *main script* that orchestrates the entire pipeline from start to finish.

**Summary of Workflow:**
- **Reads** CSV extracts from the research database (located in `./tables/sourceTables/`).
- **Preprocesses** tables where needed (e.g., special handling for family history in the A3 table).
- **Merges** tables together into a unified dataframe.
- **Selects** only the most recent visit year for each participant.
- **Transforms** the dataset:
  - Renames columns based on a standardized template (`./tables/template/`).
  - Applies any necessary calculated transformations (defined in the template).
- **Formats** the final dataframe to match the exact structure expected by the TRAC operations database.
- **Exports** the final dataset to a CSV (`outv1.csv`) and **copies it to the TRAC server** via SSH/SCP.
- **Executes** a series of SQL scripts (stored in `./queryScripts/`) on the TRAC server to insert/update the database with the transformed data.

**Inputs:**
- Tables in `./tables/sourceTables/`
- Transformation Template in `./tables/template/template.csv`

**Outputs:**
- Final transformed CSV: `./tables/output/outv1.csv`
- Updates to the TRAC PostgreSQL database via SQL scripts.

**Notes:**
- **Adding New Fields:**  
  If the research database schema or the TRAC target table changes (e.g., new fields added), this script must be updated.  
  See the "Updating the Pipeline" section of the [SOP](https://docs.google.com/document/d/1RbJdK05GV0i78IYPc521CvKf5H9oPlrgtkRVXOUEhYc/edit?tab=t.0) for full instructions.
- **Additional Documentation** can be found in the script's comments

### `tables/`
**Purpose:**  
This folder holds all data inputs, outputs, and configuration templates needed to run the pipeline.

| Subfolder        | Purpose |
|:-----------------|:--------|
| `sourceTables/`  | Raw input CSV files downloaded from the SDSC Research Database (e.g., `uds_a1subdemo.csv`, `uds_registry.csv`, etc.). |
| `output/`        | Output files generated during pipeline execution. Includes both temporary files for bug testing (e.g., `a3TEST.csv`) and the final transformed dataset (`outv1.csv`) ready to be uploaded to the operations server. |
| `template/`      | Stores `template.csv`, which defines the field mappings between the research and operations database, as well as combo mappings and calculated transformations. |

### `queryScripts/`

**Purpose:**  
This directory contains the SQL scripts that are executed (in order) on the TRAC production server to finalize the migration of the research data.

These scripts work together to ensure **safe and orderly migration** of the transformed research data into production tables.

**Important:**  
The files are **numbered** (`0_`, `1_`, `2_`) because **they must be executed in this order** for the pipeline to work correctly.

| File | Purpose |
|:---|:---|
| `0_insertSDSC.sql` | Loads (`INSERTs`) the transformed dataset (`outv1.csv`) into a *staging table* on the TRAC server (`public.test`). |
| `1_migrationProcedure.sql` | Defines a SQL **stored procedure** that moves data from the staging table into the appropriate fields in the operational tables, ensuring that fields are mapped correctly and any custom business logic is handled. |
| `2_callMigrationProcedure.sql` | Simply **calls** the stored procedure defined in `1_migrationProcedure.sql`, executing the migration of data into production tables. |

**Notes:**
- These scripts are executed automatically by the `research_to_ops_pipeline.py` script using `operationResearchMigration.sh`.
- Keeping the numbering system (`0_`, `1_`, `2_`) is crucial to prevent logical errors during the migration process.
- If changes are made to the TRAC database schema, these scripts may need to be updated accordingly.