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
 python3.12 migration.py