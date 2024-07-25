# nr-dap-dlh
This repository holds code and artifacts for the Data Analytics Platform (DAP).

The Data Lakehouse (DLH) allows for the summarization, transformation and combination of structured and semi-structured data, including the consumption of replicated data assets from the Operation Data Store (ODS), APIs and Object Storage.

## File Structure

- **ETL Tools (such as Data Build Tool (DBT) projects):**
  `nr-dap-dlh/{domain}/{name of tool}/{Files}`

- **Python or R Scripts**
  `nr-dap-dlh/{domain}/scripts/{Files}`

- **DDL (Data Definition Language)** Includes commands like CREATE, ALTER, and COMMENT that define and modify database structure.
  `nr-dap-dlh/{domain}/ddl/{Filename = name of the table}`

- **DQL (Data Query Language) / DML (Data Manipulation Language)** Includes commands like SELECT, INSERT, UPDATE, and DELETE that manipulate data within the database.
  `nr-dap-dlh/{domain}/dml/{Files}`

- **DCL (Data Control Language)** Includes commands like GRANT and REVOKE that control access to the data in the database.
  `nr-dap-dlh/{domain}/dcl/{Files}`