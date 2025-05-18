# TJR101_group2 ETL Pipeline

This repository contains the Airflow project for scheduling and executing ETL workflows used by **TJR101 Group 2**. The core ETL logic is implemented in a separate project folder and mounted into the Airflow container via Docker Compose.

---

## 📁 Project Structure

```
TJR101_group2/ 
├── src/                       # All data processing code (ETL modules, utils, etc.)
├── data/                      # Raw data, intermediate files, and error logs
├── poetry.lock                # Locked dependencies for the ETL codebase (Poetry)
└── 
```