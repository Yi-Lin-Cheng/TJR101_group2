# TJR101_group2 ETL Pipeline

This repository contains the ETL pipeline for TJR101 Group 2, responsible for extracting, transforming, and loading data from various sources such as accommodations, food, events, and more.

---

## 📁 Project Structure

```
TJR101_group2/
├── .devcontainer/             # VSCode DevContainer settings
│   ├── devcontainer.json
│   └── postCreateCommand.sh
├── .github/
│   └── workflows/             # GitHub Actions CI/CD pipeline (e.g., auto-deploy)
│       └── deploy.yml
├── src/                       # ETL source code by domain
│   ├── accomo/                # Accommodation data ETL
│   ├── accupass/              # Accupass events ETL
│   ├── food/                  # Food-related data ETL
│   ├── klook/                 # Klook activities ETL
│   ├── spot/                  # Scenic spots ETL
│   ├── tasks/                 # Shared Airflow task logic
│   └── utils/                 # Utility modules (e.g., logging, geocoding)
├── data/                      # Output data and logs
├── pyproject.toml             # Poetry dependency configuration
├── poetry.lock                # Locked dependencies for reproducibility
├── Dockerfile                 # Development Dockerfile (optional)
├── .env                       # Environment variables for local/dev use
├── .gitattributes             # Git config (e.g., line endings)
└── poetry.toml                # (Duplicate, consider removing if unnecessary)
```