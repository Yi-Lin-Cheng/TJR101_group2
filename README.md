# TJR101_group2 ETL Pipeline

This project, developed by Group 2 of TJR101, focuses on integrating open government data with enriched business information using Google Maps API and web scraping techniques. The goal is to automate data extraction, enrichment, and storage processes using Apache Airflow and GitHub Actions. The final output powers a LINE BOT that recommends customized travel itineraries based on events and exhibitions.

---

## 📌 Project Objectives

Extract and integrate data from open government sources and Google Maps
Automatically match and enrich business records (e.g., opening hours, reviews)
Manage ETL workflows using Apache Airflow
Automate deployment to a GCP VM via GitHub Actions
Power a LINE BOT that recommends nearby food, attractions, and accommodations based on ongoing exhibitions and events

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

## 🛠 Tech Stack

Python 3.12
MySQL
Docker
GitHub Actions
GCP VM
Apache Airflow
pandas, Selenium, BeautifulSoup, rapidfuzz
Google Maps API

---

## ⚙️ CI/CD Deployment Flow

Code pushed to the main branch triggers GitHub Actions
GitHub Actions connects to the GCP VM via SSH
The VM executes git pull to synchronize the latest project code
