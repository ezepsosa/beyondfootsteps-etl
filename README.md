# 🌐 beyondfootsteps-etl

![Alpha](https://img.shields.io/badge/version-alpha-blue?logo=github)
![Maintainer](https://img.shields.io/badge/maintainer-ezepsosa-lightgrey?logo=github)
![Repo](https://img.shields.io/badge/repo-beyondfootsteps--etl-181717?logo=github)
![License](https://img.shields.io/github/license/ezepsosa/beyondfootsteps-etl?logo=github)
![Issues](https://img.shields.io/github/issues/ezepsosa/beyondfootsteps-etl?logo=github)
![Last Commit](https://img.shields.io/github/last-commit/ezepsosa/beyondfootsteps-etl?logo=github)

## 🚀 Introduction

beyondfootsteps-etl is a PySpark-based ETL pipeline designed for Footsteps Atlas. It ingests, transforms, and organizes global migration, refugee, and displacement datasets into a structured bronze–silver–gold data lake, enabling advanced analytics and API integrations.

## 🛠️ Technologies

- **PySpark** for distributed data processing
- **Poetry** for dependency management and packaging
- **Parquet** for efficient columnar data storage

## 📦 Datasets

This pipeline leverages authoritative global migration data from:
- **UNHCR** (United Nations High Commissioner for Refugees)
- **WorldData** (international migration and displacement statistics)

## ⚙️ Data Processing Workflow

1. **Fetch Data**  
  Retrieve raw datasets directly from UNHCR and WorldData sources.
2. **Intake Data**  
  Load and validate raw data into the bronze layer.
3. **Bronze Layer**  
  Store unprocessed, raw data for traceability.
4. **Silver Layer**  
  Cleanse, standardize, and enrich data for analytical use.
5. **Gold Layer**  
  Aggregate and export final KPI tables for reporting and API consumption.

## 📊 Exported KPIs

This module computes and exports standardized KPIs, each available as a table in the **public** schema:

### 1. `asylum_requests_kpi`
- **Description:** Number of asylum requests registered by country of asylum and year.
- **Source:** UNHCR dataset: `asylum_requests_raw`.
- **Insight:** Measures the demand for international protection.

### 2. `asylum_decisions_kpi`
- **Description:** Decisions on asylum requests (approved, denied, pending).
- **Source:** UNHCR dataset: `asylum_decisions_raw`.
- **Insight:** Reflects the effectiveness and speed of the asylum process.

### 3. `resettlements_summary_kpi`
- **Description:** Aggregated summary of the resettlement chain: needs, requests, submissions, and departures.
- **Source:** UNHCR datasets: `resettlement_needs_raw`, `resettlement_requests_raw`, `resettlement_submissions_raw`, `resettlement_departures_raw`.
- **Insight:** Provides a global view of the gap between humanitarian need and effective outcome.

### 4. `dashboard_summary_kpi`
- **Description:** Consolidated dashboard of key KPIs (asylum, internal displacement, returns, naturalization, resettlement).
- **Source:** Union and aggregation of all individual KPIs.
- **Insight:** Enables a quick overview of all migration flows.

### 5. `idp_displacement_kpi`
- **Description:** Count of internally displaced persons by country of origin and year.
- **Source:** WorldData dataset: `idp_displacement_raw`.
- **Insight:** Monitors internal displacement crises.

### 6. `idp_returnees_kpi`
- **Description:** Number of voluntary returns of internally displaced persons.
- **Source:** WorldData dataset: `idp_returnees_raw`.
- **Insight:** Assesses the capacity for return and post-crisis recovery.

### 7. `refugee_naturalization_kpi`
- **Description:** Number of refugees who obtain nationality in host countries.
- **Source:** UNHCR dataset: `refugee_naturalization_raw`.
- **Insight:** Indicator of long-term integration and access to full rights.

---

## 📖 Usage
_Usage instructions will be added soon._
