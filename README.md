# beyondfootsteps-etl
PySpark-based ETL pipeline for Footsteps Atlas. Ingests and transforms global migration, refugee, and displacement datasets into a structured bronze–silver–gold data lake for analytical and API use.

## (IN DEVELOPMENT) KPIS – Key Performance Indicators for Refugee Resettlement (UNHCR)

This module evaluates the effectiveness and efficiency of refugee resettlement processes by combining needs, requests, submissions, and actual departures.

---

### 1. `persons`
- **Description**: Total number of individuals included in resettlement requests.
- **Source**: `resettlementsubmissionrequests` dataset.
- **Insight**: Reflects the active demand from countries of asylum toward resettlement countries.

---

### 2. `total_needs`
- **Description**: Estimated number of individuals in need of resettlement according to UNHCR.
- **Source**: `resettlementneeds` dataset.
- **Insight**: Indicates the actual scale of humanitarian needs.

---

### 3. `request_vs_needs_ratio`
- **Formula**: `persons / total_needs`
- **Description**: Percentage of needs covered by official resettlement requests.
- **Insight**:
  - > 1.0 → requests exceed estimated needs (anomalous or aggressive policy).
  - ≈ 1.0 → full coverage.
  - < 0.5 → under-requesting relative to identified needs.

---

### 4. `submissions_total`
- **Description**: Number of individuals officially submitted to resettlement countries.
- **Source**: `resettlementsubmissions` dataset.
- **Insight**: Reflects the capacity to formalize requests into actionable submissions.

---

### 5. `submissions_efficiency`
- **Formula**: `persons / submissions_total`
- **Description**: Ratio of requested individuals who were actually submitted.
- **Insight**:
  - ≈ 1.0 → efficient submission pipeline.
  - < 0.5 → bottlenecks in translating requests to submissions.

---

### 6. `departures_total`
- **Description**: Number of individuals actually resettled.
- **Source**: `resettlementdepartures` dataset.
- **Insight**: Represents the tangible outcome of the resettlement process.

---

### 7. `coverage_rate`
- **Formula**: `departures_total / total_needs`
- **Description**: Proportion of resettlement needs that were actually fulfilled.
- **Insight**:
  - > 1.0 → over-delivery (or possible data discrepancy).
  - ≈ 1.0 → full impact.
  - < 0.5 → significant unmet need.

---

### 8. `resettlement_gap`
- **Formula**: `total_needs - departures_total`
- **Description**: Number of people who needed resettlement but didn’t receive it.
- **Insight**: Quantifies the humanitarian gap.

---

### 9. `realization_rate`
- **Formula**: `departures_total / submissions_total`
- **Description**: Percentage of submissions that led to actual departures.
- **Insight**:
  - ≈ 1.0 → high conversion of submissions into action.
  - < 0.5 → high rejection or failure rate in destination countries.

---
