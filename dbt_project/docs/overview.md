{% docs __overview__ %}

# Data Engineering Pipeline Documentation

## Data Flow
1. Bronze Layer (Raw Data)
   - Contains raw data ingested from CSV
   - Schema: bronze
   - Table: landing_egm_data

2. Silver Layer (Cleaned Data)
   - Contains cleaned and validated data
   - Schema: silver
   - Includes historical snapshots

3. Gold Layer (Business Layer)
   - Contains business-ready views
   - Schema: gold
   - Optimized for analytics

## Pipeline Steps
1. CSV ingestion to bronze schema
2. Transformation to silver layer with data quality checks
3. Creation of gold layer views for business use

{% enddocs %} 