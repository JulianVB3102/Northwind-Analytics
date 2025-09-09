# Northwind-Analytics
End to end BI solution featuring interactive tableau dashboards. This repository consists of:
- The python script used to load the well known Northwind database into Tableau Publc Desktop.
- The SQL queries used to create the tables for RFM analysis, KPI indicators and inventory risk metrics.
- Documentation about the process of my analysis.

I welcome all constructive criticism in regard to my analysis, code and documentation. If you have insight(s) to share, email me at the email on my profile. 

# Overview

This project demonstrates an end-to-end data pipeline and BI workflow using the Northwind dataset. 
The objective was to practice data engineering, SQL analytics, and business intelligence storytelling. 
The workflow moved raw data from SQLite into Google BigQuery, applied SQL transformations to answer 
business questions, and finally connected the results into Tableau dashboards for visualization.

Source: Northwind dataset (classic sales & orders data).
Original format: SQLite .db file.

## Python ETL Script

Script: load_northwind_to_bq.py
Purpose: Extract tables from SQLite, load them into BigQuery.

Steps performed:

1. SQLite connection — open local .db file.

2. Table iteration — loop through all tables in the database.

3. Dataframe load — pull data into Pandas.

4. Light cleaning — ensure date fields were parsed correctly, handle nulls.

5. BigQuery load — create dataset (northwind) in project, push each table using WRITE_TRUNCATE.

6. This script created a cloud-hosted analytical dataset for SQL queries.

## SQL Analysis

All queries were run in BigQuery against the northwind dataset. Each query was designed to highlight business-relevant insights:

Top Customers by Revenue
Purpose: Identify most valuable customers by spend. Helped me generate KPIs and segment the customer base. 

Yearly Revenue Trend
Purpose: Show business growth/decline patterns over time.

RFM Segmentation View
Purpose: Prepare dataset for customer segmentation and visualization.

## Lessons Learned

Cloud ETL Basics: wrote a Python script to extract from SQLite and push to BigQuery; handled schema, nulls, and date parsing.

SQL for Business Insight: built queries that connect data to real business questions (customer value, yearly trends, geographic performance).

Tableau Best Practices: created KPI tiles, pondered bar vs. pie debates, added scatterplots, formatted tooltips, applied percent of total calculations.

Data Storytelling: learned to separate dashboards (executive vs. customer vs. ops) to reduce clutter and highlight business priorities.

Industry Alignment: produced outputs that mirror what BI analysts deliver: clean data pipeline, validated KPIs, and dashboards with high readability.

