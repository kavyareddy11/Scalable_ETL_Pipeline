# Airbnb ETL Project

This project implements an ETL (Extract, Transform, Load) workflow for Airbnb data using Metaflow, pandas, and PostgreSQL.

/data - Folder containing the raw data

## Workflow Overview

1. Start: Initialize variables and database connection
2. Create table in PostgreSQL
3. Load data from CSV
4. Transform data (clean, normalize, calculate metrics)
5. Load transformed data to database
6. End: Finalize workflow

For more detailed information, refer to the comments in the code.

## Troubleshooting

If you encounter issues, check your database credentials, CSV file path, and ensure all required packages are installed.
