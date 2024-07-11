# Airbnb ETL Project

This project implements an ETL (Extract, Transform, Load) workflow for Airbnb data using Metaflow, pandas, and PostgreSQL.

/data - Folder containing the raw data

## Workflow Overview

Steps to set up and run the Airbnb ETL project:

1. Set up the environment:
  - Install Python 3.7 or higher if not already installed
  - Install PostgreSQL and create a database named 'airbnb'
2. Clone the project repository:
3. Create and activate a virtual environment
4. Install required packages
5. Configure the project:
  - Open airbnb_etl_flow.py in a text editor
  - Update the db_url in the start step with your PostgreSQL connection details
  - Set the correct path for csv_path in the start step
6. Ensure your Airbnb CSV data file is in the specified location
7. Run the Metaflow workflow
  - python airbnb_etl_flow.py run
8. Monitor the execution in the terminal.
  - The workflow will proceed through each step, displaying progress information.
  - Upon successful completion, you should see the message "ETL workflow completed successfully."
9. Verify the results:
  - Connect to your PostgreSQL database
  - Check that the 'listings' table has been created and contains the transformed data

For more detailed information, refer to the comments in the code.

## Troubleshooting

If you encounter issues, check your database credentials, CSV file path, and ensure all required packages are installed.














