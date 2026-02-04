# Cubix Data Engineer Capstone Project

## Project Overview

This project implements an end-to-end data engineering ETL pipeline, simulating incremental batch ingestion of raw CSV data files from source folders, data transformations using locally defined functions and creation of analytics-ready datasets using a structured Lakehouse approach with Medallion Architecture. The project is packaged as a reusable Python library, installed in Databricks as a wheel file and applied using a Databricks notebook.


## Technologies Used

The project is built using Python and PySpark and runs on Databricks Free Edition.\
Local development and packaging are handled with Poetry, while testing is implemented using Pytest, and code quality is enforced using pre-commit hooks. Unity Catalog Volumes are used for file-based ingestion and transformed datasets are stored as Delta tables managed through Unity Catalog.


## Concepts Applied

The pipeline follows the Medallion Architecture with Bronze, Silver, and Gold layers.\
Slowly Changing Dimensions Type 1 (SCD1) are applied to dimension tables using merge logic, while the sales fact table is loaded incrementally in append mode.\
Data quality is validated using Great Expectations on Gold-level datasets.\
Transformed datasets are combined into One Big Table called wide_sales_df in the Gold layer.\
Engineering best practices such as automated formatting and linting are enforced through pre-commit hooks.


## How to Run the Project on Databricks

The project is executed via a Databricks notebook (`databricks_ingestion_pipeline.ipynb`) on Databricks Free Edition.\
The core ETL logic is installed as a wheel (`.whl`) package, which is imported and executed within the notebook.\
Running the notebook top-to-bottom performs Bronze ingestion, Silver transformations, Gold aggregations, and data quality checks.


## How to Install Dependencies

Dependencies and package versions are managed locally using Poetry and defined in `pyproject.toml`.\
Install all dependencies in the virtual environment created by Poetry by running `poetry add`, and build the Databricks package using `poetry build`, which generates a versioned wheel file for deployment.


## How to Run Tests

Unit tests are implemented using Pytest and focus on transformation logic and data correctness.\
Tests can be executed locally with `poetry run pytest` before packaging and deploying the project to Databricks.
