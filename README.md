# Tweets Data Pipeline

## Architecture
... 

## Overview

This project aims to create an ETL pipeline that retrieves data from the Twitter API, stores it in an AWS S3 Data Lake, processes and cleans the data using PySpark, and ultimately sends it to a Redshift cluster for storage in a data warehouse.

## Tools & Technologies

| Category                | Technology                 |
|-------------------------|----------------------------|
| Cloud                   | AWS                        |
| Infrastructure as Code  | Terraform, Python code     |
| Containerization        | Docker, Docker Compose     |
| Data Processing         | PySpark                    |
| Orchestration           | Airflow                    |
| Data Lake               | AWS S3                     |
| Data Warehouse          | AWS Redshift               |
| Language                | Python                     |



## How it works 

### Infrastructure as code 

Terraform is used to provision and manage AWS services required for the pipeline. In addition to Terraform, a Python script is provided to create and manage infrastructure as code without relying on Terraform. This alternative approach offers flexibility in choosing the desired infrastructure management tool.

The infrastructure as code setup consists of:

Terraform scripts to provision AWS services such as S3, Redshift, and any other necessary components.
Python scripts that provide an alternative to Terraform for creating and managing infrastructure.
By using infrastructure as code, the deployment and management of the project's resources become more streamlined, allowing for easier maintenance and updates.

### Data flow 
- Data Collected from the API is moved to landing zone s3 buckets.
- Once the data is moved to S3, spark job is triggered which reads the data and apply transformations. Dataset is repartitioned and moved to the Processed Zone. 
- The warehouse module of the ETL pipeline retrieves data from the processed zone and transfers it to the corresponding Redshift tables.
- ETL job execution is completed once the Data Warehouse is updated.

### Airflow Orchestration
Individual Airflow DAGs for each stage of the ETL pipeline, including data collection, data processing, and data warehousing

## Getting Started
To install and run the project : 

`make init`
`make run`
