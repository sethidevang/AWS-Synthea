
# Scalable AWS Data Pipeline for Synthea Dataset

## Overview

This repository presents a scalable and automated data pipeline built using Amazon Web Services (AWS). The system enables the ingestion, processing, transformation, cataloging, and visualization of synthetic healthcare data produced by the Synthea simulation platform.

## What is Synthea?

Synthea is an open-source synthetic patient generator that creates detailed yet fictitious healthcare records, including demographics, diagnoses, medications, procedures, and clinical visits. Unlike real data, Synthea’s output ensures privacy by containing no personally identifiable information (PII), while preserving realistic data distributions — making it ideal for testing, analysis, and academic research.

## Project Goals

The primary objective of this project is to convert raw synthetic data into clean, well-structured, and query-optimized datasets suitable for analytics and reporting. The architecture leverages AWS managed services to deliver:

- Minimal maintenance
- High performance
- Cost efficiency
- Scalability and reliability

## Key Components of the Pipeline

1. **Data Lake with Amazon S3**  
   Raw Synthea datasets are ingested into Amazon S3, which acts as a central, secure, and cost-efficient data lake.

2. **Pre-Processing via AWS Lambda**  
   Event-driven Lambda functions trigger post-upload to perform quick format normalization and filtering.

3. **ETL Using AWS Glue**  
   Comprehensive data cleaning, deduplication, schema alignment, and conversion to Parquet format are handled using AWS Glue.

4. **Query Engine with Amazon Athena**  
   Athena allows serverless SQL querying on the Parquet files, enabling the creation of flexible, business-focused data marts.

5. **Reporting with PowerBI**  
   PowerBI connects to Athena for interactive dashboards and real-time business intelligence insights.

## Architecture Components

| Component             | Service Used      | Purpose                                                  |
|-----------------------|-------------------|----------------------------------------------------------|
| **Data Upload**        | Amazon S3         | Upload and store raw Synthea data in a central data lake  |
| **Minor Cleaning**     | AWS Glue Job      | Initial cleanup (remove unnamed columns, handle extra commas) |
| **Major Cleaning**     | AWS Glue Job      | Advanced transformation, deduplication, type formatting, Parquet conversion |
| **Data Mart Creation** | AWS Glue Job      | Organize cleaned data into specific structures           |
| **Automation**         | AWS Glue Triggers | Schedule and automate ETL job execution                  |
| **Schema Management**  | Glue Data Catalog | Maintain consistent schema and structure across datasets |
| **Visualization**      | PowerBI    | Build interactive dashboards and derive business insights |

## Data Import Workflow

### Generating Synthea Data Locally

1. Clone the official Synthea repository into your local system.
2. To generate synthetic patient records, compile and run the simulation with desired parameters such as patient count and a random seed (to ensure reproducibility).

**Example Command:**

```bash
# Command to generate 1,000 synthetic patients with a specific seed
This will produce synthetic health records (in CSV/JSON format) that can be used as input for the AWS data pipeline.

Upload to Amazon S3
Upload the generated CSV files to the designated raw data folder.

Recommended S3 Folder Structure:
s3://bucp2final/source/
s3://bucp2final/raw/
s3://bucp2final/staging/

Pipeline Steps in Detail

Step 1: Upload Raw Data to Amazon S3
Service: Amazon S3
Description: Upload the generated Synthea data files (CSV) to the initial source/ folder in your S3 bucket using AWS CLI or SDKs.

Bucket Structure:
s3://bucp2final/source/
s3://bucp2final/raw/
s3://bucp2final/staging/

Step 2: Minor Cleaning with AWS Glue
Service: AWS Glue Job 1
Purpose: Remove unnamed columns, fix extra delimiters, and prepare raw data for deeper transformation.
Input: s3://bucp2final/source/
Output: s3://bucp2final/raw/

Step 3: Major Cleaning & Format Conversion
Service: AWS Glue Job 2
Purpose: Perform deduplication, type standardization, and convert data to Parquet format.
Input: s3://bucp2final/raw/
Output: s3://bucp2final/staging/

Step 4: Data Mart Creation
Service: AWS Glue Job 3
Purpose: Aggregate and model data into business-ready structures.

Data Marts Design
As part of the final processing stage, a Data Mart was created using data from the s3://bucp2final/staging/ folder.

Database in Athena:

Database Name: bucp2_glue_db6

Within this Data Mart, we designed:

1 Fact Table: patient_fact

7 Dimension Tables: encounter_dim, condition_dim, procedure_dim, medication_dim, provider_dim, organization_dim, location_dim

The schema design followed best practices of a Star Schema.

Reporting and Analysis with PowerBI
Amazon PowerBI was used to create interactive dashboards and visual reports. It connects directly with Amazon Athena to query the bucp2_glue_db6 database, allowing us to:

Visualize patient demographics, encounter trends, condition distribution, and more

Perform data-driven analysis without provisioning servers

Enable quick insights for stakeholders through shareable reports

Conclusion
We successfully built a serverless, end-to-end AWS data pipeline for synthetic healthcare data using the Synthea platform. The pipeline automates data ingestion, cleaning, transformation, and analytics using:

Amazon S3

AWS Glue

Athena

PowerBI

We designed a Data Mart with one fact and six dimension tables for structured analysis. The architecture ensures:

High scalability

Minimal maintenance

Fast and reliable insight generation
