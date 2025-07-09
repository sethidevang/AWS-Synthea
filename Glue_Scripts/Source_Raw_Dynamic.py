# IMPORTS
import boto3
from datetime import datetime
import os
import sys
import csv
from io import StringIO

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

# DYNAMIC PARAMETERS
# JOB_NAME - name of job
# S3_BUCKET - bucket name where data is fetched
# S3_INPUT_FOLDER - folder in bucket that provides input data (source)
# S3_OUTPUT_FOLDER - folder in bucket to store results (raw)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET', 'S3_INPUT_FOLDER', 'S3_OUTPUT_FOLDER'])

bucket = args['S3_BUCKET']
source_prefix = args['S3_INPUT_FOLDER'].rstrip('/') + '/'
raw_prefix = args['S3_OUTPUT_FOLDER'].rstrip('/') + '/'

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# DATE FORMAT FOR FOLDER CREATION WITH DATE
date_str = datetime.today().strftime('%Y-%m-%d')
s3 = boto3.client('s3')

# FUNCTION TO RETURN ALL CSV FILES 
def list_all_files(bucket, prefix):
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    files = []
    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv'):
                files.append(key)
    return files

# FUNCTION TO GET LATEST FOLDER 
def get_latest_folder(bucket, base_prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=base_prefix, Delimiter='/')
    folders = [prefix['Prefix'] for prefix in response.get('CommonPrefixes', [])]
    if not folders:
        return None
    latest_folder = max(folders)  
    return latest_folder

# CSV LINE PARSING
def parse_csv_line(line):
    try:
        return next(csv.reader(StringIO(line), skipinitialspace=True))
    except Exception:
        return []

# HANDLE COMMAS
def clean_and_align_rows(raw_rdd, expected_cols):
    return raw_rdd.map(lambda row: parse_csv_line(row[0])) \
                  .map(lambda fields: fields[:expected_cols] + [''] * (expected_cols - len(fields)))

# REMOVE UNNAMED COLUMNS
def remove_unnamed_columns(df):
    return df[[col for col in df.columns if col not in [None, ""]]]

# FIND LATEST FOLDER AND LIST FILES INSIDE IT
latest_folder_prefix = get_latest_folder(bucket, source_prefix)
if not latest_folder_prefix:
    print(f"No subfolders found under s3://{bucket}/{source_prefix}")
    sys.exit(1)

print(f" Latest folder detected: s3://{bucket}/{latest_folder_prefix}")
all_files = list_all_files(bucket, latest_folder_prefix)
print(f"Found {len(all_files)} CSV files in latest folder")

# PROCESS FILES
for file_key in all_files:
    try:
        print(f"\n Processing file: {file_key}")
        s3_path = f"s3://{bucket}/{file_key}"
        
        # LOAD RAW TEXT
        raw_df = spark.read.text(s3_path)
        header_line = raw_df.first()
        if header_line is None:
            print(f" Skipping empty file: {file_key}")
            continue

        header = parse_csv_line(header_line[0])
        expected_col_count = len(header)
        
        # ALIGN ROWS WITH COLUMNS
        raw_rdd = raw_df.rdd
        aligned_rdd = clean_and_align_rows(raw_rdd, expected_col_count)

        if aligned_rdd.isEmpty():
            print(f" No rows after column alignment in file: {file_key}")
            continue
        
        #CREATE DATAFRAME AND CALL FUNCTION TO REMOVE UNNAMED COLUMNS
        df = spark.createDataFrame(aligned_rdd, header)
        df_clean = remove_unnamed_columns(df)

        if df_clean.count() == 0:
            print(f" No valid rows after cleaning in file: {file_key}")
            continue

        relative_path = file_key[len(source_prefix):]
        folder_name = os.path.splitext(os.path.basename(relative_path))[0]
        output_path = f"s3://{bucket}/{raw_prefix}{date_str}/{folder_name}/"
        
        #WRITE RESULT TO OUTPUT FOLDER
        df_clean.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        print(f"Cleaned data written to: {output_path}")

    except Exception as e:
        print(f"Failed to process {file_key}: {e}")

# FINISH JOB
job.commit()
