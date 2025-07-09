#IMPORTS----

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


from pyspark.context import SparkContext
from pyspark.sql.functions import col, trim, lit, split, regexp_replace, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, LongType, DoubleType, TimestampType


from datetime import datetime
import boto3
import json
import sys
import re

#DEFINE JOB PARAMETERS
# JOB NAME-name of job
# S3_BUKCET-bucket name from which data is fetched
# S3_INPUT_FOLDER-path of input folder from where it will process data i.e. raw/
# S3_OUTPUT_FOLDER-path of output folder in which result will get stored i.e. staging/
# SCHEMA_FOLDER-path of schema folder in which schema for every table is defined i.e. schemas/
# GLUE_DATABASE-name of database in which schema will get stored
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_BUCKET',
    'S3_INPUT_FOLDER',     # raw/
    'S3_OUTPUT_FOLDER',    # staging/
    'SCHEMA_FOLDER',
    'GLUE_DATABASE'
])

bucket = args['S3_BUCKET']
raw_prefix = args['S3_INPUT_FOLDER'].rstrip('/') + '/'
staging_prefix = args['S3_OUTPUT_FOLDER'].rstrip('/') + '/'
schema_prefix = args['SCHEMA_FOLDER'].rstrip('/') + '/'
database_name = args['GLUE_DATABASE']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#TIMESTAMP FOR METADATA
created_timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
s3 = boto3.client('s3')

#FUNCTION TO PROCESS FOLDERS
def list_date_folders(bucket, prefix):
    paginator = s3.get_paginator('list_objects_v2')
    prefixes = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
        for common_prefix in page.get('CommonPrefixes', []):
            folder = common_prefix['Prefix'].rstrip('/').split('/')[-1]
            prefixes.add(folder)
    return sorted(prefixes)

#FUNCTION TO GET FILES INSIDE INPUT FOLDER
def list_csv_files(bucket, prefix):
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.csv'):
                files.append(obj['Key'])
    return files

#FUNCTION TO LOAD SCHEMA AND CAST DATA OF EACH FILE FROM SCHEMA FOLDER
def load_schema_from_s3(bucket, table_name):
    schema_key = f"{schema_prefix}{table_name}.json"
    try:
        obj = s3.get_object(Bucket=bucket, Key=schema_key)
        schema_json = json.loads(obj['Body'].read().decode('utf-8'))
        fields = []
        for field in schema_json:
            name = field['name']
            dtype = field['type']
            if dtype == "string":
                dtype_obj = StringType()
            elif dtype == "int":
                dtype_obj = IntegerType()
            elif dtype == "date":
                dtype_obj = DateType()
            elif dtype == "long":
                dtype_obj = LongType()
            elif dtype == "double":
                dtype_obj = DoubleType()
            elif dtype == "timestamp":
                dtype_obj = TimestampType()
            else:
                dtype_obj = StringType()
            fields.append(StructField(name, dtype_obj, True))
        return StructType(fields)
    except Exception as e:
        raise Exception(f"Failed to load schema for table '{table_name}': {str(e)}")
        
#CLEANING FUNCTION
def clean_dataframe(df, schema):
    #STANDARDIZE COLUMN NAMES
    new_columns = [re.sub(r'\W+', '_', col_name.strip().lower()) for col_name in df.columns]
    df = df.toDF(*new_columns)

    multi_or_pattern = r"\s+[oOóÓ][rR]\s+"

    for col_name, dtype in df.dtypes:
        #TRIM EXTRA SPACES
        if dtype == "string":
            df = df.withColumn(col_name, trim(col(col_name)))
            
            #REMOVE '-' IN MOBILE OR PHONE COLUMN
            if "phone" in col_name or "mobile" in col_name:
                df = df.withColumn(col_name, regexp_replace(col(col_name), "-", ""))
            
            #HANDLE MULTI VALUE COLUMNS    
            if df.filter(col(col_name).rlike(multi_or_pattern)).limit(1).count() > 0:
                split_col = split(col(col_name), multi_or_pattern)
                df = df.withColumn(f"{col_name}_part1", trim(split_col.getItem(0)))
                df = df.withColumn(f"{col_name}_part2", trim(split_col.getItem(1)))
                if col_name not in [field.name for field in schema.fields]:
                    df = df.drop(col_name)
                else:
                    df = df.withColumn(col_name, trim(split_col.getItem(0)))
     
    #REPLACE NULL WITH NONE              
    df = df.fillna("None")
    
    #CAST TO SCHEMA
    for field in schema.fields:
        if field.name in df.columns:
           if isinstance(field.dataType, TimestampType):
            # Format timestamp column to string without milliseconds
              df = df.withColumn(field.name, date_format(col(field.name), "yyyy-MM-dd'T'H:mm:ss"))
           else:
              df = df.withColumn(field.name, col(field.name).cast(field.dataType))

    #REMOVE DUPLICATE ROWS       
    return df.dropDuplicates()

# Main
date_folders = list_date_folders(bucket, raw_prefix)
if not date_folders:
    print(f"No date folders found under s3://{bucket}/{raw_prefix}")
    sys.exit(1)

# PICK LATEST DATE
latest_date = sorted(date_folders)[-1]
print(f"Processing latest date folder: {latest_date}")

latest_date_prefix = raw_prefix + latest_date + '/'

raw_files = list_csv_files(bucket, latest_date_prefix)
print(f"Found {len(raw_files)} CSV files under s3://{bucket}/{latest_date_prefix}")

for file_key in raw_files:
    try:
        print(f"Processing: {file_key}")
        s3_path = f"s3://{bucket}/{file_key}"
        relative_key = file_key[len(latest_date_prefix):]
        table_name = relative_key.split('/')[0]

        schema = load_schema_from_s3(bucket, table_name)

        df = spark.read.option("header", "true").csv(s3_path)
        df_clean = clean_dataframe(df, schema)

        # ADD METADATA COLUMNS
        df_clean = df_clean.withColumn("source_path", lit(s3_path)).withColumn("updated_time", lit(created_timestamp))

        # SAVE UNDER STAGING 
        output_path = f"s3://{bucket}/{staging_prefix}{created_timestamp[:10]}/{table_name}/"
        (
            df_clean.write
            .format("parquet")
            .mode("overwrite")
            .option("path", output_path)
            .saveAsTable(f"{database_name}.{table_name}")
        )
        print(f"{table_name} cleaned and saved to {output_path}")

    except Exception as e:
        print(f"Failed to process {file_key}: {e}")

# COMMIT JOB
job.commit()
