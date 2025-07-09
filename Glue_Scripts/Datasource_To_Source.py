import boto3
import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions

# DYNAMIC PARAMETERS
# JOB_NAME - name of the job
# S3_BUCKET - bucket name
# INPUT_FOLDER - to get input files - datasource/ 
# OUTPUT_FOLDER - to store result - source/ 
# ARCHIVE_FOLDER - to store data for history - archive/ 
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET', 'INPUT_FOLDER', 'OUTPUT_FOLDER', 'ARCHIVE_FOLDER'])

bucket = args['S3_BUCKET']
input_prefix = args['INPUT_FOLDER'].rstrip('/') + '/'
output_prefix = args['OUTPUT_FOLDER'].rstrip('/') + '/'
archive_prefix = args['ARCHIVE_FOLDER'].rstrip('/') + '/'

#DATE FOR FOLDER CREATION
s3 = boto3.client('s3')
date_str = datetime.today().strftime('%Y-%m-%d')

#FUNCTION THAT WILL PROCESS AND MOVE DATA TO SOURCE AND ARCHIVE FOLDER
def move_and_archive_csvs():
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=input_prefix)

    moved = 0

    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv') and '/' not in key[len(input_prefix):]:  
                filename = key.split('/')[-1]         # e.g. claims.csv
                base_name = filename[:-4]             # remove .csv

                #DESTINATION PATH
                new_key = f"{output_prefix}{date_str}/{base_name}/{filename}"
                archive_key = f"{archive_prefix}{date_str}/{base_name}/{filename}"

                print(f" Moving: {key} → {new_key}")
                print(f"️ Archiving: {key} → {archive_key}")

                #COPY TO SOURCE FOLDER
                s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=new_key)

                #COPY TO ARCHIVE FOLDER
                s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=archive_key)

                #DELETE FILES FROM DATASOURCE 
                s3.delete_object(Bucket=bucket, Key=key)

                moved += 1

    print(f"Completed: {moved} CSV files moved and archived.")

# CALL FUNCTION
move_and_archive_csvs()
