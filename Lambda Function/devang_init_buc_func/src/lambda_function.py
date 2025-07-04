import json
import boto3
import time
import os
from datetime import datetime

print('asd')

DEST_BUCKET =  os.environ['BUC_NAM']
DEST_PREFIX =  os.environ['LOCATIONB_PREFIX']
GLUE_WORKFLOW_NAME = os.environ['GLUE_WORKFLOW_NAME']
GLUE_JOB = os.environ['GLUE_JOB_ONE']
# DEST_BUCKET = 'bucp2final'
# DEST_PREFIX = 'source/'
# GLUE_WORKFLOW_NAME = 'TEAM2_WORKFLOW'

def lambda_handler(event, context):
    print("Lambda triggered.")  # Entry log

    glue = boto3.client('glue')
    s3 = boto3.client('s3')
    print("Initialized Glue and S3 clients.")

    for record in event['Records']:
        print("Processing new event record...")

        event_name = record['eventName']
        print(f"Event name: {event_name}")

        s3_object_key = record['s3']['object']['key']
        print(f"S3 object key: {s3_object_key}")

        bucket_name = record['s3']['bucket']['name']
        print(f"S3 bucket name: {bucket_name}")

        # Only process .csv files in datasource/ folder
        if event_name.startswith("ObjectCreated"):
            print("Event is an object creation event.")

            if s3_object_key.startswith("datasource/"):
                print("File is in the 'datasource/' folder.")

                if s3_object_key.endswith(".csv"):
                    print("File ends with .csv — valid file type.")

                    try:
                        print("Attempting to start Glue job...")
                        response = glue.start_job_run(
                            JobName=GLUE_JOB
                        )
                        job_run_id = response['JobRunId']
                        print(f"Glue job started. JobRunId: {job_run_id}")

                        # ✅ Poll the job status until it's SUCCEEDED or FAILED
                        while True:
                            status_response = glue.get_job_run(
                                
                                JobName=GLUE_JOB,
                                RunId=job_run_id
                            )
                            job_state = status_response['JobRun']['JobRunState']
                            print(f"[INFO] Glue job status: {job_state}")

                            if job_state == 'SUCCEEDED':
                                print("[SUCCESS] Glue job succeeded.")
                                check_and_run_workflow(s3, glue)
                                break
                            elif job_state in ['FAILED', 'STOPPED', 'TIMEOUT']:
                                print(f"[ERROR] Glue job ended with status: {job_state}")
                                break
                            else:
                                print("[INFO] Job still running... waiting 15 seconds")
                                time.sleep(15)

                    except Exception as e:
                        print(f"[ERROR] Failed during Glue job execution. Details: {e}")
                else:
                    print("File does not end with .csv — skipping.")
            else:
                print("File is not in 'datasource/' folder — skipping.")
        else:
            print("Event is not an object creation — skipping.")

    print("Lambda execution completed.")
    return {
        'statusCode': 200,
        'body': 'Lambda function executed.'
    }

# ✅ Glue workflow trigger
def check_and_run_workflow(s3, glue):
    print("[START] Checking file count for workflow trigger.")

    today_str = datetime.today().strftime('%Y-%m-%d')
    prefix_to_check = f"{DEST_PREFIX}{today_str}/"

    try:
        print(f"[INFO] Checking files under: s3://{DEST_BUCKET}/{prefix_to_check}")
        response = s3.list_objects_v2(Bucket=DEST_BUCKET, Prefix=prefix_to_check)
        files = [obj for obj in response.get('Contents', []) if not obj['Key'].endswith('/')]
        file_count = len(files)

        print(f"[INFO] Found {file_count} files in {prefix_to_check}")

        if file_count == 18:
            print(f"[INFO] File count == 18. Starting Glue Workflow: {GLUE_WORKFLOW_NAME}")
            glue.start_workflow_run(Name=GLUE_WORKFLOW_NAME)
            print("[SUCCESS] Glue workflow triggered.")
        else:
            print("[SKIP] File count is not 18. Doing nothing.")

    except Exception as e:
        print(f"[ERROR] Something went wrong: {e}")

    print("[END] Workflow check completed.")