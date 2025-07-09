
import boto3
from urllib.parse import urlparse
from datetime import datetime
import sys


from pyspark.context import SparkContext
from pyspark.sql.functions import col, sha2, concat_ws, current_timestamp, lit, count


from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job



args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)



bucket = "bucp2final"
prefix = "staging/"
database_name = "bucp2_glue_db6"
s3 = boto3.client("s3")



def get_latest_date_folder(bucket, prefix):
    """ 
    used to get the folder with latest date so that only 
    the latest data is fetched
    """
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    date_folders = [content['Prefix'].split("/")[1] for content in response.get('CommonPrefixes', [])]
    return f"{prefix}{max(date_folders)}/"

latest_folder = get_latest_date_folder(bucket, prefix)

def scd2_merge(new_df, path, key_cols, hash_cols, sk_col, table_name):
    """ 
    compares new data with old data using hash comparison,
    expires old rows if there is change in any row and inserts new row with correct timestamps.
    """
    from pyspark.sql.types import StructType

    # Add hash to new data to detect changes efficiently.
    new_df = new_df.withColumn("hash", sha2(concat_ws("|", *hash_cols), 256))

    #Trying to read the existing dimension table (historical data) from path
    try:
        existing_df = spark.read.parquet(path)
        existing_df = existing_df.withColumn("old_hash", sha2(concat_ws("|", *hash_cols), 256))
        #Cache the data to improve join/filter performance
        existing_df.cache()
    except:
        existing_df = None
    #If existing data is present and has rows
    if existing_df and existing_df.count() > 0:
        new_alias = "new"
        old_alias = "old"
        # Step 4: Join new and old data on the business/natural keys
        joined = new_df.alias(new_alias).join(
            existing_df.alias(old_alias).filter(col(f"{old_alias}.is_active") == True),
            on=[col(f"{new_alias}.{c}") == col(f"{old_alias}.{c}") for c in key_cols],
            how="left"
        )
        #Identify changed rows by comparing hashes (or if no match found)
        changed = joined.filter(
            (col(f"{new_alias}.hash") != col(f"{old_alias}.old_hash")) | col(f"{old_alias}.old_hash").isNull()
        )

        # Select columns explicitly from the "new" side
        changed = changed.select([col(f"{new_alias}.{c}").alias(c) for c in new_df.columns])
        
         #Get records that are unchanged (still active and not in changed set)
        unchanged = existing_df.join(changed, on=key_cols, how="left_anti") \
            .filter(col("is_active") == True)
        #Expire old versions of changed records
        expired = existing_df.join(changed.select(*key_cols), on=key_cols, how="inner") \
            .withColumn("is_active", lit(False)) \
            .withColumn("modified_at", current_timestamp())

    else:
        #If no existing data, everything in new_df is treated as changed
        changed = new_df
        unchanged = spark.createDataFrame([], new_df.schema \
            .add(sk_col, "string") \
            .add("created_at", "timestamp") \
            .add("modified_at", "timestamp") \
            .add("is_active", "boolean") \
            .add("old_hash", "string"))
        expired = unchanged

    #Add SCD2 metadata to the changed rows
    changed = changed.withColumn(sk_col, sha2(concat_ws("|", *key_cols), 256)) \
        .withColumn("created_at", current_timestamp()) \
        .withColumn("modified_at", current_timestamp()) \
        .withColumn("is_active", lit(True)) \
        .withColumn("old_hash", col("hash"))

    #Combine unchanged, expired, and new changed rows into the final dataset
    base_cols = unchanged.columns
    final_df = unchanged.select(base_cols).unionByName(
        expired.select(base_cols)
    ).unionByName(
        changed.select(base_cols)
    )

    final_df.write.mode("overwrite").format("parquet").option("path", path).saveAsTable(f"{database_name}.{table_name}")

# ========== DIM_LOCATION ==========
patient_df = spark.read.parquet(f"s3://{bucket}/{latest_folder}patients/")
dim_location_input = patient_df.select(
    col("address"), col("city"), col("state"), col("zip").alias("zip_code")
).dropna().dropDuplicates()
scd2_merge(dim_location_input, "s3://bucp2final/output/patient/dim_location/",
           key_cols=["address", "city", "state", "zip_code"],
           hash_cols=["address", "city", "state", "zip_code"],
           sk_col="location_sk", table_name="dim_location")

# ========== DIM_PAYER ==========
payer_df = spark.read.parquet(f"s3://{bucket}/{latest_folder}payers/")
dim_payer_input = payer_df.select(
    col("id").alias("payer_id"), col("name"), col("ownership")
).dropna().dropDuplicates()
scd2_merge(dim_payer_input, "s3://bucp2final/output/patient/dim_payer/",
           key_cols=["payer_id"],
           hash_cols=["name", "ownership"],
           sk_col="payer_sk", table_name="dim_payer")



# ========== DIM_ALLERGIES ==========
allergy_df = spark.read.parquet(f"s3://{bucket}/{latest_folder}allergies/")
dim_allergy_input = allergy_df.select(
    "start", "stop", "patient", "description", "type", "category"
).dropna().dropDuplicates()
scd2_merge(dim_allergy_input, "s3://bucp2final/output/patient/dim_allergies/",
           key_cols=["patient", "description", "start"],
           hash_cols=["start", "stop", "description", "type", "category"],
           sk_col="allergy_sk", table_name="dim_allergies")



# ========== DIM_PATIENT ==========
dim_patient_input = patient_df.select(
    col("id").alias("patient_id"),
    concat_ws(" ", col("first"), col("middle"), col("last")).alias("name"),
    "gender", "birthdate", "race", "ethnicity"
).dropna().dropDuplicates()
scd2_merge(dim_patient_input, "s3://bucp2final/output/patient/dim_patient/",
           key_cols=["patient_id"],
           hash_cols=["name", "gender", "birthdate", "race", "ethnicity"],
           sk_col="patient_sk", table_name="dim_patient")



# ========== DIM_MEDICATION ==========
med_df = spark.read.parquet(f"s3://{bucket}/{latest_folder}medications/")
dim_med_input = med_df.select("start", "stop", "patient", "description").dropna().dropDuplicates()
scd2_merge(dim_med_input, "s3://bucp2final/output/patient/dim_medication/",
           key_cols=["patient", "start", "description"],
           hash_cols=["start", "stop", "description"],
           sk_col="med_sk", table_name="dim_medication")



# ========== DIM_OBSERVATION ==========
obs_df = spark.read.parquet(f"s3://{bucket}/{latest_folder}observations/")
dim_obs_input = obs_df.select(
    "date", "patient", "encounter", "category",
    "description_part1", "value_part1",
    "description_part2", "value_part2"
).dropna().dropDuplicates()
scd2_merge(dim_obs_input, "s3://bucp2final/output/patient/dim_observation/",
           key_cols=["patient", "date", "encounter", "description_part1"],
           hash_cols=["category", "value_part1", "description_part2", "value_part2"],
           sk_col="obs_sk", table_name="dim_observation")



# ========== FACT_PATIENT ==========
encounters_df = spark.read.parquet(f"s3://{bucket}/{latest_folder}encounters/")
conditions_df = spark.read.parquet(f"s3://{bucket}/{latest_folder}conditions/")
payer_trans_df = spark.read.parquet(f"s3://{bucket}/{latest_folder}payer_transitions/")
dim_location_df = spark.read.parquet("s3://bucp2final/output/patient/dim_location/")

encounters_count_df = encounters_df.groupBy("patient").agg(count("*").alias("total_encounters"))
conditions_count_df = conditions_df.groupBy("patient").agg(count("*").alias("total_conditions"))
payer_df = payer_trans_df.select("patient", "payer").dropna().dropDuplicates(["patient"])

location_key_df = patient_df.select(
    col("id").alias("patient_id"), "address", "city", "state", col("zip")
)
dim_loc_keyed = dim_location_df.select("location_sk", "address", "city", "state", col("zip_code"))

fact_base_df = location_key_df.join(
    dim_loc_keyed,
    (location_key_df["address"] == dim_loc_keyed["address"]) &
    (location_key_df["city"] == dim_loc_keyed["city"]) &
    (location_key_df["state"] == dim_loc_keyed["state"]) &
    (location_key_df["zip"] == dim_loc_keyed["zip_code"]),
    "left"
).select("patient_id", "location_sk")

fact_patient_df = fact_base_df \
    .join(encounters_count_df, fact_base_df.patient_id == encounters_count_df.patient, "left") \
    .join(conditions_count_df, fact_base_df.patient_id == conditions_count_df.patient, "left") \
    .join(payer_df, fact_base_df.patient_id == payer_df.patient, "left") \
    .select(
        fact_base_df["patient_id"],
        col("total_encounters"),
        col("total_conditions"),
        col("payer").alias("payer_id"),
        "location_sk"
    ).fillna({"total_encounters": 0, "total_conditions": 0}) \
    .withColumn("created_at", current_timestamp()) \
    .withColumn("modified_at", current_timestamp())

fact_patient_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", "s3://bucp2final/output/patient/fact_patient/") \
    .saveAsTable(f"{database_name}.fact_patient")

job.commit()
