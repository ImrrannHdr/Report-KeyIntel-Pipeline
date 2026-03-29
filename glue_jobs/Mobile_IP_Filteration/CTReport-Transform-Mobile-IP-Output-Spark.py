import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

from pyspark.sql.functions import col, regexp_replace, substring, when
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'INPUT_BUCKET',
    'INPUT_KEY'
])
# =========================
# INIT
# =========================
#args = getResolvedOptions(sys.argv, ['JOB_NAME'])

INPUT_BUCKET = args['INPUT_BUCKET']
INPUT_KEY = args['INPUT_KEY']


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# =========================
# INPUT
# =========================
# NEW - read directly via Spark, exact file, no recurse ambiguity
input_path = f"s3://{INPUT_BUCKET}/{INPUT_KEY}"
print("Reading from:", input_path)

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path)

print("Columns loaded:", df.columns)
df.printSchema()



# =========================
# SELECT REQUIRED COLUMNS
# =========================
df_mobile = df.select("priority_level", "mobile_phone", "report_number", "esp")
df_ip = df.select("priority_level", "report_number", "esp", "ip_address", "timestamp_pkt", "hosting_company")



# =========================
# NORMALIZE MOBILE NUMBERS
# =========================
df_mobile = df_mobile.withColumn("mobile_clean", regexp_replace(col("mobile_phone"), r"\+", ""))

df_mobile = df_mobile.withColumn(
    "mobile_clean",
    regexp_replace(col("mobile_clean"), r"^0", "92")
)

# Extract prefix
df_mobile = df_mobile.withColumn("prefix", substring(col("mobile_clean"), 3, 3))

# =========================
# CLASSIFY OPERATORS
# =========================
df_mobile = df_mobile.withColumn(
    "operator",
    when(col("prefix").isin("300","301","302","303","304","305","306","307","308","309","320","321","322","323","324","325","326","327","328","329"), "Mobilink")
    .when(col("prefix").isin("333","330","331","332","334","335","336","337"), "Ufone")
    .when(col("prefix").isin("340","341","342","343","344","345","346","347","348","349"), "Telenor")
    .when(col("prefix").isin("310","311","312","313","314","315","316","317","318","319"), "Zong")
)

# Remove invalid rows
df_mobile = df_mobile.filter(col("operator").isNotNull())

# ✅ Normalize final mobile format (keep only clean number)
df_mobile = df_mobile.withColumn("mobile_phone", col("mobile_clean"))

# ✅ DROP DUPLICATES (KEY FIX)
df_mobile = df_mobile.dropDuplicates(["mobile_phone"])

# =========================
# PTCL IP FILTER
# =========================
df_ip_filtered = df_ip.filter(
    col("hosting_company").like("%Pakistan Telecommunication Company Limited%") |
    col("hosting_company").like("%PTCL%") |
    col("hosting_company").like("%Pakistan Telecommunication%")
)
df_ip_filtered = df_ip_filtered.dropDuplicates([
    "ip_address",
    "timestamp_pkt",
    
])

# =========================
# WRITE FUNCTION (NO EMPTY FILES)
# =========================
def write_if_not_empty(df, path, name):
    if not df.rdd.isEmpty():
        print(f"{name}: writing data")

        df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(path)
    else:
        print(f"{name}: no data — skipping")

# =========================
# OUTPUT PATHS
# =========================
base_output = "s3://bedrock-da-ctreport-keyinfo-mobile-ip-email/"

# =========================
# WRITE OPERATOR DATA
# =========================
write_if_not_empty(df_mobile.filter(col("operator") == "Mobilink"), base_output + "mobilink/", "Mobilink")
write_if_not_empty(df_mobile.filter(col("operator") == "Ufone"), base_output + "ufone/", "Ufone")
write_if_not_empty(df_mobile.filter(col("operator") == "Telenor"), base_output + "telenor/", "Telenor")
write_if_not_empty(df_mobile.filter(col("operator") == "Zong"), base_output + "zong/", "Zong")

# =========================
# WRITE PTCL IP DATA
# =========================
write_if_not_empty(df_ip_filtered, base_output + "ptcl/", "PTCL")
print("INPUT_BUCKET:", INPUT_BUCKET)
print("INPUT_KEY:", INPUT_KEY)
print("INPUT_PATH:", input_path)
print("COLUMNS:", df.columns)
df.printSchema()
# =========================
# OPTIONAL: SINGLE PARTITIONED OUTPUT (BETTER FOR ATHENA)
# =========================
# if not df_mobile.rdd.isEmpty():
#     df_mobile.write.partitionBy("operator").mode("overwrite").csv(base_output + "all_operators/")

# =========================
# COMPLETE JOB
# =========================
job.commit()