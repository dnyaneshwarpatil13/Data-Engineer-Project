import pymysql
import pandas as pd
from io import StringIO
import boto3
from datetime import datetime

# ==============================
# CONFIG
# ==============================
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = "admin"
MYSQL_DB = "dny"
MYSQL_PORT = 3306

TABLE_NAME = "employ"
ID_COL = "id"

BUCKET_NAME = "project-second-practice"
PREFIX = "row-data/"  # S3 folder, include trailing slash
REGION = "eu-north-1"

# ==============================
# 1️⃣ Connect to MySQL
# ==============================
conn = pymysql.connect(
     host="localhost",
    user="root",
    password="admin",
    database="dny",
    port=3306
)

print("✅ Connected to MySQL!")

# ==============================
# 2️⃣ Find last uploaded file in S3
# ==============================
s3 = boto3.client(
    's3',
    aws_access_key_id="AWS_ACCESS_KEY_ID",
    aws_secret_access_key="AWS_SECRET_ACCESS_KEY",
    region_name=REGION
)

response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)

if "Contents" not in response:
    print("⚠️ No files found in S3. Will do a full load.")
    last_id = 0
else:
    # Get latest file by LastModified
    latest_file = max(response["Contents"], key=lambda x: x["LastModified"])["Key"]
    print("📂 Latest uploaded file in S3:", latest_file)

    # Read CSV from S3
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=latest_file)
    df_old = pd.read_csv(obj["Body"])

    # Get last ID from that file
    last_id = df_old[ID_COL].max()
    print("🆔 Last record ID from latest S3 file:", last_id)

# ==============================
# 3️⃣ Fetch new records from MySQL
# ==============================
query = f"SELECT * FROM {TABLE_NAME} WHERE {ID_COL} > {last_id} ORDER BY {ID_COL} ASC;"
df_new = pd.read_sql(query, conn)

if df_new.empty:
    print("✅ No new records to upload.")
else:
    print(f"✅ Found {len(df_new)} new records.")

    # ==============================
    # 4️⃣ Convert DataFrame to CSV in memory
    # ==============================
    csv_buffer = StringIO()
    df_new.to_csv(csv_buffer, index=False)

    # Timestamp for file name
    upload_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    object_key = f"{PREFIX}mydata_{upload_time}.csv"

    # ==============================
    # 5️⃣ Upload new CSV to S3
    # ==============================
    s3.put_object(Bucket=BUCKET_NAME, Key=object_key, Body=csv_buffer.getvalue())
    print("✅ Data uploaded to S3:", f"s3://{BUCKET_NAME}/{object_key}")

# ==============================
# 6️⃣ Close MySQL connection
# ==============================
conn.close()
print("🔌 MySQL connection closed.")
