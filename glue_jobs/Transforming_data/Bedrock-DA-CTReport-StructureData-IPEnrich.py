import sys
import boto3
import pandas as pd
import requests
import io
import json
import re
from datetime import timedelta
from awsglue.utils import getResolvedOptions

# =========================
# READ ARGUMENTS FROM LAMBDA
# =========================
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'INPUT_BUCKET',
    'INPUT_KEY',
    'OUTPUT_PREFIX',
    'IPINFO_TOKEN'
])

INPUT_BUCKET = args['INPUT_BUCKET']
INPUT_KEY = args['INPUT_KEY']
OUTPUT_PREFIX = args['OUTPUT_PREFIX']
IPINFO_TOKEN = args['IPINFO_TOKEN']

report_id = INPUT_KEY.split("/")[1].split(".")[0]
print(f"Reading input file from s3://{INPUT_BUCKET}/{INPUT_KEY}")
print(f"Report Number is {report_id}")


# Optional: derive output filenames dynamically from input file name
input_filename = INPUT_KEY.split("/")[-1].replace(".json", "")
OUTPUT_KEY_CSV = f"{OUTPUT_PREFIX}/{report_id}/{report_id}_enriched.csv"
#OUTPUT_KEY_JSON = f"{OUTPUT_PREFIX}/{input_filename}_enriched.json"

# =========================
# AWS CLIENT
# =========================
s3 = boto3.client("s3")

print(f"Reading input file from s3://{INPUT_BUCKET}/{INPUT_KEY}")

# =========================
# LOAD JSON FILE FROM S3
# =========================
obj = s3.get_object(Bucket=INPUT_BUCKET, Key=INPUT_KEY)
raw_json = json.loads(obj["Body"].read().decode("utf-8"))

# =========================
# EXTRACT inference_result
# =========================
inf = raw_json.get("inference_result", {})

base_record = {
    "source_bucket": INPUT_BUCKET,
    "source_key": INPUT_KEY,
    "priority_level": inf.get("Priority Level of the CyberTipline Report", ""),
    "mobile_phone": inf.get("Mobile Phone Number of the Suspect in CyberTipline Report", ""),
    "report_number": inf.get("CyberTipline Report Number", ""),
    "esp": inf.get("Electronic Service Provider (ESP) of Cybertipline Report", ""),
    "imei": inf.get("IMEI of the Mobile of Suspect", ""),
    "email": inf.get("Email Address of the Suspect in CyberTipline Report", "")
}

ip_entries = inf.get("IP Addresses of the Suspect in CyberTipline Report", [])

# =========================
# PARSE EACH IP ENTRY
# =========================
rows = []

ip_regex = r'(\d{1,3}(?:\.\d{1,3}){3})'
type_regex = r'\((.*?)\)'
port_regex = r'Port:\s*(\d+)'
ts_regex = r'(\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2}:\d{2})\s*UTC'

for entry in ip_entries:
    row = base_record.copy()
    row["raw_ip_entry"] = entry

    # Extract IP
    ip_match = re.search(ip_regex, entry)
    row["ip_address"] = ip_match.group(1) if ip_match else ""

    # Extract type
    type_match = re.search(type_regex, entry)
    row["ip_type"] = type_match.group(1) if type_match else ""

    # Extract port
    port_match = re.search(port_regex, entry)
    row["port"] = port_match.group(1) if port_match else ""

    # Extract timestamp UTC
    ts_match = re.search(ts_regex, entry)
    utc_ts_str = ts_match.group(1) if ts_match else ""

    row["timestamp_utc"] = ""
    row["timestamp_pkt"] = ""

    if utc_ts_str:
        try:
            dt_utc = pd.to_datetime(utc_ts_str, format="%m-%d-%Y %H:%M:%S", errors="coerce")
            if pd.notnull(dt_utc):
                dt_pkt = dt_utc + timedelta(hours=5)
                row["timestamp_utc"] = dt_utc.strftime("%Y-%m-%d %H:%M:%S")
                row["timestamp_pkt"] = dt_pkt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            print(f"Timestamp parse failed for entry: {entry} | Error: {e}")

    rows.append(row)

# If no IP rows found, still create one row for metadata
if not rows:
    rows.append(base_record.copy())

# =========================
# CREATE DATAFRAME
# =========================
df = pd.DataFrame(rows)

# =========================
# WHOIS / ASN LOOKUP
# =========================
def lookup_ipinfo(ip):
    """
    Lookup IP enrichment from ipinfo.io
    """
    if not ip:
        return {
            "hosting_company": "",
            "asn_org": "",
            "hostname": "",
            "country": "",
            "region": "",
            "city": ""
        }

    try:
        url = f"https://ipinfo.io/{ip}/json?token={IPINFO_TOKEN}"
        resp = requests.get(url, timeout=10)
        data = resp.json()

        return {
            "hosting_company": data.get("org", ""),
            "asn_org": data.get("org", ""),
            "hostname": data.get("hostname", ""),
            "country": data.get("country", ""),
            "region": data.get("region", ""),
            "city": data.get("city", "")
        }
    except Exception as e:
        print(f"Lookup failed for IP {ip}: {e}")
        return {
            "hosting_company": "",
            "asn_org": "",
            "hostname": "",
            "country": "",
            "region": "",
            "city": ""
        }

# =========================
# ENRICH UNIQUE IPS
# =========================
ip_cache = {}

hosting_company_list = []
asn_org_list = []
hostname_list = []
country_list = []
region_list = []
city_list = []

for ip in df.get("ip_address", []):
    if ip not in ip_cache:
        ip_cache[ip] = lookup_ipinfo(ip)

    info = ip_cache[ip]
    hosting_company_list.append(info["hosting_company"])
    asn_org_list.append(info["asn_org"])
    hostname_list.append(info["hostname"])
    country_list.append(info["country"])
    region_list.append(info["region"])
    city_list.append(info["city"])

if "ip_address" in df.columns:
    df["hosting_company"] = hosting_company_list
    df["asn_org"] = asn_org_list
    df["hostname"] = hostname_list
    df["country"] = country_list
    df["region"] = region_list
    df["city"] = city_list

# =========================
# SAVE CSV TO S3
# =========================
csv_buffer = io.StringIO()
df.to_csv(csv_buffer, index=False)

s3.put_object(
    Bucket=INPUT_BUCKET,
    Key=OUTPUT_KEY_CSV,
    Body=csv_buffer.getvalue().encode("utf-8"),
    ContentType="text/csv"
)


print("Glue job completed successfully.")
print(f"CSV written to s3://{INPUT_BUCKET}/{OUTPUT_KEY_CSV}")
