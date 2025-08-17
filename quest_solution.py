import os
import json
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import boto3
import pandas as pd

# User Inputs
#AWS S3
S3_BUCKET_NAME = "rearc-quest-data-bhagath"
AWS_REGION = "ap-south-1"

# DATASETS
DATA_DIR = "data"
DATASET1_URL = "https://download.bls.gov/pub/time.series/pr"
DATASET1_HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:141.0) Gecko/20100101 Firefox/141.0"}

DATASET2_URL = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
DATASET2_JSON_FILE = "usa_population.json"

# Helper functions
def create_s3_bucket(bucket_name):
    """
    Function to create an S3 bucket if it does not already exist.
    """
    s3 = boto3.client('s3',region_name=AWS_REGION)
    bucket_exists = any([ bucket["Name"] for bucket in boto3.client('s3').list_buckets()["Buckets"] 
                 if bucket["Name"] == bucket_name])
    if bucket_exists:
        print(f"Bucket {bucket_name} already exists")
    else:
        print(f"Creating bucket {bucket_name}")
        s3.create_bucket(Bucket=bucket_name,CreateBucketConfiguration={'LocationConstraint': AWS_REGION})

def get_dataset1_info():
    """
    Function to retrive file, URL mappings for first dataset.
    """
    response = requests.get(DATASET1_URL,headers=DATASET1_HEADERS)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, 'html.parser')
    files_dict = {}
    for a in soup.find_all('a'):
        href = a.get('href')
        if href and not href.endswith('/'):  # skip directories & parent links
            filename = href.split('/')[-1]
            files_dict[filename] = urljoin(DATASET1_URL, href)
    return files_dict

def get_dataset2_json():
    """
    Function to download the second dataset as JSON.
    """
    response = requests.get(DATASET2_URL)
    response.raise_for_status()
    data = response.json()
    download_dir = os.path.join(DATA_DIR, "dataset2")
    os.makedirs(download_dir, exist_ok=True)
    with open(os.path.join(download_dir, DATASET2_JSON_FILE), 'w') as f:
        json.dump(data, f, indent=4)
    print(f"Dataset 2 JSON saved to {DATASET2_JSON_FILE}")

def get_s3_objects(bucket_name,s3_prefix=""):
    """
    Function to get list of objects(files) in an S3 bucket.
    """
    s3 = boto3.client('s3',region_name=AWS_REGION)
    objects = []

    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix):
        if 'Contents' in page:
            for item in page['Contents']:
                key = item['Key']
                if not key.endswith('/'):  # skip folder marker keys
                    if key.startswith(s3_prefix):
                        key = key.split(s3_prefix)[1]
                    objects.append(key)

    return objects

def upload_files_to_s3(files_info, existing_s3_files, s3_prefix=""):
    """
    Function to upload files to S3 bucket from Dataset 1.
    """
    s3 = boto3.client('s3',region_name=AWS_REGION)
    files_to_download = [ "pr.data.0.Current" ]

    download_dir = os.path.join(DATA_DIR, "dataset1")
    os.makedirs(download_dir, exist_ok=True)

    for filename, file_url in files_info.items():
        #skip filenames with no urls to download, usually these are not part of Dataset1
        if file_url is None: continue
        file_resp = None
        if filename not in existing_s3_files:
            file_resp = requests.get(file_url, stream=True, headers=DATASET1_HEADERS)
            file_resp.raise_for_status()
            print(f"Uploading {filename} to S3...")
            s3.upload_fileobj(file_resp.raw, S3_BUCKET_NAME, f"{s3_prefix}{filename}")
        else:
            print(f"Skipping {filename}, already in S3.")
        
        filepath = os.path.join(download_dir, filename)
        if filename in files_to_download and not os.path.exists(filepath):    
            print(f"Downloading {filename} to local directory...")
            if file_resp is None:
                file_resp = requests.get(file_url, stream=True, headers=DATASET1_HEADERS)
                file_resp.raise_for_status()
            with open(filepath, "wb") as f:
                for chunk in file_resp.iter_content(chunk_size=8192):
                    f.write(chunk)

def remove_files_from_s3(files_info, existing_s3_files,s3_prefix=""):
    """
    Function to delete files from S3 bucket that are not present in Dataset 1.
    """
    files_to_delete = set(existing_s3_files) - set(files_info.keys())
    s3 = boto3.client('s3',region_name=AWS_REGION)
    if files_to_delete:
        print("Deleting obsolete files from S3...")
        delete_objects = [{"Key": f"{s3_prefix}{f}"} for f in files_to_delete]
        s3.delete_objects(Bucket=S3_BUCKET_NAME, Delete={"Objects": delete_objects})
        print(f"Deleted {len(delete_objects)} files.")
    else:
        print("No files to delete.")

def generate_index_html(prefix):
    """
    Create an index.html to place in s3 and serve the bucket objects as static webpage
    """
    s3 = boto3.client("s3", region_name=AWS_REGION)

    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
    if "Contents" not in response:
        print("No files found in S3 folder.")
        return

    html_lines = [
        "<!DOCTYPE html>",
        "<html><head><title>Dataset Files</title></head><body>",
        f"<h2>Files in {prefix}</h2>",
        "<ul>"
    ]

    for obj in response["Contents"]:
        key = obj["Key"]
        if key.endswith("/"):  # skip "folder markers"
            continue
        filename = key.split("/")[-1]
        file_url = f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{key}"
        html_lines.append(f'<li><a href="{file_url}">{filename}</a></li>')

    html_lines.append("</ul></body></html>")
    html_content = "\n".join(html_lines)

    # upload index.html into the folder
    index_key = prefix + "index.html"
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=index_key,
        Body=html_content,
        ContentType="text/html"
    )

    print(f"index.html uploaded to s3://{S3_BUCKET_NAME}/{index_key}")
    print(f"Access it via: https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{index_key}")

def sync_bucket(s3_prefix="dataset/"):
    """
    Function to synchronize files with S3 bucket.
    and uploads Dataset 2 JSON to S3.
    """
    remote_files = get_dataset1_info()
    s3_files = get_s3_objects(S3_BUCKET_NAME,s3_prefix)
    remote_files["index.html"] = None
    remote_files[DATASET2_JSON_FILE] = None
    upload_files_to_s3(remote_files,s3_files,s3_prefix)
    remove_files_from_s3(remote_files,s3_files,s3_prefix)
    
    print("Uploading Dataset 2 JSON to S3...")
    get_dataset2_json()
    dataset2_path = os.path.join(DATA_DIR, "dataset2", DATASET2_JSON_FILE)
    s3 = boto3.client('s3',region_name=AWS_REGION)
    s3.upload_file(dataset2_path, S3_BUCKET_NAME, f"{s3_prefix}{DATASET2_JSON_FILE}")
    print(f"Uploaded {DATASET2_JSON_FILE} to S3 bucket {S3_BUCKET_NAME}")

    print("S3 Sync complete.")
    generate_index_html(s3_prefix)

# Data sourcing

create_s3_bucket(S3_BUCKET_NAME)
sync_bucket()

# Data analytics

df1 = pd.read_csv(os.path.join(DATA_DIR, "dataset1", "pr.data.0.Current"), sep="\t")
df1.columns = df1.columns.str.strip()
df1 = df1.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
ds2_json = json.load(open(os.path.join(DATA_DIR, "dataset2", DATASET2_JSON_FILE)))
df2 = pd.json_normalize(ds2_json['data'])

html_lines = [
        "<!DOCTYPE html>",
        "<html><head><title>Data Analytics</title></head><body>",
        f"<h2>Data Analytics</h2>"
    ]

pop_df = df2[(df2['Year']>=2013) & (df2['Year']<=2018)]['Population']
html_lines.append("<h3>Population Data statistics from 2013 to 2918</h3>")
html_lines.append(f"<p>Mean: {pop_df.mean()}</p>")
html_lines.append(f"<p>Standard Deviation: {pop_df.std()}</p>")
print("Population Data statistics from 2013 to 2918")
print(f"Mean: {pop_df.mean()}")
print(f"Standard Deviation: {pop_df.std()}")

df1_by_sid_yr = df1.groupby(['series_id','year'], as_index=False)['value'].sum()
df1_by_best_yr = df1_by_sid_yr.loc[df1_by_sid_yr.groupby("series_id")["value"].idxmax()].reset_index(drop=True)
html_lines.append("<h3>Best year info per series_id</h3>")
html_lines.append(df1_by_best_yr.to_html(index=False))

series_id = 'PRS30006032'
period = 'Q01'
year = 2018


filtered_df1 = df1[(df1['series_id'] == series_id) 
                   & (df1['period'] == period)
                   & (df1['year'] == year)]
merged_df = pd.merge(filtered_df1,df2,
         left_on=['year'],
         right_on=['Year'],
         how='inner')[['series_id','year','period','value','Population']]
html_lines.append(f"<h3>Population details for series_id: {series_id}, period: {period}, year: {year} </h3>")
html_lines.append(merged_df.to_html(index=False))

html_lines.append("</body></html>")
html_content = "\n".join(html_lines)

report_dir = os.path.join(DATA_DIR,"reports")
os.makedirs(report_dir, exist_ok=True)
html_path = os.path.join(report_dir,"report.html")
with open(html_path,"w") as html_file:
    html_file.write(html_content)
print(f"Report saved at {html_path}")