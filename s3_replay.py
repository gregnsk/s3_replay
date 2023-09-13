#!/usr/bin/python3
# gregory.touretsky@gmail.com 12-Sep-2023
# S3 Traffic Replay Tool: replay Amazon S3 workloads based on recorded traffic from a provided CSV file.
# CSV file format (not all fields are used):
#     "ReqDate","ReqHour","ReqMinute","ReqTime","tenant_id","datacenter","bucket_name","request_origin_ip","api_verb","UploadId","partNumber","ListPrefix","ListMaxKeys","objName","uri_config","ReqLength"

import boto3
from botocore.config import Config
import csv
import os
from concurrent.futures import ThreadPoolExecutor
import sys
import threading
from datetime import datetime

# S3 configuration details
S3_ENDPOINT = 'https://s3.us-east-1.lyvecloud.seagate.com'
REGION_NAME = 'us-east-1'
AWS_ACCESS_KEY = ''
AWS_SECRET_KEY = ''


multipart_uploads = {}
multipart_lock = threading.Lock()
totalRows = 0
totalRowsLock = threading.Lock()
putObjects = 0
putObjectsLock = threading.Lock()
putObjectsBytes = 0
putObjectsBytesLock = threading.Lock()
mpartObjects = 0
mpartObjectsLock = threading.Lock()
mpartObjectBytes = 0
mpartObjectBytesLock = threading.Lock()
delObjects = 0 
delObjectsLock = threading.Lock()
listObjects = 0
listObjectsLock = threading.Lock()
listObjectsV2 = 0
listObjectsV2Lock = threading.Lock()

bucket_cache = set()
bucket_cacheLock = threading.Lock()


config = Config(
    retries={'max_attempts': 10},
    max_pool_connections=100  # Increase this based on your needs and resources
)

s3 = boto3.client('s3',
                  endpoint_url=S3_ENDPOINT,
                  region_name=REGION_NAME,
                  aws_access_key_id=AWS_ACCESS_KEY,
                  aws_secret_access_key=AWS_SECRET_KEY,
                  config=config)

def print_report():
    print(f"{datetime.now()} - Processed {totalRows} rows. New objects: {putObjects} of {putObjectsBytes} bytes; New Mpart objects: {mpartObjects} of {mpartObjectBytes} bytes; DeleteObject: {delObjects}; ListObjects: {listObjects}; ListObjectsV2: {listObjectsV2} ")

# File creation
def create_temp_file(size):
    file_path = f"/tmp/testfiles/{size}.tmp"
    if not os.path.exists(file_path):
        with open(file_path, "wb") as f:
            f.seek(size - 1)
            f.write(b'\0')
    return file_path

# Upload to S3
def upload_to_s3(bucket, obj_name, file_path):
    s3.upload_file(file_path, bucket, obj_name)

# Main worker function
def process_row(row):
    global totalRows,putObjects,putObjectsBytes,mpartObjects,mpartObjectBytes,delObjects,listObjects,listObjectsV2,bucket_cache

    with totalRowsLock:
        totalRows += 1

    bucket_name = "test-" + row["bucket_name"]

    if bucket_name not in bucket_cache:
        try:
            s3.create_bucket(Bucket=bucket_name)
            with bucket_cacheLock:
                bucket_cache[bucket_name] = True
        except s3.exceptions.BucketAlreadyOwnedByYou:
            with bucket_cacheLock:
                bucket_cache[bucket_name] = True
        except s3.exceptions.BucketAlreadyExists:
            with bucket_cacheLock:
                bucket_cache[bucket_name] = True
        except Exception as err:
            print(f"Unexpected {err=}, {type(err)=}")
            raise

    if row["api_verb"] == "PutObject":
        size = int(row["ReqLength"])
        file_path = create_temp_file(size)
        upload_to_s3(bucket_name, row["objName"], file_path)
        with putObjectsLock:
            putObjects += 1
        with putObjectsBytesLock:
            putObjectsBytes += size
    elif row["api_verb"] == "DeleteObject":
        try:
            s3.delete_object(Bucket=bucket_name, Key=row["objName"])
            with delObjectsLock:
                delObjects += 1
        except s3.exceptions.NoSuchKey:
            print(f"Warning: No such object '{row['objName']}' to delete in bucket '{bucket_name}'")
        except Exception as err:
            print(f"Unexpected {err=}, {type(err)=}")
            raise
    elif row["api_verb"] == "UploadPart":
        with multipart_lock:
            if row["UploadId"] not in multipart_uploads:
                # Initiate a new multipart upload
                response = s3.create_multipart_upload(Bucket=bucket_name, Key=row["objName"])
                upload_id = response['UploadId']
                part_num = 1
                multipart_uploads[row["UploadId"]] = {
                    "uploadId": upload_id,
                    "etags": [],
                    "next_part_num": 2,
                    "bucket_name": row["bucket_name"],
                    "objName": row["objName"]
                }
            else:
                # Get stored values
                upload_id = multipart_uploads[row["UploadId"]]["uploadId"]
                part_num = multipart_uploads[row["UploadId"]]["next_part_num"]
                multipart_uploads[row["UploadId"]]["next_part_num"] += 1

            size = int(row["ReqLength"])
            file_path = create_temp_file(size)
            with open(file_path, 'rb') as file:
               response = s3.upload_part(Bucket=bucket_name, Key=row["objName"], PartNumber=part_num, UploadId=upload_id, Body=file)
            multipart_uploads[row["UploadId"]]["etags"].append({'ETag': response['ETag'], 'PartNumber': part_num, 'Size': size})

    elif row["api_verb"] == "CompleteMultipartUpload":
        with multipart_lock:
            if row["UploadId"] not in multipart_uploads:
                print("DEBUG: Unknown Multipart Upload")
            else:
                # Complete the multipart upload
                parts_for_completion = [{'ETag': part['ETag'], 'PartNumber': part['PartNumber']} for part in multipart_uploads[row["UploadId"]]["etags"]]

                response = s3.complete_multipart_upload(
                    Bucket=bucket_name,
                    Key=row["objName"],
                    UploadId=multipart_uploads[row["UploadId"]]["uploadId"],
                    MultipartUpload={'Parts': parts_for_completion}
                )
                with mpartObjectsLock:
                    mpartObjects += 1
                with mpartObjectBytesLock:
                    try:
                        mpartObjectBytes += sum(int(part["Size"]) for part in multipart_uploads[row["UploadId"]]["etags"])
                    except Exception as err:
                        print(f"Unexpected {err=}, {type(err)=}")
                        raise

                # Remove the completed upload from the dictionary
                del multipart_uploads[row["UploadId"]]


    elif row["api_verb"] == "ListObjects":
        with listObjectsLock:
            listObjects += 1
        prefix = row.get("ListPrefix", "")
        try:
            max_keys = int(row.get("ListMaxKeys", 1000))
        except ValueError:
            max_keys = 1000

        try:
             response = s3.list_objects(Bucket=bucket_name, Prefix=prefix, MaxKeys=max_keys)
        except Exception as err:
            print(f"Unexpected {err=}, {type(err)=}")
            raise

        #print("Contents: " + response.get("Contents", []))

    elif row["api_verb"] == "ListObjectsV2":
        with listObjectsV2Lock:
            listObjectsV2 += 1
        prefix = row.get("ListPrefix", "")
        try:
            max_keys = int(row.get("ListMaxKeys", 1000))
        except ValueError:
            max_keys = 1000

        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=max_keys)
        #print("Contents: " + response.get("Contents", []))

    if totalRows % rows_interval == 0:
        print_report()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: script_name <CSV_FILE_PATH> <NUM_THREADS> [ROWS_INTERVAL]")
        sys.exit(1)

    csv_file_path = sys.argv[1]
    num_threads = int(sys.argv[2])
    rows_interval = int(sys.argv[3]) if len(sys.argv) > 3 else 1000
    bucket_cache = {}

    if not os.path.exists("/tmp/testfiles"):
        os.makedirs("/tmp/testfiles")
    with open(csv_file_path, "r") as csvfile:
        csvreader = csv.DictReader(csvfile)
        rows = list(csvreader)

        # Parallelizing the workload
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            executor.map(process_row, rows)

    # Complete remaining multipart uploads
    for uploadId, upload_data in multipart_uploads.items():
        try:
            bucket_name = "test-" + upload_data["bucket_name"]
            parts_for_completion = [{'ETag': part['ETag'], 'PartNumber': part['PartNumber']} for part in upload_data["etags"]]

            response = s3.complete_multipart_upload(
                Bucket=bucket_name,
                Key=upload_data["objName"],
                UploadId=upload_data["uploadId"],
                MultipartUpload={'Parts': parts_for_completion}
            )
            # Increment the multipart objects count
            mpartObjects += 1

            # Calculate the total bytes for this multipart object
            mpartObjectBytes += sum(int(part["Size"]) for part in upload_data["etags"])

        except Exception as err:
            print(f"Error completing multipart upload {uploadId}. {err=}")

    print_report()
