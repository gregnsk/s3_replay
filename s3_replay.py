#!/usr/bin/python3
# gregory.touretsky@gmail.com 12-Sep-2023
# S3 Traffic Replay Tool: replay Amazon S3 workloads based on recorded traffic from a provided CSV file.
# CSV file format (not all fields are used):
#     "ReqDate","ReqHour","ReqMinute","ReqTime","tenant_id","datacenter","bucket_name","request_origin_ip","api_verb","UploadId","partNumber","ListPrefix","ListMaxKeys","objName","uri_config","ReqLength"

import boto3
import csv
import os
from concurrent.futures import ThreadPoolExecutor
import sys
import threading
from datetime import datetime


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


# S3 configuration details
S3_ENDPOINT = 'https://s3.us-east-1.lyvecloud.seagate.com'
REGION_NAME = 'us-east-1'
AWS_ACCESS_KEY = ''
AWS_SECRET_KEY = ''

s3 = boto3.client('s3',
                  endpoint_url=S3_ENDPOINT,
                  region_name=REGION_NAME,
                  aws_access_key_id=AWS_ACCESS_KEY,
                  aws_secret_access_key=AWS_SECRET_KEY)

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
    global totalRows,putObjects,putObjectsBytes,mpartObjects,mpartObjectBytes,delObjects,listObjects,listObjectsV2
    with totalRowsLock:
        totalRows += 1

    bucket_name = "test-" + row["bucket_name"]

    if bucket_name not in bucket_cache:
        try:
            s3.create_bucket(Bucket=bucket_name)
            bucket_cache[bucket_name] = True
        except s3.exceptions.BucketAlreadyOwnedByYou:
            bucket_cache[bucket_name] = True
        except s3.exceptions.BucketAlreadyExists:
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
                    "next_part_num": 2
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
            multipart_uploads[row["UploadId"]]["etags"].append({'ETag': response['ETag'], 'PartNumber': part_num})

    elif row["api_verb"] == "CompleteMultipartUpload":
        with multipart_lock:
            if row["UploadId"] not in multipart_uploads:
                print("DEBUG: Unknown Multipart Upload")
            else:
                # Complete the multipart upload
                response = s3.complete_multipart_upload(
                    Bucket=bucket_name,
                    Key=row["objName"],
                    UploadId=multipart_uploads[row["UploadId"]]["uploadId"],
                    MultipartUpload={'Parts': multipart_uploads[row["UploadId"]]["etags"]}
                )
                with mpartObjectsLock:
                    mpartObjects += 1
                with mpartObjectBytesLock:
                    mpartObjectBytes += sum(int(part["Size"]) for part in multipart_uploads[row["UploadId"]]["etags"])
                # Remove the completed upload from the dictionary
                del multipart_uploads[row["UploadId"]]


    elif row["api_verb"] == "ListObjects":
        with listObjectsLock:
            listObjects += 1
        prefix = row.get("ListPrefix", "")
        max_keys = int(row.get("ListMaxKeys", 1000))  # default to 1000 if not present

        response = s3.list_objects(Bucket=bucket_name, Prefix=prefix, MaxKeys=max_keys)
        #print(response.get("Contents", []))

    elif row["api_verb"] == "ListObjectsV2":
        with listObjectsV2Lock:
            listObjectsV2 += 1
        prefix = row.get("ListPrefix", "")
        max_keys = int(row.get("ListMaxKeys", 1000))  # default to 1000 if not present

        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=max_keys)
        #print(response.get("Contents", []))

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

    print_report()
