# S3 Workload Replay Tool

This tool is designed to read a CSV file containing S3 request traffic and replay it on a test environment, allowing you to simulate real-world traffic patterns on your S3 infrastructure.

## Features:

- Reads S3 traffic data from a CSV file.
- Replays `PutObject`, `DeleteObject`, `UploadPart`, `CompleteMultipartUpload`, `ListObjects`, and `ListObjectsV2` operations.
- Supports concurrent operations using threading.
- Uses local caching to avoid repeated S3 operations (like bucket existence checks).
- Efficient temporary file creation for object puts.
- Detailed progress reporting after processing a set number of rows and a final summary at the end.

## Requirements:

- Python 3.x
- `boto3` library for AWS operations (can be installed via `pip install boto3`)

## Usage:

```bash
python s3_replay_tool.py <CSV_FILE_PATH> <NUM_THREADS> [ROWS_INTERVAL]
```

**Parameters:**

- `CSV_FILE_PATH`: Path to the CSV file containing the S3 traffic data.
- `NUM_THREADS`: Number of threads to use for concurrent S3 operations.
- `ROWS_INTERVAL` (optional): Number of rows after which to print a progress report. Default is 1000.

## CSV File Format:

The CSV file should have the following columns:

- `ReqDate`: Date of the request.
- `ReqHour`: Hour of the request.
- `ReqMinute`: Minute of the request.
- `bucket_name`: Name of the S3 bucket.
- `api_verb`: S3 API verb.
- `UploadId`: (For multipart uploads) ID of the upload.
- `partNumber`: (For multipart uploads) Number of the part being uploaded.
- `objName`: Name of the object being operated on.
- `ReqLength`: Length of the request in bytes.
- `ListPrefix`: (For ListObjects/ListObjectsV2) Prefix for listing.
- `ListMaxKeys`: (For ListObjects/ListObjectsV2) Maximum number of keys to return in the list.

## Limitations:

- This tool is designed for a test environment. Ensure you have the necessary permissions and understand the consequences of replaying traffic, especially if using in a production-like setting.
- The tool assumes that rows in the CSV for a particular `UploadId` are in order (i.e., all parts for a given `UploadId` are uploaded before a `CompleteMultipartUpload` verb appears for that `UploadId`).

## Author:
gregory.touretsky@gmail.com

Contributions are welcome. Please open an issue or submit a pull request on the project's GitHub page.

