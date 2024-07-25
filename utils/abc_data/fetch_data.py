import os
import requests
import boto3
from botocore.exceptions import NoCredentialsError

# Variables
FILE_URL = "http://example.com/file.zip"
FILE_NAME = "file.zip"
S3_BUCKET = "your-bucket-name"
S3_KEY = "file.zip"  # The key under which the file will be stored in S3


def download_file(url, local_filename):
    # Perform the GET request with SSL verification turned off
    response = requests.get(url, verify=False, stream=True)

    # Check if the request was successful
    if response.status_code == 200:
        # Open the local file for writing in binary mode
        with open(local_filename, "wb") as file:
            # Write the content in chunks to avoid memory issues with large files
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"Download successful: {local_filename}")
    else:
        print(f"Failed to download file: {response.status_code}")


def upload_to_s3(file_name, bucket, key):
    s3 = boto3.client("s3")
    try:
        s3.upload_file(file_name, bucket, key)
        print(f"Upload Successful: {file_name} to s3://{bucket}/{key}")
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")


if __name__ == "__main__":

    # Iterate over all files in the text file
    # Open the file in read mode
    try:
        with open("meta_all_data.txt", "r") as file:
            # Iterate over each line in the file
            for line in file:
                # Process each line
                file_url, file_name = line.strip().split(" ")
                # file_url = line.strip()
                file_url = file_url
                file_name = "meta/" + file_name

                try:
                    print(f"Downloading {file_url}...")
                    # Download the file
                    download_file(file_url, file_name)

                except Exception as e:
                    print(e)
                    print(f"Failed to download {file_url}...moving on..")
                    continue

                # Upload the file to S3
                try:
                    print(f"Uploading {file_url} to S3...")
                    # upload_to_s3(FILE_NAME, S3_BUCKET, S3_KEY)

                except Exception as e:
                    print(e)
                    print(f"Failed to upload {file_url}...moving on..")
                    continue

                # Clean up by removing the downloaded file
                # os.remove(FILE_NAME)

    except Exception as e:
        print(e)
        print("not saving due to error...")
