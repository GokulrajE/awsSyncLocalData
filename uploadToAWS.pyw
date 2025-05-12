import boto3
import csv
import logging
import os
from botocore.exceptions import NoCredentialsError
from datetime import datetime
import shutil
import sys
import subprocess
# AWS Configuration
bucketName = 'homer-data'


# Paths
keysFilePath = "C:/homer_accessKeys.csv"
# gameAssertPath = "D:/MARS-HOMER/Assets"

# LOCK_FILE = "C:/pythonscripts/upload.lock"

import psutil  # pip install psutil

lock_file_path = "C:/pythonscripts/upload.lock"

def is_another_instance_running():
    if os.path.exists(lock_file_path):
        with open(lock_file_path, 'r') as f:
            try:
                pid = int(f.read().strip())
                if psutil.pid_exists(pid):
                    return True
            except:
                pass
        # If PID is not valid or not running
        os.remove(lock_file_path)
    return False

def create_lock_file():
    with open(lock_file_path, 'w') as f:
        f.write(str(os.getpid()))

def remove_lock_file():
    if os.path.exists(lock_file_path):
        os.remove(lock_file_path)



def initializeLogging():
    logging.basicConfig(
        filename=awsLogFilePath,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

def loadAwsKeys(keysFilePath):
    try:
        with open(keysFilePath, mode='r') as file:
            csvReader = csv.reader(file)
            for rows in csvReader:
                keys = rows
            return keys[0], keys[1] 
    except FileNotFoundError:
        logging.error(f"Keys file not found")
       
    except Exception as e:
        logging.error(f"Error reading keys file")
       
def readUploadStatus(statusFilePath):
    if os.path.exists(statusFilePath):
        with open(statusFilePath, mode="r") as file:
            data = file.read().split(",")
            status = data[1]
            filepath = data[0]
            deviceName = data[2]
            print(status,filepath,deviceName)
    
            return status,filepath,deviceName
    else:
        logging.error(f"Upload status file not found")

def updateUploadStatus(statusFilePath, newStatus):
    try:
        with open(statusFilePath, mode="w") as file:
            file.write(newStatus)
    except Exception as e:
        logging.error(f"Failed to update upload status")
       
def uploadToAws(filePath, bucketName, objectName, accessKey, secretKey):
    try:
        uploader = boto3.client(
            's3',
             aws_access_key_id=accessKey,
            aws_secret_access_key=secretKey
        )
        uploader.upload_file(filePath, bucketName, objectName)
        logging.info(f"Successfully uploaded: {filePath} to {objectName}")
        print(f"Successfully uploaded: {filePath} to {objectName}")
    except FileNotFoundError:
        logging.error(f"File not found")
    except NoCredentialsError:
        logging.error("AWS credentials not available")
    except Exception as e:
        logging.error(f"Failed to upload")

def downloadConfigFile(bucketName,accessKey, secretKey):
    download = boto3.client(
            's3',
            aws_access_key_id=accessKey,
            aws_secret_access_key=secretKey
        )
    tempFilePath = "C:/pythonscripts/tempDownloadedFile.csv"

    try:
        logging.info("downloading configFile form aws s3")
        print(awsConfigFilePath)
        print(localConfigFilePath)
        download.download_file(bucketName, awsConfigFilePath, tempFilePath)
        shutil.copy2(tempFilePath, localConfigFilePath)
        os.remove(tempFilePath)
        logging.info("ConfigFile updated succesfully")
      
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            # File does not exist; upload it
            try:
                uploadToAws(localConfigFilePath,bucketName,awsConfigFilePath,accessKey,secretKey)
                logging.info("Config file uploaded to S3.")
            except Exception as upload_err:
                logging.error(f"Failed to upload config file: {upload_err}")
        else:
            logging.error(f"Unexpected error: {e}")
      
# #To get all file from the folder
# def uploadFolderToS3_(dataFolderPath, bucketName, accessKey, secretKey):
    currentDatetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    prefix = f"{userName}/{deviceName}/"
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=accessKey,
        aws_secret_access_key=secretKey
    )
    
    for root, dirs, files in os.walk(dataFolderPath):
       
        for file in files:
            if file.endswith('.meta'):
                continue
            
            localFilePath = os.path.join(root, file)
            relativePath = os.path.relpath(localFilePath, dataFolderPath).replace("\\", "/")
            
          
            if file == "configdata.csv":
                s3ObjectName = f"{userName}/{deviceName}/{file}"
                try:
                    # Check if configdata.csv exists in S3
                    s3_client.head_object(Bucket=bucketName, Key=s3ObjectName)
                    logging.info(f"'configdata.csv' already exists on S3. Skipping upload.")
                    continue  # Skip upload if exists
                except s3_client.exceptions.ClientError as e:
                    if int(e.response['Error']['Code']) == 404:
                        logging.info(f"'configdata.csv' not found on S3. Uploading...")
                        uploadToAws(localFilePath, bucketName, s3ObjectName, accessKey, secretKey)
                    else:
                        logging.error(f"Error checking configdata.csv on S3: {e}")
                continue  # Skip rest of loop for configdata.csv

            # For other files
            s3ObjectName = prefix + relativePath
            try:
                # Try to get metadata from S3
                response = s3_client.head_object(Bucket=bucketName, Key=s3ObjectName)
                s3_last_modified = response['LastModified'].timestamp()
                local_last_modified = os.path.getmtime(localFilePath)

                # Compare timestamps
                if local_last_modified > s3_last_modified:
                    print(f"File updated locally: {file}. Uploading...")
                    uploadToAws(localFilePath, bucketName, s3ObjectName, accessKey, secretKey)
                else:
                    print(f"No changes in: {file}. Skipping upload.")

            except s3_client.exceptions.ClientError as e:
                error_code = int(e.response['Error']['Code'])
                if error_code == 404:
                    # File does not exist on S3, upload it
                    print(f"File not found on S3: {file}. Uploading...")
                    uploadToAws(localFilePath, bucketName, s3ObjectName, accessKey, secretKey)
                else:
                    logging.error(f"Error checking {file} on S3: {e}")
def sync_folder_to_s3(local_folder, bucket_name):
   
    command = f'aws s3 sync "{local_folder}" s3://{bucketName}/{userName}/{deviceName}/ --exclude "configdata.csv" --exclude "*.meta"'

    logging.info(f"Starting sync: {command}")
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        logging.info("Sync output:\n" + result.stdout)
        if result.stderr:
            logging.warning("Sync errors:\n" + result.stderr)
        else:
            logging.info("Sync completed without errors.")
    except Exception as e:
        logging.error(f"Error running aws s3 sync: {e}")

   
def main():
   
    if is_another_instance_running():
        print("Another instance is already running. Exiting.")
        logging.info("Another instance is already running. Exiting.")
        sys.exit(0)

    create_lock_file()  # lock begins here
    global userName
    global awsConfigFilePath
    global gameAssertPath
    global awsLogFilePath
    
    global dataFolderPath 
    global localConfigFilePath
    global deviceName
    statusCode = ["upload_needed","no_upload"]
    uploadStatusFile =f"C:/DeviceSetups/PLUTO/uploadStatus.txt"
    data =  readUploadStatus(uploadStatusFile)
    gameAssertPath = data[1]
    deviceName = data[2]
    dataFolderPath = f"{gameAssertPath}/data"
    awsLogFilePath = f"C:/pythonscripts/awsUploaderLog.txt"
    initializeLogging()
    localConfigFilePath = f"{gameAssertPath}/data/configdata.csv"
    print(dataFolderPath)
    try:
        with open(localConfigFilePath)as file:
            csvreader = csv.reader(file)
            for row in csvreader:
                lastrow = row
            userName = f"{lastrow[2]}"
        
            print(userName)
      
        logging.info("Script started")
        awsConfigFilePath = f"{userName}/{deviceName}/configdata.csv"  
        # Load AWS credentials
        accessKey, secretKey = loadAwsKeys(keysFilePath)
        # Check upload status
        downloadConfigFile(bucketName,accessKey,secretKey)
        status = data[0]
        if statusCode[0] == status:
            logging.info("Upload required. Starting upload process...")
            # uploadFolderToS3_(dataFolderPath, bucketName, accessKey, secretKey)
            sync_folder_to_s3(dataFolderPath,bucketName)
            updateUploadStatus(uploadStatusFile, f"{gameAssertPath},{statusCode[1]},{deviceName},{userName}")
            logging.info("Upload process completed and status updated.")
        else:
            logging.info("No upload needed. Exiting script.")
       
    except Exception as e:
        logging.error("An error occurred")
    finally :
          logging.info("lockFile removed")
          remove_lock_file()  # always clean up

if __name__ == "__main__":
    main()
