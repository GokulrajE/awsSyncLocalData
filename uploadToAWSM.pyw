
import csv
import logging
import os
import sys
import subprocess
import psutil


# === Configuration ===
BUCKET_NAME = 'homerclouds'
# BUCKET_NAME = 'homer-data'

UPLOAD_STATUS_FILE = "C:/DeviceSetups/Mars/uploadStatus.txt"
APP_INFO_FILE = "C:/AppSetups/Mars/appInfo.txt"
LOG_FILE_PATH = "C:/pythonscripts/awsUploaderLogM.txt"
LOCK_FILE_PATH = "C:/pythonscripts/uploadM.lock"
UPLOAD_NEEDED = "upload_needed"
UPLOAD_DONE = "no_upload"


# === Lock Mechanism ===
def is_another_instance_running():
    if os.path.exists(LOCK_FILE_PATH):
        with open(LOCK_FILE_PATH, 'r') as f:
            try:
                pid = int(f.read().strip())
                if psutil.pid_exists(pid):
                    return True
            except Exception:
                pass
        os.remove(LOCK_FILE_PATH)
    return False

def create_lock_file():
    with open(LOCK_FILE_PATH, 'w') as f:
        f.write(str(os.getpid()))

def remove_lock_file():
    if os.path.exists(LOCK_FILE_PATH):
        os.remove(LOCK_FILE_PATH)


# === Logging Setup ===
def initialize_logging():
    logging.basicConfig(
        filename=LOG_FILE_PATH,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

# === Upload Status Handling ===
def read_upload_status(filepath):
    if os.path.exists(filepath):
        with open(filepath, mode="r") as file:
            parts = file.read().split(",")
            if len(parts) >= 3:
                return parts[0], parts[1], parts[2],parts[3],parts[4]  # game_path, status, device_name,userid,device_location
    logging.error("Upload status file not found or invalid.")
    return None, None, None
def read_app_info(filepath):
    if os.path.exists(filepath):
        with open(filepath, mode="r") as file:
            parts = file.read().split(",")
            if len(parts) >= 2:
                return parts[0], parts[1], parts[2],parts[3]  # game_path, id,device_location,devicename
    logging.error("Upload status file not found or invalid.")
    return None, None, None
def update_upload_status(filepath, game_path, status, device_name, user_name,device_location):
    try:
        with open(filepath, mode="w") as file:
            file.write(f"{game_path},{status},{device_name},{user_name},{device_location}")
    except Exception as e:
        logging.error(f"Failed to update upload status: {e}")


# === AWS Upload Functions ===
def run_command(command):
    logging.info(f"Executing command: {command}")
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    
    if result.stdout:
        print("success...!")
        logging.info("Output:\n" + result.stdout)
    if result.stderr:
        stderr = result.stderr.lower()
        
        logging.warning("Errors:\n" + result.stderr)
        ## for unity purpose
        if "could not connect to the endpoint url" in stderr:
            print("ENDPOINT_ERROR - check Internet connection")  # Could not connect to S3
        if "404" in stderr or "not found" in stderr:
            
            print("NOTFOUND Check ID or Place,[Waiting.. to upate form clinician]")
        elif "ssl" in stderr or "certificate" in stderr:
            print("SSL_ERROR - check Internet connection")  # Unity can handle this as a network issue  
        else:
            print("ERROR")  # Other generic errors
            # upload_config_file()
    else:
        logging.info("Command completed successfully.")

def upload_config_file():
    command = f'aws s3 cp "{LOCAL_CONFIG_PATH}" s3://{BUCKET_NAME}/{PLACE}/{USER_NAME}/{DEVICE_NAME}/configdata.csv'
    run_command(command)

def download_config_file():
    command = f'aws s3 cp s3://{BUCKET_NAME}/{PLACE}/{USER_NAME}/{DEVICE_NAME}/configdata.csv "{LOCAL_CONFIG_PATH}"'
    run_command(command)

def sync_folder_to_s3(local_folder):
    command = f'aws s3 sync "{local_folder}" s3://{BUCKET_NAME}/{PLACE}/{USER_NAME}/{DEVICE_NAME}/ --exact-timestamps --exclude "configdata.csv" --exclude "*.meta"'

    run_command(command)


# === Main Execution ===
def main():
    if is_another_instance_running():
        print("Another instance is already running. Exiting.")
        logging.info("Another instance is already running. Exiting.")
        sys.exit(0)

    create_lock_file()
    initialize_logging()

    try:
        global USER_NAME, DEVICE_NAME, LOCAL_CONFIG_PATH,PLACE,status
        if os.path.exists(APP_INFO_FILE):
            game_path,id,place,device_name= read_app_info(APP_INFO_FILE)
            DEVICE_NAME = device_name
            PLACE = place
            USER_NAME = id
            data_folder = os.path.join(game_path,USER_NAME,"data")
            local_config_path = os.path.join(data_folder, "configdata.csv")
            LOCAL_CONFIG_PATH = local_config_path
        if os.path.exists(UPLOAD_STATUS_FILE) and not os.path.exists(APP_INFO_FILE):
            game_path,status,device_name,USER_NAME,place= read_upload_status(UPLOAD_STATUS_FILE)
            if not game_path or not status:
                sys.exit(1)
            
            DEVICE_NAME = device_name
            PLACE = place
            logging.info("Script started for user: %s, device: %s", USER_NAME, DEVICE_NAME)
            data_folder = os.path.join(game_path,"data")
            local_config_path = os.path.join(data_folder, "configdata.csv")
            LOCAL_CONFIG_PATH = local_config_path
       
       
       
        # Read username from configdata.csv
        
            with open(LOCAL_CONFIG_PATH) as file:
                    reader = csv.reader(file)
                    last_row = list(reader)[-1]
                    USER_NAME = last_row[2]

        if(os.path.exists(APP_INFO_FILE)):
            os.remove(APP_INFO_FILE)
         # Try downloading config file first
        download_config_file()
        if os.path.exists(LOCAL_CONFIG_PATH):
            if UPLOAD_NEEDED == status:
                logging.info("Upload required. Syncing folder to S3...")
                sync_folder_to_s3(data_folder)
                update_upload_status(UPLOAD_STATUS_FILE, game_path, UPLOAD_DONE, DEVICE_NAME, USER_NAME,PLACE)
                logging.info("Upload complete and status updated.")
            else:
                logging.info("No upload needed. Exiting.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        logging.info("Cleaning up lock file.")
        remove_lock_file()


if __name__ == "__main__":
    main()

