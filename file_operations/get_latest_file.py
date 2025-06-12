import os
from os import path, listdir
import time
from typing import Optional

def get_latest_file(directory: str) -> Optional[str]:

    """
    Get the most recently modified file in a specific folder.

    Parameters:
    directory (str): The path to the folder where the latest file should be found.

    Returns:
    str or None: The full path of the latest modified file, or None if no files are found.
    """
    print(f"Start getting lastest file in folder {directory}")
    if not path.exists(directory) or not path.isdir(directory):
        print(f"Directory '{directory}' does not exist or is not a valid directory.","warning")
        return None

    files = [path.join(directory, f) for f in listdir(directory) if path.isfile(path.join(directory, f))]

    if not files:
        print("No files found in the directory.","warning")
        return None
    
    latest_file = max(files, key=os.path.getmtime) # Get the file with the latest modification time
    print(f"Latest file: {latest_file}")
    return latest_file

def get_new_file(directory: str, before_files: set, timeout: int=60, check_interval: int=2) -> Optional[str]:
    """
    Waits for a new file to appear in the specified directory after downloading.
    
    :param directory: Directory where the file is expected to be downloaded.
    :param before_files: A snapshot of files before downloading.
    :param timeout: Maximum time (seconds) to wait.
    :param check_interval: Time (seconds) between checks.
    :return: Full path of the newly downloaded file, or None if timeout occurs.
    """
    print(f"Waiting for a new file in folder {directory}...")

    start_time = time.time()
    
    while time.time() - start_time < timeout:
        current_files = set(os.listdir(directory))  # Get updated file list
        new_files = current_files - before_files  # Identify new files

        if new_files:
            latest_file = max(new_files, key=lambda f: os.path.getctime(os.path.join(directory, f)))
            full_path = os.path.join(directory, latest_file)
            print(f"New file detected: {full_path}")
            return full_path

        time.sleep(check_interval)  # ⏳ Wait before checking again

    print("No new file detected within timeout.", "error")
    return None  # Timeout reached