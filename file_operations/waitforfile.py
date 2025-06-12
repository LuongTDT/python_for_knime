import os
import time
from typing import Optional 
import shutil

def wait_for_file_exists(file_path: str, timeout: int=60) -> bool:
    """
    Waits for a specific file to be created within the timeout period.

    :param file_path: The full path of the file to wait for.
    :param timeout: Maximum time (seconds) to wait.
    :param check_interval: Time (seconds) between checks.
    :return: True if the file exists, False if timeout occurs.
    """
    start_time = time.time()
    CHECK_INTERVAL = 1  # Set to 1 second for more frequent checks

    while time.time() - start_time < timeout:
        if os.path.exists(file_path):  # File detected
            return True
        
        time.sleep(CHECK_INTERVAL)  # Wait before checking again

    return False  #Timeout reached




def wait_for_download(folder_path: str, 
                      before_files: Optional[list], 
                      file_name: Optional[str], 
                      timeout: int=60, 
                      check_interval: int = 1) -> Optional[str]:
    """
    Waits for a file to be completely downloaded in a given folder.

    Parameters:
    - folder_path (str): The directory where the file is expected to be downloaded.
    - file_name (str, optional): The name of the file to wait for. If None, waits for any new file.
    - timeout (int): Maximum time (in seconds) to wait before giving up. Default is 60 seconds.
    - check_interval (int): Time interval (in seconds) between each check. Default is 1 second.

    Returns:
    - str or None: Full file path if the file is successfully downloaded, None if timeout occurs.
    """
    before_files = set(before_files) if before_files else set(os.listdir(folder_path))
    start_time = time.time()
    print("Start waiting for a file to be completely downloaded.")

    while time.time() - start_time < timeout:
        current_files = set(os.listdir(folder_path))
        new_files: set[str] = current_files - before_files

        # Skip temporary/incomplete downloads
        valid_files = [f for f in new_files if not f.endswith(('.crdownload', '.part', '.tmp'))]
        if not valid_files:
            print("No valid files found in the directory.") # remove this line if not needed avoid printing in production (large line of logs)
            time.sleep(check_interval)
            continue

        if file_name:
            file_path = os.path.join(folder_path, file_name)
            if file_name in valid_files:
                try:
                    with open(file_path, "rb"):
                        return file_path
                except PermissionError:
                    pass
        else:
            for f in valid_files:
                file_path = os.path.join(folder_path, f)
                try:
                    with open(file_path, "rb"):
                        return file_path
                except PermissionError:
                    pass

        time.sleep(check_interval)

    print(f"Timeout: No fully downloaded file found within {timeout} seconds.")
    return None


def wait_for_file_validation(file_path: str, timeout: int=60, check_interval: int=1) -> bool:
    """
    Waits for a file to be completely written and valid.

    :param file_path: The full path of the file to validate.
    :param timeout: Maximum time (seconds) to wait.
    :param check_interval: Time (seconds) between checks.
    :return: True if the file is valid, False if timeout occurs.
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        if os.path.exists(file_path):
            try:
                with open(file_path, "rb"):
                    return True  # File is valid
            except Exception as e:
                print(f"File validation failed: {e}")
        
        time.sleep(check_interval)

    return False  # Timeout reached


downloaded_file_path = r"C:\Users\LuongTran\Downloads\5a77bc9920e894b6cdf9.jpg"
final_fodler = r"C:\Users\LuongTran\Downloads\test"
shutil.move(downloaded_file_path,final_fodler)

