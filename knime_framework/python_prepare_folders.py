import knime.scripting.io as knio
import os
import datetime
import typing
from typing import Self
import pytz
from enum import Enum
import pandas as pd
import shutil
import stat


class PathInitializer:
    """
    Utility class to create and manage timestamp-based local directories for data processing workflows.
    Handles timezone-aware timestamping, directory creation, and cleanup for KNIME and general ETL tasks.
    """

    class TimeZone(Enum):
        """
        Common timezone options to support consistent timestamp generation.
        """
        UTC = "UTC"
        US_EASTERN = "US/Eastern"
        US_CENTRAL = "US/Central"
        US_MOUNTAIN = "US/Mountain"
        US_PACIFIC = "US/Pacific"
        EUROPE_LONDON = "Europe/London"
        EUROPE_PARIS = "Europe/Paris"
        ASIA_TOKYO = "Asia/Tokyo"
        ASIA_SHANGHAI = "Asia/Shanghai"
        ASIA_KOLKATA = "Asia/Kolkata"
        AUSTRALIA_SYDNEY = "Australia/Sydney"

    # Default timestamp format: YYYYMMDDHHMMSS
    DEFAULT_TIMESTAMP_FORMAT: typing.Final[str] = "%Y%m%d%H%M%S"
    
    # Folder naming constants
    DATA_FOLDER_NAME: typing.Final[str] = "data"
    DOWNLOAD_FOLDER_NAME: typing.Final[str] = "download"
    OUT_FOLDER_NAME: typing.Final[str] = "output"
    
    # Default timezone
    DEFAULT_TIMEZONE: typing.Final[str] = TimeZone.UTC.value

    def __init__(self, timestamp_format: str = DEFAULT_TIMESTAMP_FORMAT, tz: str = DEFAULT_TIMEZONE):
        """
        Initialize the PathInitializer.

        Args:
            timestamp_format (str): The format used for timestamps in folder names.
            tz (str): The timezone for timestamp generation.
        """
        self.root = os.getcwd()
        self.sys_sp = os.sep
        self.timestamp_format = timestamp_format
        self.set_timezone(tz)  # This also initializes folder paths

    def _init_paths(self) -> None:
        """
        Generate timestamped folder paths based on the current time and timezone.
        """
        now_str = datetime.datetime.now(self.tz).strftime(self.timestamp_format)
        self.temp_folder_name = now_str
        self.local_data_dir = os.path.join(self.root, self.DATA_FOLDER_NAME)
        self.local_temp_dir = os.path.join(self.local_data_dir, now_str)
        self.local_download_dir = os.path.join(self.local_temp_dir, self.DOWNLOAD_FOLDER_NAME)
        self.local_output_dir = os.path.join(self.local_temp_dir, self.OUT_FOLDER_NAME)

    def set_timezone(self, tz: str | None = None) -> Self:
        """
        Set the timezone for timestamp generation and update folder paths.

        Args:
            tz (str | None): Timezone string from pytz. If None, uses default.

        Returns:
            Self: Returns the instance for chaining.
        """
        if tz and tz not in pytz.all_timezones:
            raise ValueError(f"Invalid timezone: {tz}")
        self.tz = pytz.timezone(tz or PathInitializer.DEFAULT_TIMEZONE)
        self._init_paths()  # Refresh folder structure
        return self

    def _handle_remove_readonly(func, path, exc_info):
        """
        Callback for shutil.rmtree to handle read-only files.

        Args:
            func: Function passed by rmtree.
            path (str): File or directory path.
            exc_info: Exception info.
        """
        print(f"Failed to remove: {path}")
        try:
            os.chmod(path, stat.S_IWRITE)
            func(path)
            print(f"Removed after chmod: {path}")
        except Exception as e:
            print(f"Still failed: {path} -> {e}")

    def remove_folder_recursive(path: str) -> None:
        """
        Recursively delete a folder and its contents, handling permission issues.

        Args:
            path (str): Directory to delete.
        """
        if os.path.exists(path):
            shutil.rmtree(path, onerror=PathInitializer._handle_remove_readonly)
            print(f"Removed folder: {path}")
        else:
            print(f"Folder does not exist: {path}")

    def create_folders_from_dict(self, folder_paths: dict[str, str]) -> None:
        """
        Create folders based on a dictionary of paths.

        Args:
            folder_paths (dict[str, str]): Dictionary of folders to create.
        """
        for _, path in folder_paths.items():
            os.makedirs(path, exist_ok=True)

    def initialize(self) -> dict[str, str]:
        """
        Reset and initialize a fresh folder structure with current timestamp.

        Returns:
            dict[str, str]: Dictionary with all relevant folder paths.
        """
        # Clean up the old data directory before creating a new one
        PathInitializer.remove_folder_recursive(self.local_data_dir)

        paths: dict = {
            "local_data_dir": self.local_data_dir,
            "local_temp_dir": self.local_temp_dir,
            "local_download_dir": self.local_download_dir,
            "local_output_dir": self.local_output_dir
        }

        # Create all folders
        self.create_folders_from_dict(paths)

        return paths


def main():
    """
    Entry point for initializing and creating timestamped data folders.
    Outputs the folder paths as a KNIME-compatible DataFrame.
    Handles exceptions gracefully and logs errors.
    """
    try:
        # Instantiate and initialize the path structure
        path_initializer: PathInitializer = PathInitializer()

        # Optional: Set a specific timezone
        # path_initializer.set_timezone(PathInitializer.TimeZone.ASIA_TOKYO.value)

        # Generate folder paths and create directories
        local_paths: dict = path_initializer.initialize()

        # Print folder paths to console (for debugging/logging)
        print("Successfully initialized folders:")
        print(local_paths)

        # Output as KNIME table
        knio.output_tables[0] = knio.Table.from_pandas(pd.DataFrame([local_paths]))

    except Exception as e:
        error_msg = f"Folder initialization failed: {e}"
        print(error_msg)

        # Fallback output for KNIME to show error
        error_df = pd.DataFrame([{"error": error_msg}])
        knio.output_tables[0] = knio.Table.from_pandas(error_df)


main()

