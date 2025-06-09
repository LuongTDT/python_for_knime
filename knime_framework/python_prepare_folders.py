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

    class TimeZone(Enum):
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

    # %Y: Year with century (e.g., 2025)
    # %y: Year without century (e.g., 25)
    # %m: Month as a zero-padded decimal number (01-12)
    # %B: Full month name (e.g., May)
    # %b: Abbreviated month name (e.g., May)
    # %d: Day of the month as a zero-padded decimal number (01-31)
    # %A: Full weekday name (e.g., Thursday)
    # %a: Abbreviated weekday name (e.g., Thu)
    # %H: Hour (24-hour clock) as a zero-padded decimal number (00-23)
    # %I: Hour (12-hour clock) as a zero-padded decimal number (01-12)
    # %M: Minute as a zero-padded decimal number (00-59)
    # %S: Second as a zero-padded decimal number (00-59)
    # %p: AM or PM 
    DEFAULT_TIMESTAMP_FORMAT: typing.Final[str] = "%Y%m%d%H%M%S"
    DATA_FOLDER_NAME : typing.Final[str] = "data"
    DOWNLOAD_FOLDER_NAME: typing.Final[str] = "download"
    OUT_FOLDER_NAME: typing.Final[str] = "output"
    DEFAULT_TIMEZONE: typing.Final[str] = TimeZone.UTC.value
    
    def __init__(self, timestamp_format: str = DEFAULT_TIMESTAMP_FORMAT, tz: str = DEFAULT_TIMEZONE):
        self.root = os.getcwd()
        self.sys_sp = os.sep
        self.timestamp_format = timestamp_format
        self.set_timezone(tz)

    def _init_paths(self) -> None:
        now_str = datetime.datetime.now(self.tz).strftime(self.timestamp_format)
        self.temp_folder_name = now_str
        self.local_data_dir = os.path.join(self.root, self.DATA_FOLDER_NAME)
        self.local_temp_dir = os.path.join(self.local_data_dir, now_str)
        self.local_download_dir = os.path.join(self.local_temp_dir, self.DOWNLOAD_FOLDER_NAME)
        self.local_output_dir = os.path.join(self.local_temp_dir, self.OUT_FOLDER_NAME)

    def set_timezone(self, tz: str | None = None) -> Self:
        if tz and tz not in pytz.all_timezones:
            raise ValueError(f"Invalid timezone: {tz}")
        self.tz = pytz.timezone(tz or PathInitializer.DEFAULT_TIMEZONE)
        self._init_paths()  # Recompute paths if tz changes
        return self

    def _handle_remove_readonly(func, path, exc_info):
        """Callback for shutil.rmtree() to handle read-only or in-use files."""
        print(f"Failed to remove: {path}")
        try:
            os.chmod(path, stat.S_IWRITE)
            func(path)
            print(f"Removed after chmod: {path}")
        except Exception as e:
            print(f"Still failed: {path} -> {e}")

    def remove_folder_recursive(path: str) -> None:
        if os.path.exists(path):
            shutil.rmtree(path, onerror=PathInitializer._handle_remove_readonly)
            print(f"Removed folder: {path}")
        else:
            print(f"Folder does not exist: {path}")


    def create_folders_from_dict(self, folder_paths:dict[str,str]) -> None:

        for _,path in folder_paths.items():
            os.makedirs(path,exist_ok=True)


    def initialize(self) -> dict[str, str]:
        
        # Remove the data folder before creating new one.abs

        PathInitializer.remove_folder_recursive(self.local_data_dir)

        paths: dict = {
            "local_data_dir": self.local_data_dir,
            "local_temp_dir": self.local_temp_dir,
            "local_download_dir": self.local_download_dir,
            "local_output_dir": self.local_output_dir
        }


        self.create_folders_from_dict(paths)

        return paths



    # def _adapt_os_path_separator(self, path: str=None) -> str:
    #     if not path:
    #         return ""
    #     cur_sep: str = "/" if path.__contains__("/") else "\\"
    #     return path.replace(cur_sep, self.sys_sep)
    

path_initializer: PathInitializer = PathInitializer()
# path_initializer.set_timezone(PathInitializer.TimeZone.ASIA_TOKYO.value)
local_paths: dict = path_initializer.initialize()
print (local_paths)

knio.output_tables[0] = knio.Table.from_pandas(pd.DataFrame([local_paths]))

