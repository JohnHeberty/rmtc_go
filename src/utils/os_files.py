from src.utils.constants import Archive
from glob import glob
import pandas as pd
import json
import os

class folder:

    @staticmethod
    def get_files(folder_path: str) -> list[str]:
        """
        Get a list of files from the specified folder.

        Args:
            folder_path (str): The path to the folder.

        Returns:
            list[str]: A list of file naMONTH in the folder.
        """
        try:
            files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
            return files
        except Exception as e:
            raise ValueError(f"Failed to get files from folder {folder_path}: {e}")

class read:

    @staticmethod
    def read_json(file_path: str) -> dict:
        """
        Load data from a JSON file.

        Args:
            file_path (str): The path to the JSON file.

        Returns:
            dict: The loaded data.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        with open(file_path, "r") as f:
            data = json.load(f)
        return data

    @staticmethod
    def read_csv(file_path: str) -> pd.DataFrame:
        """
        Load data from a CSV file.

        Args:
            file_path (str): The path to the CSV file.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        return pd.read_csv(file_path, index_col=Archive.index, encoding=Archive.encoding, sep=Archive.sep, decimal=Archive.decimal)

class write:

    @staticmethod
    def write_json(data: dict, file_path: str) -> None:
        """
        Save data to a JSON file.

        Args:
            data (dict): The data to be saved.
            file_path (str): The path to the JSON file.
        """
        with open(file_path, "w") as f:
            json.dump(data, f)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Failed to save data to {file_path}. File does not exist.")

    @staticmethod
    def write_csv(data: pd.DataFrame, file_path: str) -> None:
        """
        Save data to a CSV file.

        Args:
            data (pd.DataFrame): The DataFrame to be saved.
            file_path (str): The path to the CSV file.
        """
        data.to_csv(file_path, index=Archive.index, encoding=Archive.encoding, sep=Archive.sep, decimal=Archive.decimal)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Failed to save data to {file_path}. File does not exist.")
