import json
from pathlib import Path

import pandas as pd


def write_local(data: pd.DataFrame | dict = pd.DataFrame,
                filename: str = '_',
                file_format: str = 'parquet'):
    """
    Write data to a local file.

    Args:
    - data: The data to be written (DataFrame for Parquet, JSON for JSON).
    - file_format: The format in which to save the data ('parquet' or 'json').
    - folder: The folder where the file should be saved.
    - filename: The name of the file without the file extension.

    Returns:
    - Path: The path to the saved file.
    """
    project_dir = Path(__file__).resolve().parents[1]

    if file_format == 'parquet':
        file_path = Path(f"{project_dir}/data/parquet")
        if not file_path.exists():
            file_path.mkdir(parents=True)
        file_path = f'{file_path}/{filename}.parquet'

        data.to_parquet(file_path, compression="gzip")

    elif file_format == 'json':
        file_path = Path(f"{project_dir}/data/json")
        if not file_path.exists():
            file_path.mkdir(parents=True)
        file_path = f'{file_path}/{filename}.json'
        data = data.json()
        with open(file_path, 'w') as json_file:
            json.dump(data, json_file, indent=2)
    return Path(file_path)