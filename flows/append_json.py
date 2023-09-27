import os
import json
from pathlib import Path

# Define the data file directory and processed file path
project_dir = Path(__file__).resolve().parents[1]
data_filepath = Path(f"{project_dir}/data/json")
processed_filepath = Path(f"{project_dir}/data/processed")
os.makedirs(processed_filepath, exist_ok=True)
final_file_path = os.path.join(processed_filepath, 'products.json')


def append_json_to_final_file(json_file: str, final_data: list):
    """Append JSON data from a file to the final JSON file."""
    with open(json_file, 'r') as f:
        data = json.load(f)
        final_data.append(data)


def main():
    final_data = []

    # Sort JSON files by modified date
    json_files = [os.path.join(data_filepath, filename) for filename in os.listdir(data_filepath) if filename.endswith(".json")]
    json_files.sort(key=lambda x: os.path.getmtime(x))

    # Iterate over JSON files and append them to the final file
    for json_file in json_files:
        append_json_to_final_file(json_file, final_data)
    with open(final_file_path, 'w') as f:
        json.dump(final_data, f, indent=2)


if __name__ == '__main__':
    main()
