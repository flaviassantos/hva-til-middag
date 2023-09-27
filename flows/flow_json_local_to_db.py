import hashlib
import json
import os
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine, MetaData

load_dotenv(find_dotenv())

# Define the data file directory and processed file path
project_dir = Path(__file__).resolve().parents[1]
data_filepath = Path(f"{project_dir}/data/json")
processed_filepath = Path(f"{project_dir}/data/processed")

nested_columns = ['price_history', 'category', 'allergens', 'labels', 'nutrition']


def remove_prefix(column_name: str) -> str:
    return column_name.replace('data.', '')


def extract_json_product_data(file_path: str, record_path=None, meta=None) -> pd.DataFrame:
    """
    Load JSON data from a file and flatten it into a DataFrame.

    Args:
    - file_path (str): The path to the JSON file.
    - record_path (list, optional): List of column names to record as separate columns.
    - meta (list, optional): List of meta columns.

    Returns:
    - pd.DataFrame: The flattened DataFrame.
    """
    with open(file_path, 'r') as f:
        data = json.loads(f.read())

    df = pd.json_normalize(data, record_path=record_path, meta=meta)
    df = df.rename(columns=remove_prefix)
    return df


# def denormalize_data(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
#     """
#     Flattens a specific column from a flattened DataFrame.
#     """
#     if column_name in df.columns:
#         try:
#             df_flat = pd.json_normalize(df[column_name][0])
#             df_flat['product_id'] = df['id'][0]
#             return df_flat
#         except (KeyError, IndexError, AttributeError) as e:
#             # Handle specific exceptions that may occur during normalization
#             print(f"Error while denormalizing {column_name}: {e}")
#             return df
#     else:
#         return pd.DataFrame()
def denormalize(row: pd.Series, name: str, product_id: str) -> pd.DataFrame:
    """
    Flattens a specific column from a flattened DataFrame.
    """
    try:
        df_flat = pd.json_normalize(row[name])
        df_flat['product_id'] = product_id
        return df_flat
    except (KeyError, IndexError, AttributeError, NotImplementedError) as e:
        # Handle specific exceptions that may occur during normalization
        if row.isna().any():
            e = 'NaN value'
        print(f"Error while denormalizing product_id {product_id} in table {name}: {e}")
    return pd.DataFrame()


def load_into_db(db_url: str = None, table_name: str = None,
                 df: pd.DataFrame = None) -> None:
    try:
        engine = create_engine(db_url)
        connection = engine.connect()

        metadata = MetaData()
        metadata.reflect(bind=engine)

        # if not metadata.tables.get(table_name):
        #     df.head(n=0).to_sql(name=table_name, con=engine, index=False)

        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

        connection.close()
    except (AttributeError) as e:
        # Handle specific exceptions that may occur during normalization
        print(f"Error while loading to db: {table_name}: {e}")
        return df

# def get_primary_key(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
#     if not df.empty:
#         df['pk'] = ''
#         try:
#             if table_name == 'price_history':
#                 df['pk'] = df.apply(
#                     lambda row: generate_hash(row['date']),
#                     axis=1)
#             if table_name == 'allergens':
#                 df['pk'] = df.apply(
#                     lambda row: generate_hash(row['code']),
#                     axis=1)
#             if table_name == 'labels':
#                 df['pk'] = df.apply(
#                     lambda row: generate_hash(row['name'], row['organization']),
#                     axis=1)
#             if table_name == 'nutrition':
#                 df['pk'] = df.apply(
#                     lambda row: generate_hash(row['code']),
#                     axis=1)
#             else:
#                 df['pk'] = df.apply(
#                     lambda row: generate_hash(row['id']),
#                     axis=1)
#             df = df.set_index('pk').reset_index(drop=True)
#             return df
#         except (KeyError, IndexError, AttributeError) as e:
#             # Handle specific exceptions that may occur during normalization
#             print(f"Error while getting primary key: {table_name}: {e}")
#             return df
#     else:
#         return df


# def generate_hash(*args) -> str:
#     """ Generates a hash for every row for different combinations of columns as primary keys in different DataFrames"""
#     data_string = '_'.join(map(str, args))
#     hash_object = hashlib.sha256()
#     hash_object.update(data_string.encode())
#     return hash_object.hexdigest()


def clean(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    try:
        if table_name == 'price_history':
            # df['date'] = pd.to_datetime(df['date'])
            # df = df.sort_values(by='date', ascending=True)
            df = df.drop_duplicates(keep='last', subset=['price', 'product_id'])
        return df
    except (AttributeError) as e:
        # Handle specific exceptions that may occur during normalization
        print(f"Error while cleaning the table '{table_name}': {e}")
        return df


def main_flow(db_url: str) -> None:
    for filename in os.listdir(processed_filepath):
        if filename.endswith(".json"):
            file_path = f"{processed_filepath}/{filename}"
            df_product = extract_json_product_data(file_path)

            # Save main table
            df_product_cleaned = df_product.drop(nested_columns, axis=1)
            #df_product_cleaned = clean(df_product_cleaned, 'product')
            load_into_db(db_url=db_url, table_name='product', df=df_product_cleaned)

            # Save dimension tables
            for name in nested_columns:
                appended_dfs = []
                for index, row in df_product.iterrows():
                    product_id = row['id']
                    df_flat = denormalize(row, name, product_id)
                    df_flat = clean(df_flat, name)
                    df_flat = df_flat.dropna(axis=1, how='all')
                    appended_dfs.append(df_flat)

                appended_df = pd.concat(appended_dfs, ignore_index=True)
                load_into_db(db_url=db_url, table_name=f"{name}", df=appended_df)


if __name__ == '__main__':

    # Environment variables for PostgreSQL 
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db = os.environ["POSTGRES_DB"]
    host = os.environ["HOST"]
    port = os.environ["PORT"]
    postgresql_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'

    main_flow(db_url=postgresql_url)
