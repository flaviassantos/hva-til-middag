import json
import os
import time

import requests
from dotenv import load_dotenv, find_dotenv

from flows.utils import write_local

load_dotenv(find_dotenv())
BEARER_API_TOKEN = os.environ['BEARER_API_TOKEN']


def download_product_data() -> None:
    """
    Fetches product data from an API with rate limiting and save it as JSON files.

    This function makes HTTP requests to an API to retrieve product data, respecting
    a specified rate limit. It saves the JSON data for each product in separate
    JSON files.

    Environment Variables:
    - API_RATE_LIMIT: Maximum requests allowed per minute.
    - PRODUCT_ID: Starting product ID to request from the API.
    - BEARER_API_TOKEN: Bearer token for API authentication.

    Returns:
    None
    """
    max_requests_per_minute = os.environ['API_RATE_LIMIT']
    product_id = int(os.environ['PRODUCT_ID'])
    token = os.environ['BEARER_API_TOKEN']

    while True:
        start_time = time.time()

        try:
            # Make an HTTP request to the API
            url = f"https://kassal.app/api/v1/products/id/{product_id}"
            headers = {"Authorization": f"Bearer {token}"}

            response = requests.get(url, headers=headers)
            print(f"id:{product_id}\n")

            if response.status_code == 200:
                _ = write_local(response,
                                f'product_{product_id}',
                                'json')
            product_id += 1
            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            print(f"Request failed with error: {e}")
        except json.JSONDecodeError as e:
            print(f"JSON decoding error: {e}")

        # Calculate the time taken for the request
        end_time = time.time()
        request_time = end_time - start_time

        # Calculate the time to wait before the next request to respect the
        # rate limit
        control_rate = 60 / int(max_requests_per_minute)
        time_to_wait = max(0, control_rate - request_time)

        time.sleep(time_to_wait)


if __name__ == '__main__':
    download_product_data()
