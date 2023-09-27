import os

import pandas as pd
import pytest
from flows.utils import write_local


def test_write_local():
    result = write_local(pd.DataFrame(), "test_file")
    assert result.exists()
    assert result.name == "test_file.parquet"

    # Clean up: Remove the temporary JSON file
    os.remove(result)


if __name__ == "__main__":
    pytest.main()

