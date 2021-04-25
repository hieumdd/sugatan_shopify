import json
import base64
from unittest.mock import Mock
from datetime import datetime, timedelta

from main import main


def test_auto():
    data = {}
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    results = res["results"]
    assert results["num_processed"] > 0
    assert results["output_rows"] > 0
    assert results["num_processed"] == results["output_rows"]


def test_manual():
    data = {
        "start_date": (datetime.now() - timedelta(days=5)).strftime(
            "%Y-%m-%dT%H:%M:%S%z"
        ),
        "end_date": (datetime.now() - timedelta(days=3)).strftime(
            "%Y-%m-%dT%H:%M:%S%z"
        ),
    }
    data = {}
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    results = res["results"]
    assert results["num_processed"] > 0
    assert results["output_rows"] > 0
    assert results["num_processed"] == results["output_rows"]
