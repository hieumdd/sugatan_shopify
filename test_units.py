import json
import base64
from unittest.mock import Mock
from datetime import datetime, timedelta

from main import main


def get_mock_context():
    mock_context = Mock()
    mock_context.event_id = "617187464135194"
    mock_context.timestamp = "2019-07-15T22:09:03.761Z"
    return mock_context


def test_auto():
    mock_context = get_mock_context()
    message = {}
    data = {"data": base64.b64encode(json.dumps(message).encode())}
    res = main(data, mock_context)
    results = res["results"]
    assert results["num_processed"] > 0
    assert results["output_rows"] > 0
    assert results["num_processed"] == results["output_rows"]


def test_manual():
    mock_context = get_mock_context()
    message = {
        "start_date": (datetime.now() - timedelta(days=5)).strftime(
            "%Y-%m-%dT%H:%M:%S%z"
        ),
        "end_date": (datetime.now() - timedelta(days=3)).strftime(
            "%Y-%m-%dT%H:%M:%S%z"
        ),
    }
    data = {"data": base64.b64encode(json.dumps(message).encode())}
    res = main(data, mock_context)
    results = res["results"]
    assert results["num_processed"] > 0
    assert results["output_rows"] > 0
    assert results["num_processed"] == results["output_rows"]
