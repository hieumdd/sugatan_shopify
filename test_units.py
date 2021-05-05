import json
import base64
from unittest.mock import Mock
from datetime import datetime, timedelta

from main import main
from assertions import assertion

mock_context = Mock()
mock_context.event_id = "617187464135194"
mock_context.timestamp = "2019-07-15T22:09:03.761Z"

def test_auto():
    """Test the scripts for default/auto mode"""

    message = {}
    message_json = json.dumps(message)
    event = {"data": base64.b64encode(message_json.encode("utf-8"))}
    res = main(event, mock_context)
    assertion(res)


def test_manual():
    """Test the scripts for manual mode"""    
    
    message = {
        "start_date": (datetime.now() - timedelta(days=5)).strftime(
            "%Y-%m-%dT%H:%M:%S%z"
        ),
        "end_date": (datetime.now() - timedelta(days=3)).strftime(
            "%Y-%m-%dT%H:%M:%S%z"
        ),
    }
    message_json = json.dumps(message)
    event = {"data": base64.b64encode(message_json.encode("utf-8"))}
    res = main(event, mock_context)
    assertion(res)
