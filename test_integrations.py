import os
import subprocess
import time
import base64
import json
import re
import ast
from datetime import datetime, timedelta

import requests

from assertions import assertion

RESULTS_REGEX = "{\\'pipelines.*}"


def process_output(out):
    out = out.decode("utf-8")
    res = re.search(RESULTS_REGEX, out).group(0)
    res = ast.literal_eval(res)
    return res


def test_auto():
    """Test the scripts for default/auto mode"""

    port = 8080
    process = subprocess.Popen(
        [
            "functions-framework",
            "--target=main",
            "--signature-type=event",
            f"--port={port}",
        ],
        cwd=os.path.dirname(__file__),
        stdout=subprocess.PIPE,
    )

    message = {}
    message_json = json.dumps(message)
    message_encoded = base64.b64encode(message_json.encode("utf-8")).decode("utf-8")
    event = {"data": {"data": message_encoded}}
    with requests.post(f"http://localhost:{port}", json=event) as r:
        assert r.status_code == 200
    process.kill()
    process.wait()
    out, err = process.communicate()
    res = process_output(out)
    assertion(res)


def test_manual():
    """Test the scripts for manual mode""" 

    port = 8081
    process = subprocess.Popen(
        [
            "functions-framework",
            "--target=main",
            "--signature-type=event",
            f"--port={port}",
        ],
        cwd=os.path.dirname(__file__),
        stdout=subprocess.PIPE,
    )

    message = {
        "start_date": (datetime.now() - timedelta(days=5)).strftime(
            "%Y-%m-%dT%H:%M:%S%z"
        ),
        "end_date": (datetime.now() - timedelta(days=3)).strftime(
            "%Y-%m-%dT%H:%M:%S%z"
        ),
    }
    message_json = json.dumps(message)
    message_encoded = base64.b64encode(message_json.encode("utf-8")).decode("utf-8")
    event = {"data": {"data": message_encoded}}
    with requests.post(f"http://localhost:{port}", json=event) as r:
        assert r.status_code == 200
    process.kill()
    process.wait()
    out, err = process.communicate()
    res = process_output(out)
    assertion(res)
