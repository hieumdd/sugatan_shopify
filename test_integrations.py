import os
import subprocess
import time
from datetime import datetime, timedelta

import requests


def test_auto():
    process = subprocess.Popen(
        ["functions-framework", "--target", "main", "--port", "8080"],
        cwd=os.path.dirname(__file__),
        stdout=subprocess.PIPE,
    )
    time.sleep(3)

    try:
        data = {}
        with requests.get("http://localhost:8080", json=data) as r:
            res = r.json()
        results = res["results"]
        assert results["num_processed"] > 0
        assert results["output_rows"] > 0
        assert results["num_processed"] == results["output_rows"]
    except AssertionError:
        pass
    finally:
        process.kill()


def test_manual():
    process = subprocess.Popen(
        ["functions-framework", "--target", "main", "--port", "8081"],
        cwd=os.path.dirname(__file__),
        stdout=subprocess.PIPE,
    )
    time.sleep(3)

    try:
        data = {
            "start_date": (datetime.now() - timedelta(days=5)).strftime(
                "%Y-%m-%dT%H:%M:%S%z"
            ),
            "end_date": (datetime.now() - timedelta(days=3)).strftime(
                "%Y-%m-%dT%H:%M:%S%z"
            ),
        }
        with requests.get("http://localhost:8081", json=data) as r:
            res = r.json()
        results = res["results"]
        assert results["num_processed"] > 0
        assert results["output_rows"] > 0
        assert results["num_processed"] == results["output_rows"]
    except AssertionError:
        pass
    finally:
        process.kill()
