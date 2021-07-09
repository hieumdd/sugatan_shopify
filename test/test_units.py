from datetime import datetime

from .utils import process

CLIENT_NAME = "AverrAglow"
SHOP_URL = "hello-aglow.myshopify.com"

def test_manual():
    """Test the scripts for manual mode"""    
    
    data = {
        "client_name": CLIENT_NAME,
        "start": (datetime(2021, 7, 1, 0, 0, 0)).strftime(
            "%Y-%m-%dT%H:%M:%S%z"
        ),
        "end": (datetime(2021, 7, 2, 0, 0, 0)).strftime(
            "%Y-%m-%dT%H:%M:%S%z"
        ),
    }
    process(data)


def test_auto():
    """Test the scripts for auto mode"""    
    
    data = {
        "client_name": CLIENT_NAME,
    }
    process(data)
