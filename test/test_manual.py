from datetime import datetime

from .utils import process

CLIENT_NAME = "SBLA"
SHOP_URL = "spencer-barnes.myshopify.com"

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
