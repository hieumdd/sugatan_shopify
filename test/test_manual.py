from datetime import datetime

from .utils import process

CLIENT_NAME = "SniffAndBark"
SHOP_URL = "sniffandbark.myshopify.com"
START = datetime(2021, 10, 1).strftime("%Y-%m-%dT%H:%M:%S%z")
END = datetime(2021, 11, 1).strftime("%Y-%m-%dT%H:%M:%S%z")

def test_manual():
    """Test the scripts for manual mode"""    
    
    data = {
        "client_name": CLIENT_NAME,
        "start": START,
        "end": END,
    }
    process(data)
