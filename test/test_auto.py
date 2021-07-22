from datetime import datetime

from .utils import process

CLIENT_NAME = "SBLA"
SHOP_URL = "spencer-barnes.myshopify.com"

def test_auto():
    """Test the scripts for auto mode"""    
    
    data = {
        "client_name": CLIENT_NAME,
    }
    process(data)
