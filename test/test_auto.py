from .utils import process

CLIENT_NAME = "MotivArt"
SHOP_URL = "willyoudo.myshopify.com"

def test_auto():
    """Test the scripts for auto mode"""    
    
    data = {
        "client_name": CLIENT_NAME,
    }
    process(data)
