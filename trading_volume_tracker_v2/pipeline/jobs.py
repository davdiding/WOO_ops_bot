import pandas as pd
from datetime import datetime as dt
from lib.utils import send_message
from lib.utils import CMCGrabber, Tools


class ListingJob:
    def __init__(self, config):
        self.config = config
        self.name = "ListingJob"
        
    def run(self):
        bot_key = self.config["BOT_KEY"]
        chat_id = self.config["DAVID_CHAT_ID"]
        exchange_name = self.config["EXCHANGE_NAME"]
        exchange_slug = self.config["EXCHANGE_SLUG"]
        
        send_message(token=bot_key, message=f"{dt.today().date()} START LISTING DB RENEW", chat_id=chat_id)
        
        tool = Tools()
        
        listing_db = tool.get_listing_db(exchange_name=exchange_name, exchange_slug=exchange_slug)
        woo_listing = tool.get_woo_listing()
        listing_db = pd.concat([listing_db, woo_listing], axis=0)
        tool.write_db(source=listing_db, path=tool.listing_db_path, index=False)
        
        send_message(token=bot_key, message=f"{dt.today().date()} FINISH LISTING DB RENEW", chat_id=chat_id)
    
    
class VolumeJob:
    def __init__(self, config):
        self.config = config
        self.name = "VolumeJob"
    
    def run(self, num):
        bot_key = self.config["BOT_KEY"]
        chat_id = self.config["DAVID_CHAT_ID"]
        send_message(token=bot_key, message=f"{dt.today().date()} START VOLUME DB RENEW", chat_id=chat_id)
        
        grabber = CMCGrabber()
        tools = Tools()
        token_info = grabber.get_token_info(num=num)
        new_db = pd.concat([tools.volume_db, token_info], axis=0)
        tools.to_db(name="volume", data=new_db, index=True)
        tools.to_online_db(name="volume", data=new_db, index=True)
        
        send_message(token=bot_key, message=f"{dt.today().date()} FINISH VOLUME DB RENEW", chat_id=chat_id)


class CleaningJob:
    
    def __init__(self, config):
        self.config = config
        self.name = "CleaningJob"
        
    def run(self):
        pass


class ReportJob:
    def __init__(self, config):
        self.config = config
        self.name = "ReportJob"
        
    def run(self):
        pass
    