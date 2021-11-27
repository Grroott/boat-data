import ssl
import logging
from pymongo import MongoClient
import time
import asyncio
import aiohttp
import os

from asgiref import sync


BASE_URL = os.environ.get("BASE_URL")
BATCH_COUNT = int(os.environ.get("BATCH_COUNT"))
DB_URL = os.environ.get("DB_URL")
DATABASE = os.environ.get("DATABASE")
COLLECTION = os.environ.get("COLLECTION")

logging.basicConfig(filename='Handle_Data.log', level=logging.DEBUG,
                    format="%(asctime)s: %(levelname)s: %(levelname)s -- %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")

class Handle_Data():
    def __init__(self) -> None:
        try:
            
            self.client = MongoClient(f"{DB_URL}", ssl_cert_reqs=ssl.CERT_NONE)
            self.db = self.client[f'{DATABASE}']
            self.table = self.db[f'{COLLECTION}']
            logging.info("Connected to MongoDB!!")

        except Exception as error:
            logging.exception(f"[__init__]: {error}")
            exit()

    def get_max_tickeid(self):
        try:
            with open("count.txt", "r") as f:
                data = f.readline()
                logging.info(f"data from count file: {data}")
            self.max_ticketid = int(data)
            logging.info(f"Max ticketid: {self.max_ticketid}")
        except Exception as error:
            logging.exception(f"[get_max_tickeid]: {error}")
            exit()

    def get_data(self, urls):
        try:
            async def get_all(urls):
                async with aiohttp.ClientSession() as session:
                    async def fetch(url):
                        async with session.get(url, ssl=False) as response:
                            data = await response.json()
                            if data['actual_status'] == 'found':
                                del data['complaint_msg_date']
                                del data['NOCAPTCHA_SITEKEY']
                                data['_id'] = int(data.pop('ticket_id'))
                                return data
                    return await asyncio.gather(*[
                        fetch(url) for url in urls
                    ])
            
            return sync.async_to_sync(get_all)(urls)
        except Exception as error:
            logging.exception(f"[get_data]: {error}")

    def mongodb_insert(self):
        try:
            logging.info(len(self.insert_data))
            final_data = [data for data in self.insert_data if data is not None]
            if final_data:
                self.table.insert_many(final_data)
                logging.info("Data inserted!!")
                with open("count.txt", "w") as f:
                    f.write(str(self.max_ticketid + BATCH_COUNT))
                logging.info("Count updated!!")
            else:
                logging.info("No eligible records!!")
        except Exception as error:
            logging.exception(f"[mongodb_insert]: {error}")
    
        

if __name__ == "__main__":
    start_time = time.perf_counter()
    obj = Handle_Data()
    obj.get_max_tickeid()
    urls = [BASE_URL + str(id) for id in range(obj.max_ticketid, obj.max_ticketid + BATCH_COUNT)]
    obj.insert_data = obj.get_data(urls)
    obj.mongodb_insert()
    time_taken = time.perf_counter() - start_time
    logging.info(f"Executed in {time_taken:0.2f} seconds.")

