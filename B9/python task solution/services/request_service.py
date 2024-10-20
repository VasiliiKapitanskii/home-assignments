import asyncio
import requests
import logging
import aiohttp
from time import sleep
from typing import Dict, Any
from services.config_service import Config

class RequestService:
    def __init__(self, config: Config, retry_attempts: int = 3, wait_between_retries: int = 1):
        self.config = config
        self.retry_attempts = retry_attempts
        self.wait_between_retries = wait_between_retries

    def fetch_data(self, url: str) -> Dict[str, Any]:
        for attempt in range(self.retry_attempts):
            logging.info(f'Requesting with attempt {attempt}: {url}')
            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()

                if not data['success']:
                    raise Exception('Request failed')
                
                return data
            except Exception as e:
                logging.error(f'Error occurred during HTTP request: {e}')
                if attempt < self.retry_attempts - 1:
                    sleep(self.wait_between_retries)

        raise Exception('Maximum retry attempts reached')

    async def fetch_data_async(self, url: str) -> Dict[str, Any]:
        for attempt in range(self.retry_attempts):
            logging.info(f'Requesting with attempt {attempt}: {url}')
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as resp:
                        resp.raise_for_status()
                        data = await resp.json()

                        if not data['success']:
                            raise Exception('Request failed')
                        
                        return data
            except Exception as e:
                logging.error(f'Error occurred during HTTP request: {e}')
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(self.wait_between_retries)

        raise Exception('Maximum retry attempts reached')