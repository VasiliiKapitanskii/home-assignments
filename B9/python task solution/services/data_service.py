from datetime import datetime
from typing import Dict, List
from services.config_service import Config
from services.request_service import RequestService
import asyncio

class DataService(RequestService):
    def __init__(self, config: Config):
        super().__init__(config)

    def _prepare_url(self, date: datetime) -> str:
        return self.config.base_url.format(date.strftime('%Y-%m-%d'),
                                           self.config.access_key, 
                                           self.config.base_currency, 
                                           ','.join(self.config.symbols))

    def get_rates(self) -> Dict[str, List[float]]:
        rates_data = {symbol: [] for symbol in self.config.symbols}

        for date in self.config.dates:
            data = self.fetch_data(self._prepare_url(date))
            for symbol in self.config.symbols:
                rates_data[symbol].append(data['rates'][symbol])

        return rates_data

    async def get_rates_async(self) -> Dict[str, List[float]]:
        rates_data = {symbol: [None]*len(self.config.dates) for symbol in self.config.symbols}
        futures_to_dates = {self.fetch_data_async(self._prepare_url(date)): date for date in self.config.dates}
        
        async def get_future_result(future):
            result = await future
            return result, futures_to_dates[future]
        
        futures_with_dates = [get_future_result(future) for future in futures_to_dates.keys()]

        for future in asyncio.as_completed(futures_with_dates):
            data, date = await future
            index = self.config.dates.index(date)
            for symbol in self.config.symbols:
                rates_data[symbol][index] = data['rates'][symbol]

        return rates_data
    