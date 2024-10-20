import asyncio
import logging
import pandas as pd
from services.config_service import Config, ConfigService
from services.data_service import DataService
from services.plot_service import PlotService
from typing import List, Dict

async def main():
    config: Config = ConfigService.read_config('config.json')

    logging.basicConfig(filename=config.log_file, level=logging.INFO)
    logging.info('Application started')

    data_service: DataService = DataService(config)
    plot_service: PlotService = PlotService(config)

    # Step 1. Read rates (sync or async)
    # rates: Dict[str, List[float]] = data_service.get_rates()
    rates: Dict[str, List[float]] = await data_service.get_rates_async()

    # Step 2. Write CSV for debugging
    pd.DataFrame(rates).to_csv(config.output_csv, index=False)
    logging.info(f'Data written to {config.output_csv}')

    # Step 3. Plot the chart
    plot_service.plot_data(rates)
    
    logging.info('Application finished')

if __name__ == '__main__':
    asyncio.run(main())
