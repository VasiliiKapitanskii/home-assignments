import matplotlib.pyplot as plt
from typing import List, Dict
import logging
from services.config_service import Config

class PlotService:
    def __init__(self, config: Config):
        self.config = config

    def plot_data(self, rates: Dict[str, List[float]], save_to_file: bool = True) -> None:
        base_cur: str = self.config.base_currency
        dates_formatted: List[str] = [date.strftime('%Y-%m-%d') for date in self.config.dates]
        fig, ax1 = plt.subplots(figsize=(10,6))

        if len(self.config.symbols) == 2:
            ax2 = ax1.twinx()
            ax1.plot(self.config.dates, rates[self.config.symbols[0]], 'g-', label=f'{self.config.symbols[0]} to {base_cur}')
            ax2.plot(self.config.dates, rates[self.config.symbols[1]], 'b-', label=f'{self.config.symbols[1]} to {base_cur}')

            ax1.set_xlabel('Date')
            ax1.set_ylabel(f'{self.config.symbols[0]} Exchange Rate', color='g')
            ax2.set_ylabel(f'{self.config.symbols[1]} Exchange Rate', color='b')
            ax1.yaxis.grid(True)
        else:
            ax1.set_xlabel('Date')
            ax1.set_ylabel('Exchange Rate')
            ax1.grid(True)
            for symbol in self.config.symbols:
                ax1.plot(self.config.dates, dates_formatted[symbol], label=f'{symbol} to {base_cur}')

        fig.legend()
        fig.suptitle('Exchange Rate Dynamics')

        if save_to_file:
            plt.savefig(self.config.output_plot)
            logging.info(f'Plot saved to {self.config.output_plot}')

        plt.show()
