from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import List

@dataclass
class Config:
    base_url: str
    access_key: str
    base_currency: str
    symbols: List[str]
    days_past: int
    log_file: str
    output_csv: str
    output_plot: str
    dates: List[datetime] = field(init=False)

    def __post_init__(self):
        now: datetime = datetime.now()
        days_range: range = range(self.days_past, -1, -1)
        self.dates: List[datetime] = [(now - timedelta(days=d)) for d in days_range]