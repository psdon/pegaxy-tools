import numpy as np
import pandas as pd

from .constants import Currency
from .utils import convert_currency


class Pega:
    def __init__(
        self,
        pega_id: int,
        price: float,
        price_currency: Currency,
        racing_history: list,
    ):
        self.pega_id = pega_id

        self.price = price  # Bought price or Rent Fee
        self.price_currency = price_currency
        self.racing_history = racing_history
        self.history_count = len(self.racing_history)
        self.max_energy_day = 23

        self._history_earnings()

    def _history_earnings(self):
        # generate start and end range.
        # i.e. 0-22, 23-45
        ranges = []
        for i in range(0, 100, self.max_energy_day):
            ranges.append(i)
        ranges = zip(ranges, ranges[1:])

        # Get history earnings per 23 races
        history_earnings_vis = []
        for start, end in ranges:
            reward = self.get_earnings_by_offset(start, end)
            history_earnings_vis.append(reward)

        # earnings per day
        self.history_earnings_min_vis_day = min(history_earnings_vis)
        self.history_earnings_ave_vis_day = np.array(history_earnings_vis).mean()
        self.history_earnings_max_vis_day = max(history_earnings_vis)

    def roi_days(self, days=23) -> float:
        # Daily Average Earnings
        minimum_earnings_vis = self.history_earnings_min_vis_day
        converted_earnings = convert_currency(minimum_earnings_vis, Currency.VIS, self.price_currency)

        days = 0
        if converted_earnings > 0:
            days = self.price / converted_earnings

        return days

    def get_earnings_by_offset(self, from_offset: int, to_offset: int):
        history = self.racing_history[from_offset:to_offset]

        total_reward = 0
        for race in history:
            total_reward += int(race["reward"])

        return total_reward

    def print_metrics(self):
        """Metrics for buying Pega."""
        data = {
            ("price", self.price_currency.name.lower(), ""): [self.price],
            ("history", "vis/day", "min"): [self.history_earnings_min_vis_day],
            ("history", "vis/day", "ave"): [self.history_earnings_ave_vis_day],
            ("history", "vis/day", "max"): [self.history_earnings_max_vis_day],
            ("roi_days", "", ""): [self.roi_days()],
        }
        df = pd.DataFrame(data)
        print(df)
