import numpy as np
import pandas as pd
import scrapy

from . import Pega
from .constants import (
    Currency,
    pgx2usd,
    vis2usd,
    zyte_smartproxy_apikey,
    zyte_smartproxy_domain,
)
from .utils import BaseSpider, convert_currency


class Pipeline:
    def __init__(self):
        self.data = []

    def process_item(self, item, _):
        self.data.append(item)

    def reform_data(self, raw: list):
        data = {
            ("listing_id", "", ""): [],
            ("pega_id", "", ""): [],
            ("price", "pgx", "day"): [],
            ("price", "pgx", "total"): [],
            ("price", "vis", "day"): [],
            ("price", "vis", "total"): [],
            ("energy", "", ""): [],
            ("rent", "", ""): [],
            ("history", "vis/day", "min"): [],
            ("history", "vis/day", "ave"): [],
            ("history", "vis/day", "max"): [],
            ("gross_earnings", "vis", "day"): [],
            ("gross_earnings", "vis", "total"): [],
            ("net_earnings", "vis", "day"): [],
            ("net_earnings", "vis", "total"): [],
            ("adjusted_net_earnings", "vis", "day"): [],
            ("adjusted_net_earnings", "vis", "total"): [],
            ("adjusted_net_earnings", "usd", "day"): [],
            ("adjusted_net_earnings", "usd", "total"): [],
            ("conversion", "vis2usd", ""): [],
            ("conversion", "pgx2usd", ""): [],
        }

        for item in raw:
            data[("listing_id", "", "")].append(item["listing_id"])
            data[("pega_id", "", "")].append(item["pega_id"])
            data[("price", "pgx", "day")].append(item["price_pgx_day"])
            data[("price", "pgx", "total")].append(item["price_pgx_total"])
            data[("price", "vis", "day")].append(item["price_vis_day"])
            data[("price", "vis", "total")].append(item["price_vis_total"])
            data[("energy", "", "")].append(item["energy"])
            data[("rent", "", "")].append(item["rent"])
            data[("history", "vis/day", "min")].append(item["history_earnings_min_vis_day"])
            data[("history", "vis/day", "ave")].append(item["history_earnings_ave_vis_day"])
            data[("history", "vis/day", "max")].append(item["history_earnings_max_vis_day"])
            data[("gross_earnings", "vis", "day")].append(item["gross_earnings_vis_day"])
            data[("gross_earnings", "vis", "total")].append(item["gross_earnings_vis_total"])
            data[("net_earnings", "vis", "day")].append(item["net_earnings_vis_day"])
            data[("net_earnings", "vis", "total")].append(item["net_earnings_vis_total"])
            data[("adjusted_net_earnings", "vis", "day")].append(item["adjusted_net_earnings_vis_day"])
            data[("adjusted_net_earnings", "vis", "total")].append(item["adjusted_net_earnings_vis_total"])
            data[("adjusted_net_earnings", "usd", "day")].append(item["adjusted_net_earnings_usd_day"])
            data[("adjusted_net_earnings", "usd", "total")].append(item["adjusted_net_earnings_usd_total"])
            data[("conversion", "vis2usd", "")].append(vis2usd)
            data[("conversion", "pgx2usd", "")].append(pgx2usd)

        return data

    def close_spider(self, spider):
        pd.set_option("display.max_rows", None)
        df = pd.DataFrame(self.data)

        df = df[df["adjusted_net_earnings_usd_day"] >= 10]
        df.sort_values(by=["adjusted_net_earnings_usd_day"], ascending=False, ignore_index=True, inplace=True)
        # df.drop("ae_usd_day", axis=1, inplace=True)

        df = df[df["history_count"] >= 100]
        df.drop("history_count", axis=1, inplace=True)
        df.drop_duplicates(subset="listing_id", keep="last", inplace=True)

        df.reset_index(drop=True, inplace=True)

        data = df.to_dict("records")
        data = self.reform_data(data)
        df = pd.DataFrame(data)
        df.to_html("./test.html")


class RentingSpider(BaseSpider):
    name = "renting_spider"
    custom_settings = {
        "DOWNLOAD_DELAY": 1 / 40,
        "RETRY_TIMES": 2,
        "CONCURRENT_REQUESTS": 25,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 25,
        "DNSCACHE_ENABLED": False,
        "LOG_LEVEL": "INFO",
        "DUPEFILTER_DEBUG": True,
        "DOWNLOAD_TIMEOUT": 120,
        "DUPEFILTER_CLASS": "scrapy.dupefilters.BaseDupeFilter",
        "DOWNLOADER_MIDDLEWARES": {"scrapy_zyte_smartproxy.ZyteSmartProxyMiddleware": 610},
        "ZYTE_SMARTPROXY_ENABLED": False,
        "ZYTE_SMARTPROXY_APIKEY": zyte_smartproxy_apikey,
        "ZYTE_SMARTPROXY_URL": f"http://{zyte_smartproxy_domain}",
        "ITEM_PIPELINES": {"pega.rent.Pipeline": 300},
    }

    def start_requests(self):
        for page in range(0, 49):
            url = f"{self.base_url}/rent/{page}?sortBy=price&sortType=ASC&rentMode=PAY_RENT_FEE"
            yield scrapy.Request(url, callback=self.parse_marketplace)

    def parse_marketplace(self, response):
        market = response.json()
        for pega in market["renting"]:
            listing_id = pega["id"]
            pega_id = pega["pega"]["id"]
            price = int(pega["price"]) / 1000000000000000000
            energy = pega["pega"]["energy"]
            rent_hours = int(pega["rentDuration"] * 0.000277778)

            data = {
                "listing_id": listing_id,
                "pega_id": pega_id,
                "price": price,
                "price_currency": Currency.PGX,
                "rent_hours": rent_hours,
                "energy": energy,
            }
            racing_history_url = f"{self.base_url}/race/history/pega/{pega_id}"
            yield scrapy.Request(racing_history_url, callback=self.parse_racing_history, cb_kwargs={"data": data})

    def parse_racing_history(self, response, data):
        max_energy_day = 23

        listing_id = data["listing_id"]
        pega_id = data["pega_id"]
        rent_hours = data["rent_hours"]
        rent_days = rent_hours / 24
        energy = data["energy"]
        racing_history = response.json()["data"]
        price_pgx_total = data["price"]
        price_pgx_day = price_pgx_total / rent_days
        price_currency = data["price_currency"]

        pega_obj = Pega(pega_id, price_pgx_total, price_currency, racing_history)
        racing_history_count = pega_obj.history_count

        price_vis_total = convert_currency(price_pgx_total, Currency.PGX, Currency.VIS)
        price_vis_day = price_vis_total / rent_days

        # earnings per day
        history_earnings_min_vis_day = pega_obj.history_earnings_min_vis_day
        history_earnings_ave_vis_day = pega_obj.history_earnings_ave_vis_day
        history_earnings_max_vis_day = pega_obj.history_earnings_max_vis_day

        gross_earnings_vis_total = history_earnings_min_vis_day * rent_days
        gross_earnings_vis_day = gross_earnings_vis_total / rent_days

        net_earnings_vis_total = gross_earnings_vis_total - price_vis_total
        net_earnings_vis_day = net_earnings_vis_total / rent_days

        rent_days_adjusted = (energy + rent_hours - rent_days) / max_energy_day
        adjusted_earning_vis_total = (history_earnings_min_vis_day * rent_days_adjusted) - price_vis_total
        adjusted_earning_vis_day = adjusted_earning_vis_total / rent_days

        data = {
            "listing_id": listing_id,
            "pega_id": pega_id,
            "price_pgx_day": round(price_pgx_day),
            "price_pgx_total": round(price_pgx_total),
            "price_vis_day": round(price_vis_day),
            "price_vis_total": round(price_vis_total),
            "history_count": racing_history_count,
            "energy": energy,
            "rent": round(rent_days, 1),
            "history_earnings_min_vis_day": round(history_earnings_min_vis_day),
            "history_earnings_ave_vis_day": round(history_earnings_ave_vis_day),
            "history_earnings_max_vis_day": round(history_earnings_max_vis_day),
            "gross_earnings_vis_day": round(gross_earnings_vis_day),
            "gross_earnings_vis_total": round(gross_earnings_vis_total),
            "net_earnings_vis_day": round(net_earnings_vis_day),
            "net_earnings_vis_total": round(net_earnings_vis_total),
            "adjusted_net_earnings_vis_day": round(adjusted_earning_vis_day),
            "adjusted_net_earnings_vis_total": round(adjusted_earning_vis_total),
            "adjusted_net_earnings_usd_day": round(
                convert_currency(adjusted_earning_vis_day, Currency.VIS, Currency.USD)
            ),
            "adjusted_net_earnings_usd_total": round(
                convert_currency(adjusted_earning_vis_total, Currency.VIS, Currency.USD)
            ),
        }
        yield data
