import pandas as pd
import scrapy

from . import Pega
from .constants import Currency, zyte_smartproxy_apikey, zyte_smartproxy_domain
from .utils import BaseSpider, convert_currency


class Pipeline:
    def __init__(self):
        self.data = []

    def process_item(self, item, _):
        self.data.append(item)

    def reform_data(self, raw):
        data = {
            ("listing_id", "", ""): [],
            ("pega_id", "", ""): [],
            ("price", "usdt", ""): [],
            ("history", "vis/day", "min"): [],
            ("history", "vis/day", "ave"): [],
            ("history", "vis/day", "max"): [],
            ("roi_days", "", ""): [],
            ("vis2usdt", "", ""): [],
        }
        for row in raw:
            data[("listing_id", "", "")].append(row["listing_id"])
            data[("pega_id", "", "")].append(row["pega_id"])
            data[("price", "usdt", "")].append(row["price_usdt"])
            data[("history", "vis/day", "min")].append(row["min_earnings_day_vis"])
            data[("history", "vis/day", "ave")].append(row["ave_earnings_day_vis"])
            data[("history", "vis/day", "max")].append(row["max_earnings_day_vis"])
            data[("roi_days", "", "")].append(row["roi_days"])
            data[("vis2usdt", "", "")].append(row["vis2usdt"])

        return data

    def close_spider(self, spider):
        pd.set_option("display.max_rows", None)
        df = pd.DataFrame(self.data)
        # df.sort_values(by=["min_earnings_day_vis"], ascending=False, ignore_index=True, inplace=True)
        df.sort_values(by=["roi_days"], ascending=True, ignore_index=True, inplace=True)

        # remove non-experienced horses, including 0 breed horses
        df = df[df["history_count"] >= 100]
        df.reset_index(drop=True, inplace=True)

        data = df.to_dict("records")
        data = self.reform_data(data)
        df = pd.DataFrame(data)

        spider.logger.info(f"\n{df}")


class MarketplaceSpider(BaseSpider):
    name = "marketplace_spider"
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
        "ITEM_PIPELINES": {"pega.marketplace.Pipeline": 300},
    }

    def __init__(self, auction=False, currency: Currency = None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.auction = auction
        self.filter_currency = currency

    def start_requests(self):
        for page in range(0, 69):
            url = f"{self.base_url}/market/pegasListing/{page}?&sortType=ASC&sortBy=price&isAuction={self.auction}&currency={Currency.USDT.value}"
            if self.filter_currency:
                url += f"&currency={self.filter_currency.name}"

            yield scrapy.Request(url, callback=self.parse_marketplace)

    def parse_marketplace(self, response):
        market = response.json()
        for pega in market["market"]:
            listing_id = pega["id"]
            pega_id = pega["pega"]["id"]
            price = int(pega["price"]) / 1000000
            currency = Currency(pega["currency"])

            data = {"listing_id": listing_id, "pega_id": pega_id, "price": price, "currency": currency}

            racing_history_url = f"{self.base_url}/race/history/pega/{pega_id}"
            yield scrapy.Request(racing_history_url, callback=self.parse_racing_history, cb_kwargs={"data": data})

    def parse_racing_history(self, response, data):
        listing_id = data["listing_id"]
        pega_id = data["pega_id"]
        price = data["price"]
        currency = data["currency"]

        racing_history = response.json()["data"]

        pega_obj = Pega(pega_id, price, currency, racing_history)
        min_earnings_day_vis = pega_obj.history_earnings_min_vis_day
        ave_earnings_day_vis = pega_obj.history_earnings_ave_vis_day
        max_earnings_day_vis = pega_obj.history_earnings_max_vis_day
        roi_days = pega_obj.roi_days()

        vis2usdt = convert_currency(1, Currency.VIS, Currency.USDT)
        data = {
            "listing_id": listing_id,
            "pega_id": pega_id,
            f"price_{currency.name.lower()}": price,
            "history_count": pega_obj.history_count,
            "min_earnings_day_vis": min_earnings_day_vis,
            "ave_earnings_day_vis": ave_earnings_day_vis,
            "max_earnings_day_vis": max_earnings_day_vis,
            "roi_days": round(roi_days),
            "vis2usdt": vis2usdt,
        }
        yield data
