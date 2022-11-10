import requests as r
import scrapy

from .constants import (
    Currency,
    base_url,
    pgx2usd,
    usd2usdt,
    usd2vis,
    usdt2usd,
    vis2usd,
    zyte_smartproxy_apikey,
    zyte_smartproxy_domain,
)


class BaseSpider(scrapy.Spider):
    name = "BaseSpider"
    custom_settings = {
        "DOWNLOAD_DELAY": 1 / 40,
        "RETRY_TIMES": 2,
        "CONCURRENT_REQUESTS": 20,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 20,
        "RETRY_HTTP_CODES": [],
        "DNSCACHE_ENABLED": False,
        "LOG_LEVEL": "DEBUG",
        "DUPEFILTER_DEBUG": True,
        "DOWNLOAD_TIMEOUT": 120,
        "DUPEFILTER_CLASS": "scrapy.dupefilters.BaseDupeFilter",
        "DOWNLOADER_MIDDLEWARES": {"scrapy_zyte_smartproxy.ZyteSmartProxyMiddleware": 610},
        "ZYTE_SMARTPROXY_ENABLED": True,
        "ZYTE_SMARTPROXY_APIKEY": zyte_smartproxy_apikey,
        "ZYTE_SMARTPROXY_URL": f"http://{zyte_smartproxy_domain}",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base_url = base_url


def convert_currency(value: float, from_currency: Currency, to_currency: Currency) -> float:
    if from_currency == Currency.VIS and to_currency == Currency.USD:
        return value * vis2usd
    if from_currency == Currency.VIS and to_currency == Currency.USDT:
        value_usd = convert_currency(value, Currency.VIS, Currency.USD)
        return value_usd * usd2usdt
    elif from_currency == Currency.PGX and to_currency == Currency.USD:
        return value * pgx2usd
    elif from_currency == Currency.USDT and to_currency == Currency.USD:
        return value * usdt2usd
    elif from_currency == Currency.PGX and to_currency == Currency.VIS:
        value_usd = value * pgx2usd
        return value_usd * usd2vis

    raise ValueError(f"from {from_currency} to {to_currency} is not supported")
