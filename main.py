import requests as r
from pega import Pega, marketplace, rent
from pega.constants import (
    Currency,
    base_url,
    zyte_smartproxy_apikey,
    zyte_smartproxy_domain,
)
from scrapy.crawler import CrawlerProcess


def get_single_pega(pega_id, price, currency: Currency):
    proxies = {
        "http": f"http://{zyte_smartproxy_apikey}:@{zyte_smartproxy_domain}",
        "https": f"http://{zyte_smartproxy_apikey}:@{zyte_smartproxy_domain}",
    }
    history = r.get(f"{base_url}/race/history/pega/{pega_id}", proxies=proxies, verify=False).json()
    d = Pega(pega_id, price, currency, history["data"])
    d.print_metrics()


# You can get
# get_single_pega(63747, 1425, Currency.USDT)

process = CrawlerProcess()
# Find best Pega to Rent
# process.crawl(rent.RentingSpider)

# Find best Pega to Buy
# process.crawl(marketplace.MarketplaceSpider)
process.start()
