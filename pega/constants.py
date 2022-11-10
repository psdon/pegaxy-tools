from enum import Enum

from pycoingecko import CoinGeckoAPI


class Currency(Enum):
    USDT = "0xc2132D05D31c914a87C6611C10748AEb04B58e8F"
    PGX = "0xc1c93D475dc82Fe72DBC7074d55f5a734F8cEEAE"
    VIS = "VIS"
    USD = "USD"


base_url = "https://api-apollo.pegaxy.io/v1/game-api"
zyte_smartproxy_apikey = ""
zyte_smartproxy_domain = ""

vis2usd = CoinGeckoAPI().get_price("vigorus", "usd")["vigorus"]["usd"]
pgx2usd = CoinGeckoAPI().get_price("pegaxy-stone", "usd")["pegaxy-stone"]["usd"]
usdt2usd = CoinGeckoAPI().get_price("tether", "usd")["tether"]["usd"]

usd2vis = 1 / vis2usd
usd2pgx = 1 / pgx2usd
usd2usdt = 1 / usdt2usd
