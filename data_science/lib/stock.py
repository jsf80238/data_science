from datetime import date, datetime
from random import gauss

BASELINE_DATE = date(2024, 5, 8)

DEFAULT_VOLUME = 100
# As of 2024-05-08 the Vanguard Total Stock Market Index Fund
# has returned 510.29% since its inception on 2000-11-13 (8577 days).
# Using the formula:
# A = P(1 + R/100)^t
# That's a compounded daily rate of %0.000727.
AVERAGE_DAILY_RETURN = 0.0727 / 100

# Closing prices as of 2024-05-08, used as a baseline when generating random prices
TICKER_PRICE_DICT = {
    "INOD": 10.48,
    "CRCT": 8.52,
    "SNRC": 9.2,
    "WAVD": 3.5,
    "NNE": 5.1,
    "PG": 164.91,
    "HSBC": 45.77,
    "MS": 95.52,
    "GS": 446.95,
    "RTX": 103.9,
    "TDOC": 12.21,
    "KVYO": 23.4,
    "MGA": 47.74,
    "SONY": 78.39,
    "TMCI": 4.09,
    "DV": 18.74,
    "INSP": 165,
    "ETAO": 1.22,
    "TRIP": 18.15,
}


def get_volume() -> int:
    """
    Use a Gaussian distribution centered on the average volume with a standard deviation of 10% of the average volume.
    :return: a volume
    """
    return round(gauss(DEFAULT_VOLUME, DEFAULT_VOLUME/10))


def get_price(ticker: str, on: date) -> float:
    """
    Get a random price but with an inclination towards increase based on the number of days
    :param ticker: the security to be priced
    :param on: generate a price for this date
    :return: semi-random price for the given date
    """
    if ticker.upper() not in TICKER_PRICE_DICT:
        raise ValueError("No such ticker.")
    baseline_price = TICKER_PRICE_DICT[ticker.upper()]
    randomized_price = gauss(baseline_price, baseline_price/50)
    elapsed_days = (on - BASELINE_DATE).days
    return round(randomized_price * (1+AVERAGE_DAILY_RETURN)**elapsed_days, 2)


print(get_price("GS", date(2024, 11, 11)))
