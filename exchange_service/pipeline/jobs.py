from ..lib.binance import Binance


class BinanceJob(object):
    NAME = "binance"

    def __init__(self):
        self.binance = Binance().create()
