#!/usr/bin/env python3
"""IBKR TWS client wrapper – EWrapper + EClient."""
import time
from datetime import datetime

from ibapi.client import EClient
from ibapi.wrapper import EWrapper


def _default_log(msg):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}")


class TWSApp(EWrapper, EClient):
    def __init__(self, logger=None):
        EWrapper.__init__(self)
        EClient.__init__(self, self)
        self.log = logger if callable(logger) else _default_log
