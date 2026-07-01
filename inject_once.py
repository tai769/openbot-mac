import logging
import sys

from injector import QianniuInjector


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

sys.exit(0 if QianniuInjector().inject() else 1)
