import os

from aorta.listener.base import IListener
from aorta.listener.base import Listener

DEBUG = os.getenv('AORTA_IMPORT_DEBUG') is not None
try:
    from aorta.listener.proton_ import ProtonListener
except ImportError:
    if DEBUG:
        raise

