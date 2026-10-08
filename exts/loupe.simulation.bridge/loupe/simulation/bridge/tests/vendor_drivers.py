"""
Register the Beckhoff and B&R drivers from their libraries, for the tests and
the headless harness. The vendor extensions register them in their own
on_startup; the tests here do not load those extensions, so they register the
drivers from the libraries directly.

The libraries come from `pip install` into Kit's Python (see
tools/dev_link.py --driver); a missing one is skipped and reported.
"""

import logging

from .. import registry
from ..registry import Option

logger = logging.getLogger(__name__)

BECKHOFF = dict(
    name="beckhoff",
    options=[Option("AmsNetId", "str", "127.0.0.1.1.1", "PLC AMS Net Id")],
    legacy_namespace="beckhoff_bridge",
    title="Beckhoff (ADS)",
)

BR = dict(
    name="br",
    options=[
        Option("Host", "str", "127.0.0.1", "PLC address"),
        Option("Port", "int", 8000, "OMJSON port"),
    ],
    legacy_namespace="br_bridge",
    title="B&R (OMJSON)",
)


def register_vendor_drivers() -> dict:
    """
    Register whichever of the two drivers can be imported.

    Returns:
        driver name -> True when registered, else the import error text.
    """
    result = {}
    try:
        from beckhoff_bridge import AdsDriver
    except ImportError as e:
        result["beckhoff"] = str(e)
    else:
        registry.register(BECKHOFF["name"], AdsDriver, BECKHOFF["options"],
                          legacy_namespace=BECKHOFF["legacy_namespace"], title=BECKHOFF["title"])
        result["beckhoff"] = True
    try:
        from br_bridge import BrDriver
    except ImportError as e:
        result["br"] = str(e)
    else:
        registry.register(BR["name"], BrDriver, BR["options"],
                          legacy_namespace=BR["legacy_namespace"], title=BR["title"])
        result["br"] = True
    for name, status in result.items():
        if status is not True:
            logger.warning("driver %r not registered: %s", name, status)
    return result


def unregister_vendor_drivers():
    registry.unregister("beckhoff")
    registry.unregister("br")
