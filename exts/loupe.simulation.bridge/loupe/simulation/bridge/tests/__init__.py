# omni.kit.test is only present when the test runner enables it; the harness
# imports tests.vendor_drivers from an ordinary app, where it must not fail.
try:
    from .test_bridge import *  # noqa: F401,F403
except ImportError:
    pass
