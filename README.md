# Omni-Utils
Common tools for Loupe Omniverse extensions

## plc_bridge

[`plc_bridge/`](plc_bridge/README.md) is a plain-Python package with no Omniverse dependency: the
driver contract every vendor bridge implements (`PlcDriver`) and the polling runtime that drives it
(`PlcRuntime`). The Kit modules at the root of this repo are unchanged and do not use it yet;
`Runtime_Base` stays for extensions that still derive from it.
