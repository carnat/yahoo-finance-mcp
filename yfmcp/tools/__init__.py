"""Domain tool handler modules.

Each sub-module registers its handlers on the shared ``yfinance_server``
instance from ``yfmcp.app`` at import time.  ``server.py`` imports every
domain module (so the registrations happen) and re-exports the handler
functions by name so that ``globals()``-based grouped routing keeps working.
"""
