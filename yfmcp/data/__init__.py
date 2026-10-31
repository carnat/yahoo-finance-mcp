"""Shared JSON registries read by both runtimes (the Worker imports them from here; Python reads them via DATA_DIR)."""

from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent
