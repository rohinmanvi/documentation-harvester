#!/usr/bin/env python
"""
Run the documentation harvester in scheduled (periodic) mode.
"""
from harvester.scheduler import start_scheduler

if __name__ == "__main__":
    start_scheduler()