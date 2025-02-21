#!/usr/bin/env python
"""
Run the documentation harvesting process one time.
"""
from harvester.scheduler import harvest_documentation

if __name__ == "__main__":
    harvest_documentation()