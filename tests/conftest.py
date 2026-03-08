"""
conftest.py — shared pytest fixtures and path setup.
"""
import sys
import os

# Make the root of the project importable in tests
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
