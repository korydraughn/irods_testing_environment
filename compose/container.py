"""Minimal container wrapper for compose project container listings."""

class Container:
    """Container wrapper exposing only attributes used by this project."""
    def __init__(self, name):
        self.name = name
