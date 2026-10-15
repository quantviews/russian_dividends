"""Immutable dividend evidence packages. No network I/O on reads."""

from .reader import read_as_of

__all__ = ["read_as_of"]
