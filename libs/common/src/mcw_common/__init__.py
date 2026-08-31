"""Shared kernel for the mini crypto wallet platform.

Scope is deliberately narrow: *cross-cutting infrastructure only* (money,
logging, correlation, event envelope, bus, outbox, HTTP plumbing). No domain
model is shared between services -- wallet and blockchain each own their
schema, so this package cannot become a hidden coupling point.
"""

__version__ = "1.0.0"
