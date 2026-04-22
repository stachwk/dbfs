#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

from mod.repository import (
    NamespaceRepository,
    NamespaceRepositoryCreateMutations,
    NamespaceRepositoryDeleteMutations,
    NamespaceRepositoryLookup,
    NamespaceRepositoryMutations,
)

__all__ = [
    "NamespaceRepository",
    "NamespaceRepositoryLookup",
    "NamespaceRepositoryCreateMutations",
    "NamespaceRepositoryDeleteMutations",
    "NamespaceRepositoryMutations",
]
