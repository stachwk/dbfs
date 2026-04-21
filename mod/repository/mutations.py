#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

from .create import NamespaceRepositoryCreateMutations
from .delete import NamespaceRepositoryDeleteMutations


class NamespaceRepositoryMutations(NamespaceRepositoryCreateMutations, NamespaceRepositoryDeleteMutations):
    pass
