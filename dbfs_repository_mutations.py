#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

from dbfs_repository_create import NamespaceRepositoryCreateMutations
from dbfs_repository_delete import NamespaceRepositoryDeleteMutations


class NamespaceRepositoryMutations(NamespaceRepositoryCreateMutations, NamespaceRepositoryDeleteMutations):
    pass
