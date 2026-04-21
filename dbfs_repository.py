#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

from dbfs_repository_lookup import NamespaceRepositoryLookup
from dbfs_repository_mutations import NamespaceRepositoryMutations


class NamespaceRepository(NamespaceRepositoryLookup, NamespaceRepositoryMutations):
    def __init__(self, dbfs):
        self.dbfs = dbfs
        self._lookup_epoch = None
        self._dir_id_cache = {}
        self._file_id_cache = {}
        self._file_mode_cache = {}
        self._hardlink_id_cache = {}
        self._symlink_id_cache = {}
        self._entry_cache = {}
        self._symlink_attrs_cache = {}
