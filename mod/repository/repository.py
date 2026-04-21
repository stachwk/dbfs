#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

from .lookup import NamespaceRepositoryLookup
from .attrs_listing import NamespaceRepositoryAttrsListing
from .create import NamespaceRepositoryCreateMutations
from .delete import NamespaceRepositoryDeleteMutations


class NamespaceRepository(
    NamespaceRepositoryLookup,
    NamespaceRepositoryAttrsListing,
    NamespaceRepositoryCreateMutations,
    NamespaceRepositoryDeleteMutations,
):
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
