#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import signal
import threading
import multiprocessing
import faulthandler

_VERBOSE = os.environ.get("DBFS_HANG_DEBUG_VERBOSE", "1") == "1"

def print_err(s):
    # Wypisuj tylko gdy wlaczono verbose.
    if _VERBOSE:
        print(f"stderr: {s}", file=sys.stderr)

def _dump_state():
    print_err("=== DBFS HANG DEBUG: watki ===")
    for t in threading.enumerate():
        print_err(f"thread name={t.name} daemon={t.daemon} alive={t.is_alive()}")
    try:
        children = multiprocessing.active_children()
    except Exception as exc:
        print_err(f"active_children() blad: {exc!r}")
        children = []
    print_err(f"=== DBFS HANG DEBUG: active_children={children} ===")
    sys.stderr.flush()

def install_hang_debug(timeout_seconds=10, repeat=True):
    # Wlacz traceback wszystkich watkow po przekroczeniu timeoutu.
    faulthandler.enable(all_threads=True)
    faulthandler.dump_traceback_later(timeout_seconds, repeat=repeat)
    print_err(f"hang debug aktywny: timeout_seconds={timeout_seconds}, repeat={repeat}")

def finalize_or_force_exit(exit_code=0, grace_seconds=2):
    # Pokaz stan tuz przed wyjsciem.
    _dump_state()

    # Sprobuj anulowac planowany dump.
    try:
        faulthandler.cancel_dump_traceback_later()
    except Exception as exc:
        print_err(f"cancel_dump_traceback_later() blad: {exc!r}")

    # Daj chwile na naturalne domkniecie ewentualnych buforow.
    sys.stdout.flush()
    sys.stderr.flush()
    time.sleep(grace_seconds)

    # Jesli interpreter nadal zyje przez niezamkniete watki/procesy,
    # wymus twarde wyjscie.
    print_err(f"wymuszam os._exit({exit_code}) po grace_seconds={grace_seconds}")
    os._exit(exit_code)
