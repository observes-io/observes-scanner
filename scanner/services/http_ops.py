#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

import os

from scanner.http_client import fetch_data, fetch_data_with_headers, post_data
from scanner.services.runtime import endpoint_family


class HttpOps:
    def __init__(self, token: str, runtime_state, logger):
        self.token = token
        self.runtime_state = runtime_state
        self.logger = logger

    def _mark(self, verb: str, url: str):
        family = endpoint_family(url)
        with self.runtime_state.perf_lock:
            if verb == "POST":
                self.runtime_state.perf.post_total += 1
                self.runtime_state.perf.by_family_post[family] += 1
            else:
                self.runtime_state.perf.get_total += 1
                self.runtime_state.perf.by_family_get[family] += 1

    def fetch_data(self, url, qret=False):
        self._mark("GET", url)
        return fetch_data(url, self.token, qret=qret)

    def fetch_data_with_headers(self, url):
        self._mark("GET", url)
        return fetch_data_with_headers(url, self.token)

    def post_data(self, url, payload):
        self._mark("POST", url)
        return post_data(url, payload, self.token)

    def log_perf_summary(self):
        if os.environ.get("SCANNER_PERF_DEBUG") != "1":
            return
        get_by_family = dict(self.runtime_state.perf.by_family_get)
        post_by_family = dict(self.runtime_state.perf.by_family_post)
        self.logger.info(
            "Scanner perf counters | GET total=%s POST total=%s GET by family=%s POST by family=%s",
            self.runtime_state.perf.get_total,
            self.runtime_state.perf.post_total,
            get_by_family,
            post_by_family,
        )
