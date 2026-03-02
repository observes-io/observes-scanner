#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


def requests_session_with_retries(total=6, backoff_factor=1, status_forcelist=(500, 502, 503, 504)):
    session = requests.Session()
    retry_strategy = Retry(
        total=total,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session


http = requests_session_with_retries()


class AdoHttpClient:
    def __init__(self, token: str):
        self.token = token

    @property
    def headers(self):
        return {
            "Content-Type": "application/json",
            "Authorization": f"Basic {self.token}",
        }

    def get_json(self, url):
        response = http.get(url=url, headers=self.headers)
        response.raise_for_status()
        data = response.json()
        return data["value"] if "value" in data else data

    def get_text(self, url):
        response = http.get(url=url, headers=self.headers)
        response.raise_for_status()
        return response.text

    def post_json(self, url, payload):
        response = http.post(url=url, headers=self.headers, data=payload)
        response.raise_for_status()
        return response.json()


def fetch_data(url, token, qret=False):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {token}",
    }
    try:
        logger.debug(f"Fetching data from {url}")
        try:
            response = http.get(url=url, headers=headers)
        except ConnectionResetError as cre:
            logger.warning(f"Connection reset error: {cre}")
            return None

        if qret:
            return response.text

        response.raise_for_status()
        data = response.json()

        return data["value"] if "value" in data.keys() else data
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error: {http_err}")
    except Exception as err:
        logger.error(f"Error fetching data: {err}")
        return None


def fetch_data_with_headers(url, token):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {token}",
    }

    try:
        logger.debug(f"Fetching data with headers from {url}")
        response = http.get(url=url, headers=headers)
        response.raise_for_status()
        data = response.json()
        result_data = data["value"] if "value" in data.keys() else data
        return result_data, response.headers
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error: {http_err}")
        return None, None
    except Exception as err:
        logger.error(f"Error fetching data: {err}")
        return None, None


def post_data(url, payload, token):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {token}",
    }
    try:
        response = http.post(url=url, headers=headers, data=payload)
        response.raise_for_status()
        logger.debug(f"Data posted to {url}")
        return response.json(), None
    except requests.exceptions.HTTPError as http_err:
        try:
            error_message = response.json().get("message", str(http_err))
        except Exception:
            error_message = str(http_err)
        return None, error_message
    except Exception as err:
        return None, str(err)
