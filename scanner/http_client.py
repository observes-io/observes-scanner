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
from urllib3.exceptions import ResponseError

logger = logging.getLogger(__name__)


def requests_session_with_retries(total=5, backoff_factor=0.1, status_forcelist=(429, 502, 503, 504)):
    session = requests.Session()
    retry_strategy = Retry(
        total=total,
        backoff_factor=backoff_factor,
        backoff_max=2,
        status_forcelist=status_forcelist,
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session


http = requests_session_with_retries()


class AdoHttpClient:
    def __init__(self, token: str = None, auth_provider=None):
        self._auth_provider = auth_provider
        self._static_token = token  # legacy: base64-encoded PAT

    @property
    def headers(self):
        if self._auth_provider:
            auth_header = self._auth_provider.authorization_header
        else:
            auth_header = f"Basic {self._static_token}"
        return {
            "Content-Type": "application/json",
            "Authorization": auth_header,
        }

    def get_json(self, url):
        try:
            response = http.get(url=url, headers=self.headers)
            if response.status_code == 500:
                logger.warning(f"Server error (500) from {url} - resource may be unavailable")
                return None
            response.raise_for_status()
            data = response.json()
            return data["value"] if "value" in data else data
        except ResponseError as re:
            logger.warning(f"Response error from Azure DevOps: {re}")
            return None

    def get_text(self, url):
        try:
            response = http.get(url=url, headers=self.headers)
            if response.status_code == 500:
                logger.warning(f"Server error (500) from {url} - resource may be unavailable")
                return None
            response.raise_for_status()
            return response.text
        except ResponseError as re:
            logger.warning(f"Response error from Azure DevOps: {re}")
            return None

    def post_json(self, url, payload):
        response = http.post(url=url, headers=self.headers, data=payload)
        response.raise_for_status()
        return response.json()


def fetch_data(url, token=None, qret=False, auth_provider=None):
    if auth_provider:
        auth_header = auth_provider.authorization_header
    else:
        auth_header = f"Basic {token}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": auth_header,
    }
    try:
        logger.debug(f"Fetching data from {url}")
        try:
            response = http.get(url=url, headers=headers)
        except ConnectionResetError as cre:
            logger.warning(f"Connection reset error: {cre}")
            return None
        except ResponseError as re:
            # Handle urllib3 ResponseError (e.g., too many 500 errors)
            logger.warning(f"Response error from Azure DevOps (resource may be unavailable): {re}")
            return None

        # Handle 500 errors gracefully (Azure DevOps sometimes returns 500 for missing/deleted resources)
        if response.status_code == 500:
            logger.warning(f"Server error (500) from {url} - resource may be unavailable or deleted")
            return None

        if qret:
            if response.ok:
                text = response.text
                # Azure DevOps sometimes returns error JSON with 200 status
                # (e.g. TaskOrchestrationPlanNotFoundException for builds
                # that failed before creating an execution plan).
                if text and text.lstrip().startswith("{") and "Exception" in text:
                    try:
                        import json
                        err_body = json.loads(text)
                        if "typeName" in err_body or "typeKey" in err_body:
                            logger.debug(f"API returned error object for {url}: {err_body.get('message', text[:120])}")
                            return None
                    except (json.JSONDecodeError, ValueError):
                        pass
                return text
            logger.debug(f"Non-success status {response.status_code} for {url}")
            return None

        response.raise_for_status()
        data = response.json()

        return data["value"] if "value" in data.keys() else data
    except requests.exceptions.HTTPError as http_err:
        if "500" in str(http_err):
            logger.warning(f"HTTP 500 error (resource may be unavailable): {http_err}")
        else:
            logger.error(f"HTTP error: {http_err}")
    except Exception as err:
        logger.error(f"Error fetching data: {err}")
        return None


def fetch_data_with_headers(url, token=None, auth_provider=None):
    if auth_provider:
        auth_header = auth_provider.authorization_header
    else:
        auth_header = f"Basic {token}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": auth_header,
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


def post_data(url, payload, token=None, auth_provider=None):
    if auth_provider:
        auth_header = auth_provider.authorization_header
    else:
        auth_header = f"Basic {token}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": auth_header,
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
