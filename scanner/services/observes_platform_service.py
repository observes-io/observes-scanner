#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

import json
import logging
from typing import Dict, Optional
from urllib.parse import urljoin

import requests

logger = logging.getLogger(__name__)

# Timeout for all HTTP calls to the platform API (connect, read) in seconds
_REQUEST_TIMEOUT = (10, 30)


class ObservesPlatformService:
    def __init__(self, observes_platform_url: str = None, observes_platform_api_key: str = None):
        self._base_url = observes_platform_url.rstrip("/") + "/" if observes_platform_url else None
        self._api_key = observes_platform_api_key
        self._session: Optional[requests.Session] = None
        self._access_token: Optional[str] = None

    # ------------------------------------------------------------------
    # Availability
    # ------------------------------------------------------------------

    @property
    def is_configured(self) -> bool:
        return bool(self._base_url and self._api_key)

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def authenticate(self) -> bool:
        if not self.is_configured:
            return False

        try:
            url = urljoin(self._base_url, "api/v1/auth/token")
            resp = requests.post(
                url,
                json={"apiKey": self._api_key},
                headers={"Content-Type": "application/json"},
                timeout=_REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            self._access_token = data.get("accessToken")
            if not self._access_token:
                logger.warning("Observes Platform authentication succeeded but no accessToken returned")
                return False
            self._session = requests.Session()
            self._session.headers.update({
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            })
            logger.info("Authenticated to Observes Platform successfully")
            return True
        except requests.RequestException as e:
            logger.warning(f"Observes Platform authentication failed: {e}")
            return False

    def _get(self, path: str) -> Optional[Dict]:
        """Authenticated GET - returns parsed JSON or None."""
        if not self._session:
            return None
        try:
            url = urljoin(self._base_url, path)
            resp = self._session.get(url, timeout=_REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.warning(f"Observes Platform GET {path} failed: {e}")
            return None

    # ------------------------------------------------------------------
    # Scan configuration
    # ------------------------------------------------------------------

    def get_inventory_config(self) -> Optional[Dict]:
        """Fetch the starter inventory configuration from the platform."""
        data = self._get("api/v1/config/inventory")
        if data:
            logger.info("Loaded inventory configuration from Observes Platform")
        return data

    def get_build_settings_expectations(self) -> Optional[Dict]:
        """Fetch expected build settings from the platform."""
        data = self._get("api/v1/config/build-settings")
        if data:
            logger.info("Loaded build settings expectations from Observes Platform")
        return data

    def get_sast_patterns(self) -> Optional[Dict]:
        """
        Fetch SAST patterns from the platform.

        Implements the interface expected by ``SastService(api_client=...)``.
        """
        data = self._get("api/v1/config/sast-patterns")
        if data:
            logger.info("Loaded SAST patterns from Observes Platform")
        return data

    # ------------------------------------------------------------------
    # Result upload
    # ------------------------------------------------------------------

    def upload_results(self, result: dict, job_id: str) -> bool:
        if not self._session:
            logger.debug("Platform not authenticated - skipping result upload")
            return False

        # Step 1: obtain upload URL
        try:
            url = urljoin(self._base_url, "api/v1/scans/upload-url")
            resp = self._session.post(
                url,
                json={"jobId": job_id},
                timeout=_REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            upload_info = resp.json()
            blob_url = upload_info.get("uploadUrl")
            if not blob_url:
                logger.warning("Platform returned no uploadUrl")
                return False
        except requests.RequestException as e:
            logger.warning(f"Failed to obtain upload URL from platform: {e}")
            return False

        # Step 2: serialise as JSON Lines
        lines = []
        for key, value in result.items():
            lines.append(json.dumps({"type": key, "data": value}, default=str))
        payload = "\n".join(lines)

        # Step 3: upload to blob
        try:
            put_resp = requests.put(
                blob_url,
                data=payload.encode("utf-8"),
                headers={
                    "x-ms-blob-type": "BlockBlob",
                    "Content-Type": "application/x-ndjson",
                },
                timeout=(10, 120),
            )
            put_resp.raise_for_status()
            logger.info(f"Scan results uploaded to platform ({len(payload)} bytes)")
            return True
        except requests.RequestException as e:
            logger.warning(f"Failed to upload scan results to platform: {e}")
            return False
