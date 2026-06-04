#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

import base64
import logging
import threading
import time
from typing import Literal

logger = logging.getLogger(__name__)

AZURE_DEVOPS_SCOPE = "https://app.vssps.visualstudio.com/.default"

# Refresh the token 5 minutes before it expires to avoid mid-request failures
_TOKEN_REFRESH_BUFFER_SECONDS = 300

AuthMode = Literal["pat", "service-principal", "managed-identity", "default"]


class AuthProvider:
    """
    Provides authorization headers for Azure DevOps API requests.

    Supports four authentication modes:
    - PAT: Personal Access Token (Basic auth)
    - Service Principal: Microsoft Entra app registration (Bearer token via client credentials)
    - Managed Identity: Azure-managed identity (Bearer token, no secrets needed)
    - Default: Uses DefaultAzureCredential which automatically tries, in order:
        1. Environment variables (AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET / AZURE_CLIENT_CERTIFICATE_PATH)
        2. Workload identity (Azure Kubernetes Service workload identity webhook)
        3. Managed identity (system- or user-assigned)
        4. Shared token cache (Windows only - Visual Studio / Azure CLI sign-in)
    """

    def __init__(self, config):
        self.auth_mode: AuthMode = getattr(config, "auth_mode", "pat")
        self._credential = None
        self._cached_token = None      # The AccessToken object
        self._token_lock = threading.Lock()

        # Silence verbose azure-identity / azure-core HTTP debug logs.
        # These loggers emit full request/response details for every credential
        # that DefaultAzureCredential tries (and fails) before finding one that works.
        # On failure, the AuthProvider surfaces a clear error message instead.
        for noisy_logger in (
            "azure.identity",
            "azure.core.pipeline.policies.http_logging_policy",
        ):
            logging.getLogger(noisy_logger).setLevel(logging.WARNING)

        if self.auth_mode == "pat":
            pat_token = config.pat_token
            if not pat_token:
                raise ValueError("Personal Access Token (PAT) must be provided for auth_mode='pat'")
            self._basic_token = base64.b64encode(f":{pat_token}".encode()).decode()

        elif self.auth_mode == "service-principal":
            self._init_service_principal(config)

        elif self.auth_mode == "managed-identity":
            self._init_managed_identity(config)

        elif self.auth_mode == "default":
            self._init_default_credential(config)

        else:
            raise ValueError(f"Unsupported auth_mode: {self.auth_mode}")

    def _init_service_principal(self, config):
        try:
            from azure.identity import ClientSecretCredential, CertificateCredential
        except ImportError:
            raise ImportError(
                "The 'azure-identity' package is required for service principal authentication. "
                "Install it with: pip install azure-identity"
            )

        tenant_id = getattr(config, "tenant_id", None)
        client_id = getattr(config, "client_id", None)
        client_secret = getattr(config, "client_secret", None)
        client_certificate_path = getattr(config, "client_certificate_path", None)

        if not tenant_id:
            raise ValueError("--tenant-id is required for service principal authentication")
        if not client_id:
            raise ValueError("--client-id is required for service principal authentication")

        if client_certificate_path:
            logger.info("Using certificate-based service principal authentication")
            self._credential = CertificateCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                certificate_path=client_certificate_path,
            )
        elif client_secret:
            logger.info("Using client secret service principal authentication")
            self._credential = ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
            )
        else:
            raise ValueError(
                "Either --client-secret or --client-certificate-path is required "
                "for service principal authentication"
            )

    def _init_managed_identity(self, config):
        try:
            from azure.identity import ManagedIdentityCredential
        except ImportError:
            raise ImportError(
                "The 'azure-identity' package is required for managed identity authentication. "
                "Install it with: pip install azure-identity"
            )

        client_id = getattr(config, "client_id", None)
        if client_id:
            logger.info("Using user-assigned managed identity (client_id=%s)", client_id)
            self._credential = ManagedIdentityCredential(client_id=client_id)
        else:
            logger.info("Using system-assigned managed identity")
            self._credential = ManagedIdentityCredential()

    def _init_default_credential(self, config):
        try:
            from azure.identity import DefaultAzureCredential
        except ImportError:
            raise ImportError(
                "The 'azure-identity' package is required for default credential authentication. "
                "Install it with: pip install azure-identity"
            )

        client_id = getattr(config, "client_id", None)
        kwargs = {}
        if client_id:
            # When a client_id is provided, use it for managed identity selection
            kwargs["managed_identity_client_id"] = client_id

        logger.info(
            "Using DefaultAzureCredential (auto-detects environment variables, "
            "workload identity, managed identity, shared token cache)"
        )
        self._credential = DefaultAzureCredential(**kwargs)

    @property
    def authorization_header(self) -> str:
        if self.auth_mode == "pat":
            return f"Basic {self._basic_token}"

        # Service principal, managed identity, or default - acquire/reuse a Bearer token
        return f"Bearer {self._get_cached_token()}"

    def _get_cached_token(self) -> str:
        """Return a valid access token string, refreshing only when near expiry."""
        with self._token_lock:
            if self._cached_token and not self._is_token_expired():
                return self._cached_token.token

            try:
                logger.debug("Acquiring new Entra ID token (scope=%s)", AZURE_DEVOPS_SCOPE)
                self._cached_token = self._credential.get_token(AZURE_DEVOPS_SCOPE)
                remaining = self._cached_token.expires_on - time.time()
                if remaining > 0:
                    logger.info(
                        "Entra ID token acquired successfully (expires in %d minutes)",
                        int(remaining / 60),
                    )
                return self._cached_token.token
            except Exception as exc:
                # On failure, temporarily raise azure log levels so the user
                # can see diagnostic details in the retry or final error.
                logging.getLogger("azure.identity").setLevel(logging.DEBUG)
                logger.error(
                    "Failed to acquire Entra ID token (%s). "
                    "Ensure the identity is configured correctly. "
                    "Details: %s",
                    type(exc).__name__,
                    exc,
                )
                raise

    def _is_token_expired(self) -> bool:
        """Check if the cached token is expired or about to expire."""
        if not self._cached_token:
            return True
        return time.time() >= (self._cached_token.expires_on - _TOKEN_REFRESH_BUFFER_SECONDS)

    @property
    def token_for_basic_compat(self) -> str:
        """
        Returns a value compatible with the existing code that expects
        a base64-encoded token for Basic auth headers.

        For PAT mode, returns the base64 token.
        For SP/MI modes, this should NOT be used - use authorization_header instead.
        """
        if self.auth_mode == "pat":
            return self._basic_token
        raise RuntimeError(
            f"token_for_basic_compat is not available in '{self.auth_mode}' mode. "
            "Use authorization_header property instead."
        )

    @property
    def is_bearer_auth(self) -> bool:
        return self.auth_mode != "pat"
