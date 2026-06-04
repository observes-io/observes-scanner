#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

from dataclasses import dataclass, field
from typing import List, Literal, Optional

SUPPORTED_PLATFORMS = ("azure_devops", "github")
TargetPlatform = Literal["azure_devops", "github"]
AUTH_MODES = ("pat", "service-principal", "managed-identity", "default")
AuthMode = Literal["pat", "service-principal", "managed-identity", "default"]


@dataclass
class ScannerConfig:
    organization: str
    job_id: str
    pat_token: Optional[str] = None
    target_platform: TargetPlatform = "azure_devops"
    results_dir: Optional[str] = None
    projects: List[str] = field(default_factory=list)
    top_branches_to_scan: int = 5
    # Authentication mode
    auth_mode: AuthMode = "pat"
    # Service principal / managed identity settings
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    client_certificate_path: Optional[str] = None
    # Identity resolution settings (requires laughing-lamp package)
    resolve_identities: bool = False  # Enable identity resolution
    identity_resolution_resolve: bool = True  # Actually call cloud APIs (vs just extract)
    # Skip options for faster scans
    skip_feeds: bool = False  # Skip artifact feeds scanning
    skip_committer_stats: bool = False  # Skip committer stats calculation
    skip_builds: bool = False  # Skip builds scanning
    skip_users: bool = False  # Skip users, RBAC, and PAT token discovery
    skip_sast: bool = False  # Skip CICD SAST scanning on PIR snapshots
    # Observes Platform integration
    observes_platform_url: Optional[str] = None  # Observes Platform API base URL
    observes_platform_api_key: Optional[str] = None  # Observes Platform API key
