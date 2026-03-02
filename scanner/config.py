#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class ScannerConfig:
    organization: str
    job_id: str
    pat_token: str
    results_dir: Optional[str] = None
    projects: List[str] = field(default_factory=list)
    top_branches_to_scan: int = 5
    # Identity resolution settings (requires laughing-lamp package)
    resolve_identities: bool = False  # Enable identity resolution
    identity_resolution_resolve: bool = True  # Actually call cloud APIs (vs just extract)
    # Skip options for faster scans
    skip_feeds: bool = False  # Skip artifact feeds scanning
    skip_committer_stats: bool = False  # Skip committer stats calculation
    skip_builds: bool = False  # Skip builds scanning
