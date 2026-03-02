#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

SCANNER_VERSION = "1.1.0"

import os
import sys

from scanner.cli import parse_config
from scanner.config import ScannerConfig
from scanner.orchestrator import run_scan
from scanner.output import format_size


def check_laughing_lamp_available():
    """Check if laughing-lamp package is installed."""
    try:
        import laughing_lamp
        return True
    except ImportError:
        return False


def scan_azdevops(
    organization,
    job_id,
    pat_token=None,
    results_dir=None,
    projects=None,
    top_branches_to_scan=0,
    resolve_identities=False,
    skip_feeds=False,
    skip_committer_stats=False,
    skip_builds=False,
):
    # Check if laughing-lamp is available when identity resolution is requested
    if resolve_identities and not check_laughing_lamp_available():
        print("\n" + "=" * 70)
        print("WARNING: --resolve-identities requested but laughing-lamp is not installed.")
        print("=" * 70)
        print("\nContinuing scan without identity resolution...\n")
        resolve_identities = False
    
    config = ScannerConfig(
        organization=organization,
        job_id=job_id,
        pat_token=pat_token,
        results_dir=results_dir or os.getcwd(),
        projects=projects or [],
        top_branches_to_scan=top_branches_to_scan,
        resolve_identities=resolve_identities,
        skip_feeds=skip_feeds,
        skip_committer_stats=skip_committer_stats,
        skip_builds=skip_builds,
    )
    return run_scan(config=config, scanner_version=SCANNER_VERSION)


def main():
    config = parse_config()
    
    # Check if laughing-lamp is available when identity resolution is requested
    if config.resolve_identities and not check_laughing_lamp_available():
        print("\n" + "=" * 70)
        print("WARNING: --resolve-identities requested but laughing-lamp is not installed.")
        print("=" * 70)
        print("\nContinuing scan without identity resolution...\n")
        config.resolve_identities = False
    
    run_scan(config=config, scanner_version=SCANNER_VERSION)


if __name__ == "__main__":
    main()
