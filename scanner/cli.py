#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

import argparse
import os
import sys

from scanner.config import ScannerConfig


def build_parser():
    parser = argparse.ArgumentParser(description="Run Azure DevOps scan.")
    parser.add_argument("-o", "--organization", required=True, help="Azure DevOps organization name")
    parser.add_argument("-j", "--job-id", required=True, help="Job ID for this scan")
    parser.add_argument(
        "-p",
        "--pat-token",
        required=False,
        help="Azure DevOps Personal Access Token (can also be set via AZURE_DEVOPS_PAT environment variable)",
    )
    parser.add_argument(
        "-r", "--results-dir", default=None, help="Directory to save scan results (default: current working directory)"
    )
    parser.add_argument(
        "--skip-committer-stats",
        action="store_true",
        default=False,
        help="Skip committer stats calculation for faster scans",
    )
    parser.add_argument(
        "-rb",
        "--top-branches-to-scan",
        type=int,
        default=5,
        help="Number of default plus top branches to scan for each repository. -1 for all branches, 0 for default branch only, >= X for default and X top branches (default: 5)",
    )
    parser.add_argument("--projects", default=None, help="Optional comma separated list of project names or IDs to filter scan")
    parser.add_argument(
        "--resolve-identities",
        action="store_true",
        default=False,
        help="Enable identity resolution for service connections, variable groups, and secure files (requires laughing-lamp package)",
    )
    parser.add_argument(
        "--skip-feeds",
        action="store_true",
        default=False,
        help="Skip artifact feeds scanning for faster scans",
    )
    parser.add_argument(
        "--skip-builds",
        action="store_true",
        default=False,
        help="Skip build and build pipeline data collection (only resources and permissions)",
    )
    return parser


def resolve_pat_token(cli_pat_token):
    pat_token = cli_pat_token or os.environ.get("AZURE_DEVOPS_PAT")
    if not pat_token:
        print(
            "Error: Azure DevOps Personal Access Token must be provided via --pat-token or AZURE_DEVOPS_PAT environment variable.",
            file=sys.stderr,
        )
        raise SystemExit(1)
    return pat_token


def parse_config(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    pat_token = resolve_pat_token(args.pat_token)
    projects = [p.strip() for p in args.projects.split(",")] if args.projects else []
    return ScannerConfig(
        organization=args.organization,
        job_id=args.job_id,
        pat_token=pat_token,
        results_dir=args.results_dir,
        projects=projects,
        top_branches_to_scan=args.top_branches_to_scan,
        resolve_identities=args.resolve_identities,
        skip_feeds=args.skip_feeds,
        skip_committer_stats=args.skip_committer_stats,
        skip_builds=args.skip_builds,
    )
