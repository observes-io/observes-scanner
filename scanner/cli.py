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

from scanner.config import ScannerConfig, SUPPORTED_PLATFORMS, AUTH_MODES


def build_parser():
    parser = argparse.ArgumentParser(description="Run DevOps platform scan.")
    parser.add_argument(
        "-t",
        "--target-platform",
        choices=SUPPORTED_PLATFORMS,
        default="azure_devops",
        help="Target platform to scan (default: azure_devops)",
    )
    parser.add_argument("-o", "--organization", required=True, help="Azure DevOps organization name")
    parser.add_argument("-j", "--job-id", required=True, help="Job ID for this scan")
    parser.add_argument(
        "-p",
        "--pat-token",
        required=False,
        help="Azure DevOps Personal Access Token (can also be set via AZURE_DEVOPS_PAT environment variable)",
    )

    # --- Authentication mode ---
    parser.add_argument(
        "--auth-mode",
        choices=AUTH_MODES,
        default="default",
        help=(
            "Authentication method: "
            "'pat' (Personal Access Token, default), "
            "'service-principal' (Microsoft Entra app registration), "
            "'managed-identity' (Azure-managed identity for VM/App Service/Functions), "
            "'default' (auto-detect via DefaultAzureCredential - tries env vars, "
            "workload identity, managed identity, shared token cache in order)"
        ),
    )
    parser.add_argument(
        "--tenant-id",
        required=False,
        help="Microsoft Entra tenant ID (required for --auth-mode=service-principal). "
             "Can also be set via AZURE_TENANT_ID environment variable.",
    )
    parser.add_argument(
        "--client-id",
        required=False,
        help="Application (client) ID for service principal, or client ID for user-assigned managed identity. "
             "Can also be set via AZURE_CLIENT_ID environment variable.",
    )
    parser.add_argument(
        "--client-secret",
        required=False,
        help="Client secret for service principal authentication. "
             "Can also be set via AZURE_CLIENT_SECRET environment variable. "
             "Prefer --client-certificate-path for production use.",
    )
    parser.add_argument(
        "--client-certificate-path",
        required=False,
        help="Path to a PEM or PFX certificate file for service principal authentication (more secure than client secret).",
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
    parser.add_argument(
        "--skip-users",
        action="store_true",
        default=False,
        help="Skip users, RBAC, and PAT token discovery",
    )
    parser.add_argument(
        "--skip-sast",
        action="store_true",
        default=False,
        help="Skip CICD SAST scanning on pipeline snapshots",
    )
    # --- Platform integration ---
    parser.add_argument(
        "--observes-platform-url",
        required=False,
        help="Observes Platform API URL. Can also be set via OBSERVES_PLATFORM_URL environment variable.",
    )
    parser.add_argument(
        "--observes-platform-api-key",
        required=False,
        help="Observes Platform API key. Can also be set via OBSERVES_PLATFORM_API_KEY environment variable.",
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


def resolve_auth_config(args):
    """Resolve authentication configuration based on auth mode."""
    auth_mode = args.auth_mode

    if auth_mode == "pat":
        pat_token = resolve_pat_token(args.pat_token)
        return {"auth_mode": "pat", "pat_token": pat_token}

    if auth_mode == "service-principal":
        tenant_id = args.tenant_id or os.environ.get("AZURE_TENANT_ID")
        client_id = args.client_id or os.environ.get("AZURE_CLIENT_ID")
        client_secret = args.client_secret or os.environ.get("AZURE_CLIENT_SECRET")
        client_certificate_path = args.client_certificate_path

        if not tenant_id:
            print("Error: --tenant-id or AZURE_TENANT_ID is required for service principal auth.", file=sys.stderr)
            raise SystemExit(1)
        if not client_id:
            print("Error: --client-id or AZURE_CLIENT_ID is required for service principal auth.", file=sys.stderr)
            raise SystemExit(1)
        if not client_secret and not client_certificate_path:
            print(
                "Error: --client-secret (or AZURE_CLIENT_SECRET) or --client-certificate-path is required "
                "for service principal auth.",
                file=sys.stderr,
            )
            raise SystemExit(1)

        return {
            "auth_mode": "service-principal",
            "pat_token": None,
            "tenant_id": tenant_id,
            "client_id": client_id,
            "client_secret": client_secret,
            "client_certificate_path": client_certificate_path,
        }

    if auth_mode == "managed-identity":
        client_id = args.client_id or os.environ.get("AZURE_CLIENT_ID")
        return {
            "auth_mode": "managed-identity",
            "pat_token": None,
            "client_id": client_id,  # None = system-assigned
        }

    if auth_mode == "default":
        client_id = args.client_id or os.environ.get("AZURE_CLIENT_ID")
        return {
            "auth_mode": "default",
            "pat_token": None,
            "client_id": client_id,  # optional: used for managed identity selection
        }

    print(f"Error: Unknown auth mode '{auth_mode}'.", file=sys.stderr)
    raise SystemExit(1)


def parse_config(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    auth = resolve_auth_config(args)
    projects = [p.strip() for p in args.projects.split(",")] if args.projects else []
    return ScannerConfig(
        organization=args.organization,
        job_id=args.job_id,
        target_platform=args.target_platform,
        results_dir=args.results_dir,
        projects=projects,
        top_branches_to_scan=args.top_branches_to_scan,
        resolve_identities=args.resolve_identities,
        skip_feeds=args.skip_feeds,
        skip_committer_stats=args.skip_committer_stats,
        skip_builds=args.skip_builds,
        skip_users=args.skip_users,
        skip_sast=args.skip_sast,
        observes_platform_url=args.observes_platform_url or os.environ.get("OBSERVES_PLATFORM_URL"),
        observes_platform_api_key=args.observes_platform_api_key or os.environ.get("OBSERVES_PLATFORM_API_KEY"),
        **auth,
    )
