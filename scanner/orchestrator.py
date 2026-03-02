#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from scanner.ado_client import AzureDevOpsManager
from scanner.output import write_scan_result
from scanner.html_report import write_html_report
from scanner.services.identity_resolution import IdentityResolutionService
from scanner.filters import filter_builds, filter_definitions, filter_protected_resources

logger = logging.getLogger(__name__)


def setup_logging(job_id: str, results_dir: str = None):
    """
    Configure logging to write detailed logs to scanner_logs/ folder
    and minimal high-level messages to console.
    """
    # Create scanner_logs directory
    log_dir = Path(results_dir or os.getcwd()) / "scanner_logs"
    log_dir.mkdir(exist_ok=True)
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"scan_{job_id}_{timestamp}.log"
    
    # Remove any existing handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Set root logger level to INFO (captures important messages)
    root_logger.setLevel(logging.INFO)
    
    # File handler - captures INFO and above
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)
    
    # Console handler - only high-level workflow messages (INFO and above)
    # Add filter to exclude verbose urllib3 retry messages
    class ConsoleFilter(logging.Filter):
        def filter(self, record):
            # Exclude urllib3 connection retry warnings from console
            if record.name.startswith('urllib3.connectionpool'):
                return False
            return True
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.addFilter(ConsoleFilter())
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    logger.info(f"Logging initialized. Detailed logs: {log_file}")
    return str(log_file)


def build_starter_inventory():
    return {
        "endpoint": {
            "api_endpoint": "serviceendpoint/endpoints",
            "api_version": "?api-version=7.1",
            "project_path": "serviceEndpointProjectReferences[].projectReference.id",
            "protected_resources": [],
            "level": "project",
        },
        "pools": {
            "api_endpoint": "distributedtask/pools",
            "api_version": "?api-version=7.1",
            "project_path": "projectId",
            "protected_resources": [],
            "level": "org",
        },
        "queue": {
            "api_endpoint": "distributedtask/queues",
            "api_version": "?api-version=7.1",
            "project_path": "projectId",
            "protected_resources": [],
            "level": "project",
        },
        "variablegroup": {
            "api_endpoint": "distributedtask/variablegroups",
            "api_version": "?api-version=7.1",
            "protected_resources": [],
            "level": "project",
        },
        "securefile": {
            "api_endpoint": "distributedtask/securefiles",
            "api_version": "?api-version=7.1",
            "protected_resources": [],
            "level": "project",
        },
        "repository": {
            "api_endpoint": "git/repositories",
            "api_version": "?api-version=7.1",
            "protected_resources": [],
            "level": "project",
        },
        "environment": {
            "api_endpoint": "distributedtask/environments",
            "query_params": "expands=1",
            "api_version": "?api-version=7.1",
            "protected_resources": [],
            "level": "project",
        },
    }


def default_build_settings_expectations():
    return {
        "enforceReferencedRepoScopedToken": True,
        "disableClassicPipelineCreation": True,
        "disableClassicBuildPipelineCreation": True,
        "disableClassicReleasePipelineCreation": True,
        "forkProtectionEnabled": True,
        "buildsEnabledForForks": False,
        "enforceJobAuthScopeForForks": True,
        "enforceNoAccessToSecretsFromForks": True,
        "isCommentRequiredForPullRequest": True,
        "requireCommentsForNonTeamMembersOnly": False,
        "requireCommentsForNonTeamMemberAndNonContributors": True,
        "enableShellTasksArgsSanitizing": True,
        "enableShellTasksArgsSanitizingAudit": True,
        "disableImpliedYAMLCiTrigger": True,
        "statusBadgesArePrivate": True,
        "enforceSettableVar": True,
        "enforceJobAuthScope": True,
        "enforceJobAuthScopeForReleases": True,
        "publishPipelineMetadata": True,
    }


def run_scan(config, scanner_version: str):
    organization = config.organization
    job_id = config.job_id
    pat_token = config.pat_token
    projects = config.projects or []
    results_dir = config.results_dir or os.getcwd()
    top_branches_to_scan = config.top_branches_to_scan
    resolve_identities = getattr(config, 'resolve_identities', False)
    identity_resolution_resolve = getattr(config, 'identity_resolution_resolve', True)
    skip_feeds = getattr(config, 'skip_feeds', False)
    skip_committer_stats = getattr(config, 'skip_committer_stats', False)
    skip_builds = getattr(config, 'skip_builds', False)

    if not organization:
        raise ValueError("Organization must be provided")
    if not job_id:
        raise ValueError("Job ID must be provided")
    if not pat_token:
        raise ValueError("Personal Access Token (PAT) must be provided")

    # Setup logging
    setup_logging(job_id=job_id, results_dir=results_dir)
    
    start_date = datetime.now().isoformat()
    logger.info(f"Starting scan for {organization} (Job ID: {job_id})")
    logger.debug(f"Configuration: projects={projects}, top_branches={top_branches_to_scan}, "
                 f"skip_builds={skip_builds}, skip_feeds={skip_feeds}, skip_committer_stats={skip_committer_stats}")
    
    az_manager = AzureDevOpsManager(
        organization=organization,
        project_filter=projects if projects else [],
        default_build_settings_expectations=default_build_settings_expectations(),
        pat_token=pat_token,
    )

    logger.info("Gathering project metrics and tasks...")
    stats = az_manager.get_project_language_metrics(az_manager.projects.values())
    tasks = az_manager.get_task_list()
    logger.debug(f"Retrieved {len(tasks)} task definitions")
    
    logger.info("Collecting build definitions and builds...")
    definitions, builds = az_manager.get_builds_per_definition_per_project(top_branches_to_scan=top_branches_to_scan, skip_builds=skip_builds)
    logger.debug(f"Found {len(definitions)} definitions and {len(builds)} builds")
    
    definitions = az_manager.get_build_definition_authorised_resources(definitions)
    
    logger.info("Scanning protected resources...")
    protected_resources_inventory_resources = az_manager.get_protected_resources(build_starter_inventory())
    builds = az_manager.resources_service.attach_used_service_connections_to_builds(
        builds,
        protected_resources_inventory_resources.get("endpoint", {}).get("protected_resources", [])
    )
    
    logger.info("Analyzing checks, approvals, and permissions...")
    protected_resources_inventory_resources_checks = az_manager.get_checks_approvals(protected_resources_inventory_resources)
    protected_resources_inventory_resources_checks_definitions = az_manager.get_permissions(
        protected_resources_inventory_resources_checks, definitions, builds
    )
    protected_resources_inventory_resources_checks_definitions = az_manager.enrich_resource_protection_and_cross_project(
        protected_resources_inventory_resources_checks_definitions
    )
    definitions = az_manager.get_enriched_build_definitions(definitions, protected_resources_inventory_resources_checks_definitions)
    build_service_accounts = az_manager.get_all_build_service_accounts()
    logger.debug(f"Found {len(build_service_accounts)} build service accounts")
    
    logger.info("Collecting repository commits...")
    commits = az_manager.get_commits_per_repository(
        protected_resources_inventory_resources_checks_definitions["repository"]["protected_resources"]
    )
    logger.debug(f"Retrieved {len(commits)} commits")
    if skip_committer_stats:
        logger.info("Skipping committer stats calculation")
        committer_stats = []
        protected_resources_inventory_resources_checks_definitions["repository"]["protected_resources"] = \
            protected_resources_inventory_resources_checks_definitions["repository"]["protected_resources"]
    else:
        logger.info("Calculating committer statistics...")
        committer_stats = az_manager.get_committer_stats(commits, build_service_accounts=build_service_accounts)
        logger.debug(f"Generated stats for {len(committer_stats)} committers")
        protected_resources_inventory_resources_checks_definitions["repository"][
            "protected_resources"
        ] = az_manager.enrich_repositories_with_committer_stats(
            protected_resources_inventory_resources_checks_definitions["repository"]["protected_resources"], commits
        )
    
    # Optionally skip artifact feeds scanning
    if skip_feeds:
        logger.info("Skipping artifact feeds scanning")
        artifacts = {"active": [], "recyclebin": []}
    else:
        logger.info("Scanning artifact feeds...")
        artifacts = az_manager.get_artifacts_feeds()
        logger.debug(f"Found {len(artifacts.get('active', []))} active feeds, "
                     f"{len(artifacts.get('recyclebin', []))} in recycle bin")
    
    logger.info("Enriching statistics...")
    stats = az_manager.get_enriched_stats(
        stats, protected_resources_inventory_resources_checks_definitions, definitions, builds, commits, artifacts
    )

    project_refs = [
        {"id": proj["id"], "name": proj["name"]}
        for proj in az_manager.projects.values()
        if "id" in proj and "name" in proj
    ]
    logger.debug(f"Processing {len(project_refs)} project references")
    
    # ADD "last_run_date" to pipeline definitions
    logger.debug("Adding last run dates to pipeline definitions")
    for definition in definitions:
        if "builds" in definition and isinstance(definition["builds"], dict) and "builds" in definition["builds"]:
            builds_list = definition["builds"]["builds"]
            if isinstance(builds_list, list) and len(builds_list) > 0:
                latest_build_id = max(builds_list)
                latest_build = next((b for b in builds if b.get("id") == latest_build_id), None)
                if latest_build:
                    definition["last_run_date"] = {
                        "id": latest_build.get("id"),
                        "queueTime": latest_build.get("queueTime"),
                        "startTime": latest_build.get("startTime"),
                        "finishTime": latest_build.get("finishTime"),
                    }

    # Filter builds
    logger.info("Applying filters to scan results...")
    filtered_builds = filter_builds(builds)
    logger.debug(f"Filtered builds: {len(builds)} -> {len(filtered_builds)}")

    # Filter build definitions
    filtered_definitions = filter_definitions(definitions)
    logger.debug(f"Filtered definitions: {len(definitions)} -> {len(filtered_definitions)}")

    # Filter protected resources for each type
    filtered_protected_resources = {}
    for res_type, res_data in protected_resources_inventory_resources_checks_definitions.items():
        if "protected_resources" in res_data and isinstance(res_data["protected_resources"], list):
            original_count = len(res_data["protected_resources"])
            res_data["protected_resources"] = filter_protected_resources(res_data["protected_resources"])
            logger.debug(f"Filtered {res_type}: {original_count} -> {len(res_data['protected_resources'])} resources")
        filtered_protected_resources[res_type] = res_data

    result = {
        "scanner_version": scanner_version,
        "id": organization,
        "scan_start": start_date,
        "scan_end": datetime.now().isoformat(),
        "organisation": {
            "id": organization,
            "name": organization,
            "url": os.environ.get("SYSTEM_COLLECTIONURI", f"https://dev.azure.com/{organization}"),
            "type": "AzureDevOps",
            "owner": "unknown",
            "shadow_color": "0, 0, 0",
            "partial_scan": True if projects else False,
            "projects_filter": projects if projects else [],
            "projectRefs": project_refs,
            "resource_counts": {
                "projects": len(az_manager.projects),
                "pools": len(filtered_protected_resources["pools"]["protected_resources"]),
                "queue": len(filtered_protected_resources["queue"]["protected_resources"]),
                "endpoint": len(filtered_protected_resources["endpoint"]["protected_resources"]),
                "variablegroup": len(filtered_protected_resources["variablegroup"]["protected_resources"]),
                "securefile": len(filtered_protected_resources["securefile"]["protected_resources"]),
                "repository": len(filtered_protected_resources["repository"]["protected_resources"]),
                "environment": len(filtered_protected_resources["environment"]["protected_resources"]),
                "pipelines": len(filtered_definitions),
                "builds": len(filtered_builds),
                "commits": len(commits),
                "committers": len(committer_stats),
                "artifacts_feeds": len(artifacts["active"]) if "active" in artifacts else 0
                + len(artifacts["recyclebin"])
                if "recyclebin" in artifacts
                else 0,
                "artifacts_packages": sum(len(feed.get("packages", [])) for feed in artifacts.get("active", []))
                if "active" in artifacts
                else 0,
            },
        },
        "stats": stats,
        "projects": az_manager.projects,
        "protected_resources": filtered_protected_resources,
        "build_definitions": filtered_definitions,
        "builds": filtered_builds,
        "tasks": tasks,
        "commits": commits,
        "committer_stats": committer_stats,
        "build_service_accounts": build_service_accounts,
        "artifacts": artifacts,
    }

    # Optional: Resolve cloud identities for service connections, variable groups, secure files
    # This step is fault-tolerant - if it fails, the scan continues without identity data
    if resolve_identities:
        logger.info("Resolving cloud identities (Entra ID, GCP)...")
        identity_service = IdentityResolutionService(enabled=True)
        if identity_service.is_available:
            result = identity_service.resolve_identities(
                result, 
                resolve=identity_resolution_resolve
            )
            status = result.get('_identity_resolution', {}).get('status', 'unknown')
            logger.info(f"Identity resolution complete: {status}")
            logger.debug(f"Identity resolution details: {result.get('_identity_resolution', {})}")
        else:
            logger.warning("Identity resolution not available (laughing-lamp not installed)")

    logger.info("Writing scan results...")
    output_path = write_scan_result(result, results_dir=results_dir, job_id=job_id)
    html_report_path = write_html_report(result, results_dir=results_dir, job_id=job_id, config=config)
    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    logger.debug(f"Scan result file size: {file_size_mb:.2f} MB")
    
    if hasattr(az_manager, "log_perf_summary"):
        az_manager.log_perf_summary()
    
    logger.info(f"Scan complete. Report: {html_report_path}")
    return result, output_path


def scan_azdevops(config, scanner_version: str):
    try:
        result, output_path = run_scan(config=config, scanner_version=scanner_version)
        return result, output_path
    except Exception as e:
        logger.error(f"Scan failed: {e}", exc_info=True)
        red = "\033[91m"
        reset = "\033[0m"
        print(f"{red}Error during scan: {e}{reset}", file=sys.stderr)
        sys.exit(1)
