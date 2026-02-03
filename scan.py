#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

SCANNER_VERSION = "1.0.3"

from azuredevops import AzureDevOpsManager
import time
import psutil
import threading
import argparse
import sys
import shutil


import json
import logging
import os
from datetime import datetime
import re


def scan_azdevops(organization, job_id, pat_token=None, results_dir=None, 
                  #enable_secrets_scanner=False, 
                  projects=None,
                  top_branches_to_scan=0):
    if not organization:
        raise ValueError("Organization must be provided")
    if not job_id:
        raise ValueError("Job ID must be provided")
    if not pat_token:
        raise ValueError("Personal Access Token (PAT) must be provided")
    
    if results_dir is None:
        results_dir = os.getcwd()

    # Hardcoded starter_inventory for protected resources
    starter_inventory = {
        "endpoint": {
            "api_endpoint": "serviceendpoint/endpoints",
            "api_version": "?api-version=7.1",
            "project_path": "serviceEndpointProjectReferences[].projectReference.id",
            "protected_resources": [],
            "level": "project"
        },
        "pools": {
            "api_endpoint": "distributedtask/pools",
            "api_version": "?api-version=7.1",
            "project_path": "projectId",
            "protected_resources": [],
            "level": "org"
        },
        "queue": {
            "api_endpoint": "distributedtask/queues",
            "api_version": "?api-version=7.1",
            "project_path": "projectId",
            "protected_resources": [],
            "level": "project"
        },
        "variablegroup": {
            "api_endpoint": "distributedtask/variablegroups",
            "api_version": "?api-version=7.1",
            "protected_resources": [],
            "level": "project"
        },
        "securefile": {
            "api_endpoint": "distributedtask/securefiles",
            "api_version": "?api-version=7.1",
            "protected_resources": [],
            "level": "project"
        },
        "repository": {
            "api_endpoint": "git/repositories",
            "api_version": "?api-version=7.1",
            "protected_resources": [],
            "level": "project"
        },
        # "deploymentgroups": {
        #     "api_endpoint": "distributedtask/deploymentgroups",
        #     "api_version": "?api-version=7.1",
        #     "protected_resources": [],
        #     "level": "project"
        # },
        # needs other permissions
        # "deploymentPoolsSummary": {
        #     "api_endpoint": "distributedtask/deploymentPools/deploymentPoolsSummary?expands=2",
        #     "api_version": "",
        #     "protected_resources": [],
        #     "level": "org"
        # },
        "environment": {
            "api_endpoint": "distributedtask/environments",
            "query_params": "expands=1",
            "api_version": "?api-version=7.1",
            "protected_resources": [],
            "level": "project"
        }
    }

    default_build_settings_expectations = {
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
        "publishPipelineMetadata": True
    }
    try:
        # # Detect if gitleaks is installed
        # gitleaks_installed = shutil.which("gitleaks") is not None

        # if not gitleaks_installed:
        #     gunicorn_logger = logging.getLogger('gunicorn.error')
        #     gunicorn_logger.warning("gitleaks is not installed, gitleaks scans will be skipped!")

        start_time = time.time()
        start_date = datetime.now().isoformat()
        # process = psutil.Process(os.getpid())
        # max_mem = [0]
        # max_cpu = [0]
        # monitoring = [True]
        # def monitor_resources():
        #     while monitoring[0]:
        #         mem = process.memory_info().rss
        #         cpu = process.cpu_percent(interval=0.1)
        #         if mem > max_mem[0]:
        #             max_mem[0] = mem
        #         if cpu > max_cpu[0]:
        #             max_cpu[0] = cpu
        #         time.sleep(0.1)
        # monitor_thread = threading.Thread(target=monitor_resources)
        # monitor_thread.start()
        try:
            az_manager = AzureDevOpsManager(
                organization=organization,
                project_filter=projects if projects else [],
                default_build_settings_expectations=default_build_settings_expectations,
                pat_token=pat_token,
                # gitleaks_installed=gitleaks_installed and enable_secrets_scanner
            )

            print(f"Starting scan for {organization} with job ID: {job_id}")

            # 0 ORG STATS - ignores expire date
            stats = az_manager.get_project_language_metrics(az_manager.projects.values())

            # 0.1 Task Discovery
            tasks = az_manager.get_task_list()

            # 1 PIPELINE DISCOVERY
            definitions, builds = az_manager.get_builds_per_definition_per_project(top_branches_to_scan=top_branches_to_scan)

            # 1.1 ENRICH WITH LOG SCAN
            # for build in builds:
            #     if enable_secrets_scanner:
            #          build = az_manager.get_enriched_build_with_log_secret_scan(build, gitleaks_installed=gitleaks_installed)
            
            # 1.2 PIPELINE PERMISSIONS
            definitions = az_manager.get_build_definition_authorised_resources(definitions)


            # 2 PROTECTED RESOURCES DISCOVERY
            protected_resources_inventory_resources = az_manager.get_protected_resources(starter_inventory)
            
            # 2.1 APPROVALS & CHECKS
            protected_resources_inventory_resources_checks = az_manager.get_checks_approvals(protected_resources_inventory_resources)
            
            # 2.2 PIPELINE PERMISSIONS x PROTECTED RESOURCE
            protected_resources_inventory_resources_checks_definitions = az_manager.get_permissions(protected_resources_inventory_resources_checks, definitions, builds)

            # 2.2.1 GET CROSS PROJECT & PROTECTION STATUSES
            protected_resources_inventory_resources_checks_definitions = az_manager.enrich_resource_protection_and_cross_project(protected_resources_inventory_resources_checks_definitions)

            # 2.3 ENRICH PIPELINE DEFINITIONS with which resources they have access to
            definitions = az_manager.get_enriched_build_definitions(definitions, protected_resources_inventory_resources_checks_definitions)

            # 3. Get all build service accounts
            build_service_accounts = az_manager.get_all_build_service_accounts()

            # 3.1 Get Commits for each repository
            commits = az_manager.get_commits_per_repository(protected_resources_inventory_resources_checks_definitions["repository"]["protected_resources"])

            # 3.2 Get Commits for each unique committer
            committer_stats = az_manager.get_committer_stats(commits, build_service_accounts=build_service_accounts)

            # 3.3 Enrich repositories stats with committer counts and list of unique committers
            protected_resources_inventory_resources_checks_definitions["repository"]["protected_resources"] = az_manager.enrich_repositories_with_committer_stats(
                protected_resources_inventory_resources_checks_definitions["repository"]["protected_resources"],
                commits
            )
            
            # 4. Get Artifacts
            artifacts = az_manager.get_artifacts_feeds()

            # 5. Enrich stats with resource counts
            stats = az_manager.get_enriched_stats(stats, protected_resources_inventory_resources_checks_definitions, definitions, builds, commits, artifacts)


            project_refs = [
                {"id": proj["id"], "name": proj["name"]}
                for proj in az_manager.projects.values()
                if "id" in proj and "name" in proj
            ]
            result = {
                "scanner_version": SCANNER_VERSION,
                "id": organization,
                "scan_start": start_date,
                "scan_end": datetime.now().isoformat(),
                "organisation": {
                    "id": organization,
                    "name": organization,
                    "url": os.environ.get('SYSTEM_COLLECTIONURI', f"https://dev.azure.com/{organization}"),
                    "type": "AzureDevOps",
                    "owner": "unknown",
                    "shadow_color": "0, 0, 0",
                    "partial_scan": True if projects else False,
                    "projects_filter": projects if projects else [],
                    "projectRefs": project_refs,
                    "resource_counts": {
                        "projects": len(az_manager.projects),
                        "pools": len(protected_resources_inventory_resources_checks_definitions["pools"]["protected_resources"]),
                        "queue": len(protected_resources_inventory_resources_checks_definitions["queue"]["protected_resources"]),
                        "endpoint": len(protected_resources_inventory_resources_checks_definitions["endpoint"]["protected_resources"]),
                        "variablegroup": len(protected_resources_inventory_resources_checks_definitions["variablegroup"]["protected_resources"]),
                        "securefile": len(protected_resources_inventory_resources_checks_definitions["securefile"]["protected_resources"]),
                        "repository": len(protected_resources_inventory_resources_checks_definitions["repository"]["protected_resources"]),
                        "environment": len(protected_resources_inventory_resources_checks_definitions["environment"]["protected_resources"]),
                        "pipelines": len(definitions),
                        "builds": len(builds),
                        "commits": len(commits),
                        "committers": len(committer_stats),
                        "artifacts_feeds": len(artifacts["active"]) if "active" in artifacts else 0 + len(artifacts["recyclebin"]) if "recyclebin" in artifacts else 0,
                        "artifacts_packages": sum(len(feed.get("packages", [])) for feed in artifacts.get("active", [])) if "active" in artifacts else 0,
                    }
                },
                "stats": stats,
                "projects": az_manager.projects,
                "protected_resources": protected_resources_inventory_resources_checks_definitions,
                "build_definitions": definitions,
                "builds": builds,
                "tasks": tasks,
                "commits": commits,
                "committer_stats": committer_stats,
                "build_service_accounts": build_service_accounts,
                "artifacts": artifacts
            }

            # Save the results to a JSON file in the specified results directory
            if not os.path.exists(results_dir):
                os.makedirs(results_dir)
            results_dir = os.path.abspath(results_dir)

            safe_job_id = re.sub(r'[^a-zA-Z0-9_-]', '_', job_id)
            output_path = os.path.join(results_dir, f"scan_{safe_job_id}.json")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, 'w') as f:
                json.dump(result, f)
                f.close()

            # Print file sizes for results and metadata
            results_size = os.path.getsize(output_path)
            # green = "\033[92m"
            # reset = "\033[0m"
            # print(f"{green}Scan completed successfully. Results folder is {results_dir}{reset}")
            # print(f"{green}Results file saved to {output_path} ({format_size(results_size)}){reset}")
        except Exception as e:
            gunicorn_logger = logging.getLogger('gunicorn.error')
            gunicorn_logger.error(f"Error during scan: {e}")
            red = "\033[91m"
            reset = "\033[0m"
            print(f"{red}Error during scan: {e}{reset}", file=sys.stderr)
            sys.exit(1)
        finally:
            print("Scan finished.")
            # monitoring[0] = False
            # monitor_thread.join()
            # purple = "\033[95m"
            # reset = "\033[0m"
            # end_time = time.time()
            # duration = end_time - start_time
            # print(f"{purple}Scan duration: {duration:.2f} seconds{reset}")
            # print(f"{purple}Max memory usage: {max_mem[0] / (1024 * 1024):.2f} MB (RSS){reset}")
            # total_cores = psutil.cpu_count(logical=True)
            # cpu_percent_of_total = max_cpu[0] / (total_cores if total_cores else 1)
            # print(f"{purple}Max CPU usage: {cpu_percent_of_total:.2f}% of total system capacity, {total_cores} cores{reset}")
    except Exception as e:
        gunicorn_logger = logging.getLogger('gunicorn.error')
        gunicorn_logger.error(f"Error during scan: {e}")
        red = "\033[91m"
        reset = "\033[0m"
        print(f"{red}Error during scan: {e}{reset}", file=sys.stderr)
        sys.exit(1)

def format_size(size_bytes):
    if size_bytes >= 1024 ** 3:
        return f"{size_bytes / (1024 ** 3):.2f} GB"
    elif size_bytes >= 1024 ** 2:
        return f"{size_bytes / (1024 ** 2):.2f} MB"
    elif size_bytes >= 1024:
        return f"{size_bytes / 1024:.2f} KB"
    else:
        return f"{size_bytes} B"

def main():
    parser = argparse.ArgumentParser(description="Run Azure DevOps scan.")
    parser.add_argument('-o', '--organization', required=True, help='Azure DevOps organization name')
    parser.add_argument('-j', '--job-id', required=True, help='Job ID for this scan')
    parser.add_argument('-p', '--pat-token', required=False, help='Azure DevOps Personal Access Token (can also be set via AZURE_DEVOPS_PAT environment variable)')
    parser.add_argument('-r', '--results-dir', default=None, help='Directory to save scan results (default: current working directory)')
    parser.add_argument('-rb', '--top-branches-to-scan', type=int, default=5, help='Number of default plus top branches to scan for each repository. -1 for all branches, 0 for default branch only, >= X for default and X top branches (default: 5)')
    #parser.add_argument('--enable-secrets-scanner', action='store_true', help='Enable secrets scanner (default: disabled)')
    parser.add_argument('--projects', default=None, help='Optional comma separated list of project names or IDs to filter scan')
    args = parser.parse_args()

    # PAT token: CLI argument takes precedence, fallback to environment variable
    pat_token = args.pat_token or os.environ.get('AZURE_DEVOPS_PAT')
    if not pat_token:
        print("Error: Azure DevOps Personal Access Token must be provided via --pat-token or AZURE_DEVOPS_PAT environment variable.", file=sys.stderr)
        sys.exit(1)

    projects = [p.strip() for p in args.projects.split(',')] if args.projects else []
    scan_azdevops(
        organization=args.organization,
        job_id=args.job_id,
        pat_token=pat_token,
        results_dir=args.results_dir,
        #enable_secrets_scanner=args.enable_secrets_scanner,
        projects=projects,
        top_branches_to_scan=args.top_branches_to_scan
    )

if __name__ == "__main__":
    main()





