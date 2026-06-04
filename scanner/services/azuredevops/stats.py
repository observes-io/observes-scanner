#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

import logging
from collections import defaultdict

from scanner.services.runtime import extract_owner_project_id

logger = logging.getLogger(__name__)


class StatsService:
    def __init__(self, manager):
        self.manager = manager

    def get_enriched_stats(self, stats, resource_inventory, definitions, builds, commits, artifacts):
        logger.debug("Enriching stats with resource counts")
        idx = self.manager._build_runtime_indexes(definitions, builds)

        commit_counts = defaultdict(int)
        unique_committers = defaultdict(set)
        for commit in commits:
            project_id = commit.get("k_project", {}).get("id") if isinstance(commit.get("k_project"), dict) else None
            if not project_id:
                continue
            commit_counts[project_id] += 1
            if commit.get("committerEmail"):
                unique_committers[project_id].add(commit["committerEmail"])

        endpoint_counts = defaultdict(int)
        for resource in resource_inventory["endpoint"]["protected_resources"]:
            for reference in resource["resource"].get("k_projects_refs", []):
                project_id = reference.get("id")
                if project_id:
                    endpoint_counts[project_id] += 1

        simple_counts = {}
        for key in ["variablegroup", "securefile", "queue", "repository", "environment"]:
            counter = defaultdict(int)
            for resource in resource_inventory[key]["protected_resources"]:
                project_id = extract_owner_project_id(resource["resource"])
                if project_id:
                    counter[project_id] += 1
            simple_counts[key] = counter

        definitions_by_project = {project_id: len(keys) for project_id, keys in idx.definition_keys_by_project_id.items()}
        builds_by_project = defaultdict(int)
        for build in builds:
            project_id = build.get("k_project", {}).get("id") if isinstance(build.get("k_project"), dict) else None
            if project_id:
                builds_by_project[project_id] += 1

        artifacts_feeds_by_project = defaultdict(int)
        artifacts_packages_by_project = defaultdict(int)
        projects_in_stats = set(stats.keys())

        for feed in artifacts.get("active", []):
            k_proj = feed.get("k_project")
            if k_proj and k_proj.get("id") in projects_in_stats:
                project_id = k_proj["id"]
                artifacts_feeds_by_project[project_id] += 1
                packages = feed.get("packages", [])
                if isinstance(packages, list):
                    artifacts_packages_by_project[project_id] += len(packages)
            elif not k_proj:
                for project_id in projects_in_stats:
                    artifacts_feeds_by_project[project_id] += 1
                    packages = feed.get("packages", [])
                    if isinstance(packages, list):
                        artifacts_packages_by_project[project_id] += len(packages)

        for feed in artifacts.get("recyclebin", []):
            k_proj = feed.get("k_project")
            if k_proj and k_proj.get("id") in projects_in_stats:
                artifacts_feeds_by_project[k_proj["id"]] += 1
            elif not k_proj:
                for project_id in projects_in_stats:
                    artifacts_feeds_by_project[project_id] += 1

        for project in stats:
            logger.debug(f"Processing stats for project: {project}")
            if "resource_counts" not in stats[project]:
                stats[project]["resource_counts"] = {}
            stats[project]["resource_counts"]["pipelines"] = definitions_by_project.get(project, 0)
            stats[project]["resource_counts"]["builds"] = builds_by_project.get(project, 0)
            stats[project]["resource_counts"]["endpoint"] = endpoint_counts.get(project, 0)
            stats[project]["resource_counts"]["variablegroup"] = simple_counts["variablegroup"].get(project, 0)
            stats[project]["resource_counts"]["securefile"] = simple_counts["securefile"].get(project, 0)
            stats[project]["resource_counts"]["queue"] = simple_counts["queue"].get(project, 0)
            stats[project]["resource_counts"]["repository"] = simple_counts["repository"].get(project, 0)
            stats[project]["resource_counts"]["environment"] = simple_counts["environment"].get(project, 0)
            stats[project]["resource_counts"]["commits"] = commit_counts.get(project, 0)
            stats[project]["resource_counts"]["unique_committers"] = len(unique_committers.get(project, set()))
            stats[project]["resource_counts"]["artifacts_feeds"] = artifacts_feeds_by_project.get(project, 0)
            stats[project]["resource_counts"]["artifacts_packages"] = artifacts_packages_by_project.get(project, 0)

        return stats
