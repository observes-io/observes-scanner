#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

import base64
from datetime import datetime
import logging

from scanner.services import (
    ArtifactsService,
    IdentitiesService,
    PipelinesService,
    ProjectsService,
    RepositoriesService,
    ResourcesService,
    StatsService,
    TasksService,
)
from scanner.services.http_ops import HttpOps
from scanner.services.runtime import RuntimeIndexes, ScanRuntimeState, ordered_dedupe


class AzureDevOpsManager:
    def __init__(
        self,
        organization,
        project_filter,
        pat_token,
        default_build_settings_expectations={},
        branch_limit=5,
        exception_strings=False,
    ):
        self.organization = organization
        self.token = base64.b64encode(f":{pat_token}".encode()).decode()
        self.default_build_settings_expectations = default_build_settings_expectations or {}
        self.exceptions = exception_strings if exception_strings else [f"/dev.azure.com/{organization}/"]
        self.scan_start_time = datetime.now()
        self.scan_finish_time = None
        self.projects = {}
        self.repo_scan = {"top": branch_limit}

        self.logger = logging.getLogger("gunicorn.error")
        self.runtime_state = ScanRuntimeState()
        self.http_ops = HttpOps(token=self.token, runtime_state=self.runtime_state, logger=self.logger)

        self.projects_service = ProjectsService(manager=self, http_ops=self.http_ops, logger=self.logger)
        self.pipelines_service = PipelinesService(manager=self, http_ops=self.http_ops, runtime_state=self.runtime_state)
        self.resources_service = ResourcesService(manager=self, http_ops=self.http_ops, logger=self.logger)
        self.repositories_service = RepositoriesService(manager=self, http_ops=self.http_ops, runtime_state=self.runtime_state, logger=self.logger)
        self.artifacts_service = ArtifactsService(manager=self, http_ops=self.http_ops, logger=self.logger)
        self.stats_service = StatsService(manager=self)
        self.tasks_service = TasksService(manager=self, http_ops=self.http_ops)
        self.identities_service = IdentitiesService(manager=self, http_ops=self.http_ops)

        self.projects = self.get_projects(project_filter=project_filter)


    def get_endpoint_execution_history(self, endpoint_id, project_id=None):
        """
        Fetch execution history for a given service endpoint.
        If project_id is provided, use project-scoped URL, else use org-scoped.
        Returns a list of execution history records or an empty list.
        """
        if project_id:
            url = f"https://dev.azure.com/{self.organization}/{project_id}/_apis/serviceendpoint/{endpoint_id}/executionhistory?api-version=7.1"
        else:
            url = f"https://dev.azure.com/{self.organization}/_apis/serviceendpoint/{endpoint_id}/executionhistory?api-version=7.1"
        try:
            data = self.http_ops.fetch_data(url)
            if isinstance(data, dict) and "value" in data:
                return data["value"]
            elif isinstance(data, list):
                return data
            else:
                return []
        except Exception as err:
            self.logger.warning(f"Failed to fetch execution history for endpoint {endpoint_id}: {err}")
            return []

    def _build_runtime_indexes(self, definitions=None, builds=None):
        idx = RuntimeIndexes()
        for project_id, project_data in self.projects.items():
            if not isinstance(project_data, dict):
                continue
            if project_data.get("state", "").lower() == "wellformed":
                idx.wellformed_project_ids.append(project_id)
            project_name = project_data.get("name")
            if project_name:
                idx.project_id_by_project_name[project_name] = project_id

        definitions = definitions or []
        builds = builds or []
        for definition in definitions:
            if not isinstance(definition, dict):
                continue
            k_key = definition.get("k_key")
            if not k_key or "_" not in k_key:
                continue
            project_id = definition.get("k_project", {}).get("id") or k_key.split("_", 1)[0]
            idx.definitions_by_project_id.setdefault(project_id, []).append(definition)
            idx.definition_keys_by_project_id.setdefault(project_id, []).append(k_key)
            repo_id = definition.get("repository", {}).get("id")
            if repo_id:
                idx.definitions_by_repo_id.setdefault(repo_id, []).append(definition)

        for build in builds:
            if not isinstance(build, dict):
                continue
            k_key = build.get("k_key")
            if k_key and "_" in k_key:
                project_id = build.get("k_project", {}).get("id") or k_key.split("_", 1)[0]
                definition_id = build.get("definition", {}).get("id")
                if definition_id is not None:
                    def_key = f"{project_id}_{definition_id}"
                    idx.builds_by_definition_key.setdefault(def_key, []).append(build)
            repo_id = build.get("repository", {}).get("id")
            if repo_id:
                idx.builds_by_repo_id.setdefault(repo_id, []).append(build)

        for project_id, keys in list(idx.definition_keys_by_project_id.items()):
            idx.definition_keys_by_project_id[project_id] = ordered_dedupe(keys)

        self.runtime_state.indexes = idx
        return idx

    def _wellformed_project_ids(self):
        if self.runtime_state.indexes and self.runtime_state.indexes.wellformed_project_ids:
            return list(self.runtime_state.indexes.wellformed_project_ids)
        return [
            project_id
            for project_id, project_data in self.projects.items()
            if isinstance(project_data, dict) and project_data.get("state", "").lower() == "wellformed"
        ]

    def log_perf_summary(self):
        self.http_ops.log_perf_summary()

    def get_feed_packages(self, feed_id, project_id=None):
        return self.artifacts_service.get_feed_packages(feed_id, project_id=project_id)

    def get_feed_views(self, feed_id, project_id=None):
        return self.artifacts_service.get_feed_views(feed_id, project_id=project_id)

    def get_artifacts_feeds(self):
        return self.artifacts_service.get_artifacts_feeds()

    def enrich_repositories_with_committer_stats(self, protected_resources, commits):
        return self.repositories_service.enrich_repositories_with_committer_stats(protected_resources, commits)

    def get_committer_stats(self, commits, build_service_accounts):
        return self.repositories_service.get_committer_stats(commits, build_service_accounts)

    def get_commits_per_repository(self, protected_resources):
        return self.repositories_service.get_commits_per_repository(protected_resources)

    def get_repository_pull_requests_count(self, project_id, repo_id):
        return self.repositories_service.get_repository_pull_requests_count(project_id, repo_id)

    def get_repository_commit_dates(self, project_id, repo_id):
        return self.repositories_service.get_repository_commit_dates(project_id, repo_id)

    def get_repository_branches(self, source_project_id, repo_id, project_name, repo_name, top_branches_to_scan, default_branch_name):
        return self.repositories_service.get_repository_branches(
            source_project_id,
            repo_id,
            project_name,
            repo_name,
            top_branches_to_scan,
            default_branch_name,
        )

    def get_projects(self, api_endpoint="projects", api_version="?api-version=7.1-preview.4", project_filter=None):
        return self.projects_service.get_projects(api_endpoint=api_endpoint, api_version=api_version, project_filter=project_filter)

    def get_checks_approvals(self, inventory):
        return self.resources_service.get_checks_approvals(inventory)

    def enrich_resource_protection_and_cross_project(self, inventory):
        return self.resources_service.enrich_resource_protection_and_cross_project(inventory)

    def get_permissions(self, inventory, all_definitions, builds):
        return self.resources_service.get_permissions(inventory, all_definitions, builds)

    def scan_string_with_regex(self, string, engine, source_of_data):
        return self.pipelines_service.scan_string_with_regex(string, engine, source_of_data)

    def get_project_build_general_settings(self, project):
        return self.projects_service.get_project_build_general_settings(project)

    def get_project_build_metrics(self, project, metric_aggregation_type="hourly"):
        return self.projects_service.get_project_build_metrics(project, metric_aggregation_type=metric_aggregation_type)

    def get_build_definition_metrics(self, build_definition_id):
        return self.pipelines_service.get_build_definition_metrics(build_definition_id)

    def get_builds_per_definition_per_project(self, manager_pipeline={"preview":{"api_version": "api-version=7.1", "api_endpoint": "_apis/pipelines"}, "builds":{"api_version": "api-version=7.1", "api_endpoint": "_apis/build/builds"}, "build_definitions":{"api_version": "api-version=7.1", "api_endpoint": "_apis/build/definitions"}}, top_branches_to_scan=0, skip_builds=False):
        return self.pipelines_service.get_builds_per_definition_per_project(
            manager_pipeline=manager_pipeline, top_branches_to_scan=top_branches_to_scan, skip_builds=skip_builds
        )

    def get_build_definition_authorised_resources(self, build_definitions, manager_pipeline={"preview":{"api_version": "api-version=7.1", "api_endpoint": "_apis/pipelines"}, "builds":{"api_version": "api-version=7.1", "api_endpoint": "_apis/build/builds"}, "build_definitions":{"api_version": "api-version=7.1", "resources_api_version": "api-version=7.2-preview.1", "api_endpoint": "_apis/build/definitions"}}):
        return self.pipelines_service.get_build_definition_authorised_resources(
            build_definitions=build_definitions, manager_pipeline=manager_pipeline
        )

    def get_enriched_stats(self, stats, resource_inventory, definitions, builds, commits, artifacts):
        return self.stats_service.get_enriched_stats(stats, resource_inventory, definitions, builds, commits, artifacts)

    def calculate_pool_pipeline_permissions(self, queues):
        return self.resources_service.calculate_pool_pipeline_permissions(queues)

    def merge_pools_and_queues(self, pools, queues):
        return self.resources_service.merge_pools_and_queues(pools, queues)

    def enrich_k_project(self, curr_project_id, self_attribute=None, current_project_name=None):
        return self.resources_service.enrich_k_project(curr_project_id, self_attribute=self_attribute, current_project_name=current_project_name)

    def enrich_protected_resources_projectinfo(self, resource_type, resource, curr_project_id):
        return self.resources_service.enrich_protected_resources_projectinfo(resource_type, resource, curr_project_id)

    def get_deployment_group_details(self, project_id, deployment_group):
        return self.resources_service.get_deployment_group_details(project_id, deployment_group)

    def get_protected_resources(self, inventory):
        return self.resources_service.get_protected_resources(inventory)

    def get_enriched_build_definitions(self, definitions, resource_inventory):
        return self.resources_service.get_enriched_build_definitions(definitions, resource_inventory)

    def parse_pipeline_yaml(self, yaml_content):
        return self.pipelines_service.parse_pipeline_yaml(yaml_content)

    def get_project_language_metrics(self, projects):
        return self.projects_service.get_project_language_metrics(projects)

    def get_k_shared_from_endpoint(self, resource):
        return self.resources_service.get_k_shared_from_endpoint(resource)

    def get_task_list(self):
        return self.tasks_service.get_task_list()

    def get_all_build_service_accounts(self):
        return self.identities_service.get_all_build_service_accounts()
