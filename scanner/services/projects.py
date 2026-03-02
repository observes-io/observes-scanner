#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

from concurrent.futures import ThreadPoolExecutor, as_completed

from scanner.services.runtime import normalize_to_list


class ProjectsService:
    def __init__(self, manager, http_ops, logger):
        self.manager = manager
        self.http_ops = http_ops
        self.logger = logger

    def get_project_build_general_settings(self, project):
        url = f"https://dev.azure.com/{self.manager.organization}/{project}/_apis/build/generalsettings?api-version=7.1"
        try:
            self.logger.debug(f"Fetching general settings for project {project}")
            general_settings = self.http_ops.fetch_data(url)
            if general_settings:
                self.logger.debug(f"Retrieved general settings for project {project}")
            else:
                self.logger.debug(f"Failed to retrieve general settings for project {project}")
            return general_settings
        except Exception as e:
            self.logger.warning(f"Error fetching general settings for project {project}: {e}")
            return None

    def get_project_build_metrics(self, project, metric_aggregation_type="hourly"):
        url = f"https://dev.azure.com/{self.manager.organization}/{project}/_apis/build/metrics/{metric_aggregation_type}?api-version=7.1-preview.1"
        try:
            self.logger.debug(f"Fetching build metrics for project {project} ({metric_aggregation_type})")
            build_metrics = self.http_ops.fetch_data(url)
            if build_metrics:
                self.logger.debug(f"Retrieved build metrics for project {project}")
            else:
                self.logger.debug(f"Failed to retrieve build metrics for project {project}")
            return build_metrics
        except Exception as e:
            self.logger.warning(f"Error fetching build metrics for project {project}: {e}")
            return None

    def _fetch_project_settings(self, project):
        project_id = project["id"]
        general_settings = self.get_project_build_general_settings(project_id)
        build_metrics = self.get_project_build_metrics(project_id)
        return project_id, general_settings, build_metrics

    def get_projects(self, api_endpoint="projects", api_version="?api-version=7.1-preview.4", project_filter=None):
        url = f"https://dev.azure.com/{self.manager.organization}/_apis/{api_endpoint}{api_version}"
        url_deleted = f"https://dev.azure.com/{self.manager.organization}/_apis/{api_endpoint}{api_version}&stateFilter=deleted"
        self.logger.debug("Discovering projects")
        self.manager.projects = {}

        active_projects = normalize_to_list(self.http_ops.fetch_data(url))
        deleted_projects = normalize_to_list(self.http_ops.fetch_data(url_deleted))
        all_projects = active_projects + deleted_projects

        if project_filter:
            pf_lc = {str(project_value).lower() for project_value in project_filter}
            selected_projects = []
            for project in all_projects:
                project_id = str(project.get("id", "")).lower()
                project_name = str(project.get("name", "")).lower()
                if project_id in pf_lc or project_name in pf_lc:
                    selected_projects.append(project)
                else:
                    self.logger.debug(f"Project {project.get('name')} ({project.get('id')}) is NOT in the filter")
        else:
            selected_projects = list(all_projects)

        for project in selected_projects:
            project_id = project.get("id")
            if project_id:
                self.manager.projects[project_id] = project

        eligible_projects = [project for project in selected_projects if project.get("state") != "deleted" and project.get("id")]
        with ThreadPoolExecutor(max_workers=4) as pool:
            future_to_pid = {pool.submit(self._fetch_project_settings, project): project["id"] for project in eligible_projects}
            for future in as_completed(future_to_pid):
                project_id = future_to_pid[future]
                try:
                    _, build_general_settings, build_metrics = future.result()
                except Exception as err:
                    self.logger.warning(f"Error fetching settings for project {project_id}: {err}")
                    build_general_settings = None
                    build_metrics = None

                if project_id not in self.manager.projects:
                    continue
                self.manager.projects[project_id]["general_settings"] = {
                    "build_settings": build_general_settings if isinstance(build_general_settings, dict) else {},
                    "build_metrics": build_metrics,
                }
                for expected_key, expected_value in self.manager.default_build_settings_expectations.items():
                    found_value = self.manager.projects[project_id]["general_settings"]["build_settings"].get(expected_key)
                    self.manager.projects[project_id]["general_settings"]["build_settings"][expected_key] = {
                        "expected": expected_value,
                        "found": found_value,
                    }

        for project in selected_projects:
            if project.get("state") == "deleted":
                self.logger.debug(f"Project {project.get('name')} ({project.get('id')}) is DELETED")

        return self.manager.projects

    def get_project_language_metrics(self, projects):
        stats = {}
        for project in projects:
            project_id = project.get("id")
            project_name = project.get("name")
            url = f"https://dev.azure.com/{self.manager.organization}/{project_name}/_apis/projectanalysis/languagemetrics?api-version=6.0-preview.1"
            try:
                language_stats = self.http_ops.fetch_data(url)
                stats[project_id] = {"language_stats": language_stats}
            except Exception as err:
                self.logger.warning(f"An error occurred while retrieving metrics for project {project_name}: {err}")
        return stats
