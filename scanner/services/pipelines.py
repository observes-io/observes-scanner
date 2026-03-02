#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

import json
import logging
import urllib.parse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

import yaml

from scanner.services.runtime import normalize_to_list

logger = logging.getLogger(__name__)


class PipelinesService:
    def __init__(self, manager, http_ops, runtime_state):
        self.manager = manager
        self.http_ops = http_ops
        self.runtime_state = runtime_state

    def parse_pipeline_yaml(self, yaml_content):
        if not yaml_content:
            logger.debug("No YAML content provided")
            return None
        try:
            return yaml.safe_load(yaml_content)
        except yaml.YAMLError as e:
            logger.warning(f"Error parsing YAML: {e}")
            return None

    def scan_string_with_regex(self, string, engine, source_of_data):
        if not self.runtime_state.regex_patterns_loaded:
            regex_cache = {}
            try:
                with open("datastore/scanners/patterns/cicd_sast.json", "r") as file:
                    patterns_data = json.load(file)
                    for current_engine, engine_data in patterns_data.items():
                        compiled_patterns = []
                        categories = engine_data.get("categories", [])
                        for category in categories:
                            category_name = category.get("name", "Unknown")
                            category_severity = category.get("severity", "unknown")
                            category_description = category.get("description", "")
                            for pattern in category.get("patterns", []):
                                try:
                                    compiled_pattern = re.compile(pattern)
                                    compiled_patterns.append({
                                        "pattern": compiled_pattern,
                                        "category": category_name,
                                        "severity": category_severity,
                                        "description": category_description
                                    })
                                except re.error as regex_error:
                                    logger.warning(f"Invalid regex pattern skipped for engine {current_engine}, category {category_name}: {regex_error}")
                        regex_cache[current_engine] = compiled_patterns
            except FileNotFoundError:
                logger.error("Regex patterns file not found. Please ensure 'patterns/cicd_sast.json' exists.")
            except json.JSONDecodeError:
                logger.error("Failed to parse the regex patterns file as JSON.")
            except Exception as e:
                logger.error(f"Error loading regex patterns: {e}")
            self.runtime_state.regex_patterns_cache = regex_cache
            self.runtime_state.regex_patterns_loaded = True

        compiled_patterns = self.runtime_state.regex_patterns_cache.get(engine, [])
        if not compiled_patterns:
            logger.debug(f"No patterns found for engine {engine}")
            return []

        findings = []
        for pattern_info in compiled_patterns:
            compiled_pattern = pattern_info["pattern"]
            for match in compiled_pattern.finditer(string):
                should_skip = False
                for exception in self.manager.exceptions:
                    if isinstance(match.group(), str) and exception in match.group():
                        logger.debug(f"Skipping match {match.group()} due to exception")
                        should_skip = True
                        break
                
                if not should_skip:
                    findings.append(
                        {
                            "source": source_of_data,
                            "match": match.group(),
                            "start": match.start(),
                            "end": match.end(),
                            "pattern": compiled_pattern.pattern,
                            "category": pattern_info["category"],
                            "severity": pattern_info["severity"],
                            "description": pattern_info["description"]
                        }
                    )
                    logger.debug(f"Found match: {match.group()} at {match.start()}-{match.end()} [Category: {pattern_info['category']}, Severity: {pattern_info['severity']}]")
        return findings

    def get_build_definition_metrics(self, build_definition_id):
        project, definition_id = build_definition_id.split("_")
        try:
            url = f"https://dev.azure.com/{self.manager.organization}/{project}/_apis/build/definitions/{definition_id}/metrics?api-version=7.1-preview.1"
            def_metrics = self.http_ops.fetch_data(url)
            if def_metrics:
                logger.debug(f"Retrieved def_metrics for project {project} / pipeline ID {definition_id}")
            else:
                logger.debug(f"Failed to retrieve def_metrics for project {project} / pipeline ID {definition_id}")
            return def_metrics
        except Exception as e:
            logger.warning(f"Error fetching def_metrics for project {project} / pipeline ID {definition_id}: {e}")
            return None

    def _process_build_definition(self, project, build_definition, project_name_to_id, manager_pipeline, top_branches_to_scan, skip_builds=False):
        
        specific_url = f"https://dev.azure.com/{self.manager.organization}/{project}/{manager_pipeline['build_definitions']['api_endpoint']}/{build_definition['id']}?{manager_pipeline['build_definitions']['api_version']}"

        enriched_build_definition = self.http_ops.fetch_data(specific_url)
        
        if not isinstance(enriched_build_definition, dict):
            logger.warning(f"Could not get build definition {build_definition.get('name')} for project {self.manager.projects[project]['name']}")
            return None, []

        enriched_build_definition["k_project"] = self.manager.enrich_k_project(project)
        enriched_build_definition["k_key"] = f"{project}_{build_definition['id']}"
        enriched_build_definition["builds"] = {
            "metrics": self.get_build_definition_metrics(build_definition_id=f"{project}_{build_definition['id']}"),
            "preview": {},
            "builds": [],
        }

        # Initialize variables that are used in return statement
        processed_builds = []
        builds = []

        if not skip_builds:
            builds_url = f"https://dev.azure.com/{self.manager.organization}/{project}/{manager_pipeline['builds']['api_endpoint']}?definitions={enriched_build_definition['id']}&{manager_pipeline['builds']['api_version']}"
            builds = normalize_to_list(self.http_ops.fetch_data(builds_url))
            logger.debug(f"{len(builds)} builds for build definition {build_definition.get('name')}")

            for build in builds:
                build["k_project"] = self.manager.enrich_k_project(project)
                build["k_key"] = f"{project}_{build.get('id')}"

            def _fetch_yaml(build):
                yaml_url = f"https://dev.azure.com/{self.manager.organization}/{project}/{manager_pipeline['builds']['api_endpoint']}/{build['id']}/logs/1?{manager_pipeline['builds']['api_version']}"
                yaml_content = self.http_ops.fetch_data(yaml_url, qret=True)
                return build["id"], yaml_content

            yaml_results = {}
            with ThreadPoolExecutor(max_workers=4) as pool:
                future_map = {pool.submit(_fetch_yaml, build): build["id"] for build in builds}
                for future in as_completed(future_map):
                    try:
                        build_id, yaml_content = future.result()
                        yaml_results[build_id] = yaml_content
                    except Exception as err:
                        logger.warning(f"Could not get YAML for build {future_map[future]}: {err}")
                        yaml_results[future_map[future]] = None

            processed_builds = []
            for build in builds:
                logger.debug(f"Processing build {build.get('id')} for definition {build_definition.get('name')}")
                yaml_content = yaml_results.get(build.get("id"))
                try:
                    pipeline_recipe = self.parse_pipeline_yaml(yaml_content)
                    build["pipeline_recipe"] = pipeline_recipe
                    if yaml_content is not None:
                        build["yaml"] = yaml_content
                        source_url = build.get("_links", {}).get("self", {}).get("href", "")
                        regex_results = self.scan_string_with_regex(yaml_content, "regex", source_url)
                        build.setdefault("cicd_sast", [])
                        if regex_results:
                            build["cicd_sast"].append({"engine": "regex", "scope": "pipeline_yaml", "results": regex_results})
                        enriched_build_definition["builds"]["builds"].append(str(build.get("id")))
                    processed_builds.append(build)
                except Exception:
                    logger.warning(f"Could not parse YAML for build {build.get('id')} for build definition {build_definition.get('name')}")
                    continue

            if manager_pipeline.get("preview"):
                preview_url = f"https://dev.azure.com/{self.manager.organization}/{project}/{manager_pipeline['preview']['api_endpoint']}/{build_definition['id']}/preview?{manager_pipeline['preview']['api_version']}"
                repository_info = enriched_build_definition.get("repository", {})
                repo_id = repository_info.get("id")
                repo_name = repository_info.get("name")
                default_branch = repository_info.get("defaultBranch", "refs/heads/main")

                repository_url = repository_info.get("url", "")
                project_name = repository_url.split("/")[4] if len(repository_url.split("/")) > 4 else self.manager.projects[project].get("name", "")
                decoded_string = urllib.parse.unquote(urllib.parse.unquote(project_name))
                source_project_id = project_name_to_id.get(decoded_string)

                branch_builds = defaultdict(list)
                for build in builds:
                    source_branch = build.get("sourceBranch")
                    if source_branch and build.get("finishTime"):
                        branch_builds[source_branch].append(build)

                if source_project_id is None:
                    logger.warning(
                        f"Project name {decoded_string} not found in projects. May need to increase scope of observability in config"
                    )
                else:
                    if top_branches_to_scan == 0:
                        branches_names = [default_branch.split("/")[-1]]
                    else:
                        _, branches_names = self.manager.get_repository_branches(
                            source_project_id,
                            repo_id,
                            project_name,
                            repo_name,
                            top_branches_to_scan,
                            default_branch.split("/")[-1],
                        )

                    def _preview_one_branch(branch_name):
                        branch_result = {"is_yaml_preview_available": False, "cicd_sast": []}

                        if build_definition.get("queueStatus") == "disabled":
                            branch_result["yaml"] = "Build Definition is disabled"
                            branch_result["pipeline_recipe"] = "Build Definition is disabled"
                            return branch_name, branch_result

                        process_info = enriched_build_definition.get("process", {})
                        if process_info.get("type") == 1:
                            process_info.pop("phases", None)
                            process_info.pop("target", None)
                            preview = {}
                            error_message = None
                        else:
                            payload_obj = {
                                "resources": {
                                    "pipelines": {},
                                    "repositories": {"self": {"refName": branch_name}},
                                    "builds": {},
                                    "containers": {},
                                    "packages": {},
                                },
                                "templateParameters": {},
                                "previewRun": True,
                                "yamlOverride": "",
                            }
                            branch_candidates = branch_builds.get(f"refs/heads/{branch_name}", [])
                            if branch_candidates:
                                latest_build = max(branch_candidates, key=lambda b: b["finishTime"])
                                template_params = latest_build.get("templateParameters", {})
                                if isinstance(template_params, str):
                                    try:
                                        template_params = json.loads(template_params)
                                    except Exception:
                                        template_params = {}
                                payload_obj["templateParameters"] = template_params
                                build_vars = latest_build.get("variables", {})
                                if build_vars:
                                    payload_obj["resources"]["builds"] = {
                                        "variables": {
                                            key: value.get("value")
                                            for key, value in build_vars.items()
                                            if isinstance(value, dict) and "value" in value
                                        }
                                    }
                            preview, error_message = self.http_ops.post_data(preview_url, json.dumps(payload_obj))

                        if preview is not None:
                            if preview == {}:
                                yaml_url = f"https://dev.azure.com/{self.manager.organization}/{project}/{manager_pipeline['build_definitions']['api_endpoint']}/{enriched_build_definition['id']}/yaml?{manager_pipeline['build_definitions']['api_version']}"
                                yaml_preview = self.http_ops.fetch_data(yaml_url, qret=True)
                                try:
                                    should_parse_yaml = isinstance(yaml_preview, str) or (
                                        isinstance(yaml_preview, dict)
                                        and yaml_preview.get("message", "") != f"Build pipeline {str(enriched_build_definition['id'])} is not designer."
                                    )
                                    if should_parse_yaml and yaml_preview:
                                        yaml_preview_json = json.loads(yaml_preview) if isinstance(yaml_preview, str) else yaml_preview
                                        yaml_content = yaml_preview_json.get("yaml", "")
                                        branch_result["yaml"] = yaml_content
                                        branch_result["pipeline_recipe"] = self.parse_pipeline_yaml(yaml_content)
                                        regex_results = self.scan_string_with_regex(
                                            yaml_preview if isinstance(yaml_preview, str) else json.dumps(yaml_preview),
                                            "regex",
                                            f"{branch_name} @ {enriched_build_definition.get('_links', {}).get('self', {}).get('href', '')}",
                                        )
                                        branch_result["cicd_sast"].append(
                                            {
                                                "engine": "regex",
                                                "scope": "potential_pipeline_execution_yaml",
                                                "results": regex_results,
                                            }
                                        )
                                        branch_result["is_yaml_preview_available"] = True
                                    else:
                                        branch_result["yaml"] = "Empty YAML PREVIEW"
                                        branch_result["pipeline_recipe"] = self.parse_pipeline_yaml(preview)
                                except Exception as err:
                                    branch_result["yaml"] = f"Could not parse YAML PREVIEW - {err}"
                                    branch_result["pipeline_recipe"] = None
                            else:
                                branch_result["yaml"] = preview.get("finalYaml")
                                branch_result["pipeline_recipe"] = self.parse_pipeline_yaml(preview.get("finalYaml"))
                                branch_result["is_yaml_preview_available"] = True
                                regex_results = self.scan_string_with_regex(
                                    preview.get("finalYaml", ""),
                                    "regex",
                                    f"{branch_name} @ {enriched_build_definition.get('_links', {}).get('self', {}).get('href', '')}",
                                )
                                branch_result["cicd_sast"].append(
                                    {
                                        "engine": "regex",
                                        "scope": "potential_pipeline_execution_yaml",
                                        "results": regex_results,
                                    }
                                )
                        else:
                            branch_result["yaml"] = f"Could not get YAML PREVIEW - {error_message}"
                            branch_result["cicd_sast"] = []
                            branch_result["is_yaml_preview_available"] = False
                        return branch_name, branch_result

                    with ThreadPoolExecutor(max_workers=4) as preview_pool:
                        preview_futures = [preview_pool.submit(_preview_one_branch, branch_name) for branch_name in branches_names]
                        preview_results = {}
                        for future in as_completed(preview_futures):
                            branch_name, branch_payload = future.result()
                            preview_results[branch_name] = branch_payload
                    for branch_name in branches_names:
                        enriched_build_definition["builds"]["preview"][branch_name] = preview_results.get(
                            branch_name,
                            {"is_yaml_preview_available": False, "yaml": "No preview", "pipeline_recipe": None, "cicd_sast": []},
                        )

        return enriched_build_definition, processed_builds

    def get_builds_per_definition_per_project(
        self,
        manager_pipeline={
            "preview": {"api_version": "api-version=7.1", "api_endpoint": "_apis/pipelines"},
            "builds": {"api_version": "api-version=7.1", "api_endpoint": "_apis/build/builds"},
            "build_definitions": {"api_version": "api-version=7.1", "api_endpoint": "_apis/build/definitions"},
        },
        top_branches_to_scan=0,
        skip_builds=False,
    ):
        logger.debug("Starting pipeline discovery")
        build_def_list = []
        builds_list = []
        project_name_to_id = {
            project_data.get("name"): project_id
            for project_id, project_data in self.manager.projects.items()
            if isinstance(project_data, dict)
        }

        for project in self.manager._wellformed_project_ids():
            url = f"https://dev.azure.com/{self.manager.organization}/{project}/{manager_pipeline['build_definitions']['api_endpoint']}?{manager_pipeline['build_definitions']['api_version']}"
            build_definitions = normalize_to_list(self.http_ops.fetch_data(url))
            logger.debug(f"{len(build_definitions)} build definitions for {self.manager.projects[project]['name']}")
            if not build_definitions:
                continue

            ordered_results = {}
            with ThreadPoolExecutor(max_workers=4) as pool:
                future_map = {
                    pool.submit(
                        self._process_build_definition,
                        project,
                        build_definition,
                        project_name_to_id,
                        manager_pipeline,
                        top_branches_to_scan,
                        skip_builds,
                    ): index
                    for index, build_definition in enumerate(build_definitions)
                }
                for future in as_completed(future_map):
                    index = future_map[future]
                    try:
                        ordered_results[index] = future.result()
                    except Exception as err:
                        logger.warning(f"Could not process build definition index {index} in project {project}: {err}")
                        ordered_results[index] = (None, [])

            for index in range(len(build_definitions)):
                definition_data, build_items = ordered_results.get(index, (None, []))
                if definition_data is not None:
                    build_def_list.append(definition_data)
                    builds_list.extend(build_items)

        self.manager._build_runtime_indexes(build_def_list, builds_list)

        return build_def_list, builds_list

    def get_build_definition_authorised_resources(
        self,
        build_definitions,
        manager_pipeline={
            "preview": {"api_version": "api-version=7.1", "api_endpoint": "_apis/pipelines"},
            "builds": {"api_version": "api-version=7.1", "api_endpoint": "_apis/build/builds"},
            "build_definitions": {
                "api_version": "api-version=7.1",
                "resources_api_version": "api-version=7.2-preview.1",
                "api_endpoint": "_apis/build/definitions",
            },
        },
    ):
        def _fetch_one(index, build_definition):
            project, build_definition_id = build_definition["k_key"].split("_")
            url = f"https://dev.azure.com/{self.manager.organization}/{project}/{manager_pipeline['build_definitions']['api_endpoint']}/{str(build_definition_id)}/resources?{manager_pipeline['build_definitions']['resources_api_version']}"
            authorized_resources = self.http_ops.fetch_data(url)
            return index, normalize_to_list(authorized_resources)

        results = {}
        with ThreadPoolExecutor(max_workers=4) as pool:
            future_map = {
                pool.submit(_fetch_one, index, build_definition): index
                for index, build_definition in enumerate(build_definitions)
            }
            for future in as_completed(future_map):
                index, resources = future.result()
                results[index] = resources

        for index, build_definition in enumerate(build_definitions):
            build_definition["resources"] = list(results.get(index, []))
        return build_definitions
