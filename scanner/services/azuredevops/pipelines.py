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

import yaml

from scanner.services.runtime import normalize_to_list
from scanner.services.azuredevops.sast_service import SastService
from scanner.services.azuredevops.pipeline_parser import AzureDevOpsPipelineParser
from scanner.services.azuredevops.pipeline_graph.log_parser import EvaluationLogParser
from scanner.services.runtime import normalize_to_list

logger = logging.getLogger(__name__)


class PipelinesService:
    def __init__(self, manager, http_ops, runtime_state, observes_platform_service=None, skip_sast=False):
        self.manager = manager
        self.http_ops = http_ops
        self.runtime_state = runtime_state
        self.sast_service = SastService(
            api_client=observes_platform_service,
            exceptions=getattr(manager, 'exceptions', []),
            skip=skip_sast
        )

    def parse_pipeline_yaml(self, yaml_content, metadata=None):
        """
        Returns:
            dict: Parsed YAML (current) or structured pipeline representation
        """
        if not yaml_content:
            logger.debug("No YAML content provided")
            return None
        
        try:
            parsed_yaml = yaml.safe_load(yaml_content)
            return parsed_yaml
            
        except yaml.YAMLError as e:
            logger.warning(f"Error parsing YAML: {e}")
            return None

    def _build_pipeline_metadata(self, build_definition, project_id, branch_name=None):
        """
        Build metadata dict for structured pipeline parsing.
        
        Args:
            build_definition: Build definition dict from API
            project_id: Project GUID
            branch_name: Optional branch name for preview context
        
        Returns:
            dict: Metadata for pipeline parser
        """
        repository_info = build_definition.get("repository", {})
        project = self.manager.projects.get(project_id, {})
        
        metadata = {
            "structured": True,  # Enable structured parsing
            "project_id": project_id,
            "project_name": project.get("name") if isinstance(project, dict) else project_id,
            "pipeline_id": str(build_definition.get("id")),
            "pipeline_name": build_definition.get("name"),
            "pipeline_url": build_definition.get("_links", {}).get("self", {}).get("href"),
            "pipeline_type": "yaml" if build_definition.get("process", {}).get("type") == 2 else "designer",
            "pipeline_status": "enabled" if build_definition.get("queueStatus") != "disabled" else "disabled"
        }
        
        # Add repository info if available
        if repository_info:
            metadata["repository"] = {
                "id": repository_info.get("id"),
                "type": repository_info.get("type"),
                "name": repository_info.get("name"),
                "url": repository_info.get("url"),
                "ref": repository_info.get("defaultBranch", "refs/heads/main"),
                "commit": repository_info.get("checkoutSubmodules")  # This might not be right, adjust as needed
            }
            
            # Extract YAML path from process if available
            process = build_definition.get("process", {})
            if process.get("yamlFilename"):
                metadata["yaml_path"] = process.get("yamlFilename")
        
        # Add branch context if provided
        if branch_name:
            metadata["branch"] = branch_name
        
        return metadata

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

    def _fetch_build_logs(self, build, manager_pipeline):
        base_url = f"https://dev.azure.com/{self.manager.organization}/{build['project']['id']}/{manager_pipeline['builds']['api_endpoint']}/{build['id']}"
        api_ver = manager_pipeline['builds']['api_version']
        yaml_url = f"{base_url}/logs/1?{api_ver}"
        yaml_content = self.http_ops.fetch_data(yaml_url, qret=True)
        template_eval = self.http_ops.fetch_data(f"{base_url}/logs/2?{api_ver}", qret=True)
        timeline = self.http_ops.fetch_data(f"{base_url}/Timeline?{api_ver}")
        return build["id"], yaml_content, yaml_url, template_eval, timeline

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

            yaml_results = {}
            template_eval_results = {}
            timeline_results = {}
            with ThreadPoolExecutor(max_workers=4) as pool:
                future_map = {pool.submit(self._fetch_build_logs, build, manager_pipeline): str(build.get("id", idx)) for idx, build in enumerate(builds)}
                yaml_url_results = {}
                for future in as_completed(future_map):
                    build_key = future_map[future]
                    try:
                        build_id, yaml_content, yaml_url, template_eval, timeline = future.result()
                        yaml_results[build_id] = yaml_content
                        yaml_url_results[build_id] = yaml_url
                        template_eval_results[build_id] = template_eval
                        timeline_results[build_id] = timeline
                    except Exception as err:
                        logger.warning(f"Could not get YAML for build {build_key}: {err}")
                        yaml_results[build_key] = None
                        yaml_url_results[build_key] = None
                        template_eval_results[build_key] = None
                        timeline_results[build_key] = None

            processed_builds = []
            eval_log_parser = EvaluationLogParser()
            for build in builds:
                logger.debug(f"Processing build {build.get('id')} for definition {build_definition.get('name')}")
                yaml_content = yaml_results.get(build.get("id"))
                template_eval_raw = template_eval_results.get(build.get("id"))
                build["cicd_sast"] = []
                try:
                    # Structured parsing of the rendered YAML (logs/1)
                    metadata = self._build_pipeline_metadata(enriched_build_definition, project)
                    pipeline_recipe = self.parse_pipeline_yaml(yaml_content, metadata=metadata)

                    # Parse template evaluation log (logs/2) and enrich the
                    # structured pipeline_recipe with condition evaluations
                    if template_eval_raw and pipeline_recipe and isinstance(pipeline_recipe, dict):
                        eval_summary = eval_log_parser.parse(template_eval_raw)
                        parser = AzureDevOpsPipelineParser(organization=self.manager.organization)
                        pipeline_recipe = parser.enrich_with_template_evaluation(pipeline_recipe, eval_summary)

                    # Enrich with timeline execution status and templateParameters
                    timeline_data = timeline_results.get(build.get("id"))
                    template_params = build.get("templateParameters")
                    if isinstance(template_params, str):
                        try:
                            template_params = json.loads(template_params)
                        except Exception:
                            template_params = {}

                    # No OP
                    # if pipeline_recipe and isinstance(pipeline_recipe, dict):
                    #     parser = AzureDevOpsPipelineParser(organization=self.manager.organization)
                    #     pipeline_recipe = parser.enrich_with_timeline(pipeline_recipe, timeline_data, template_params)
                    #     if pipeline_recipe.get("snapshotId"):
                    #         pipeline_recipe = AzureDevOpsPipelineParser.finalize_recipe(pipeline_recipe, self.pir_registry, yaml_content=yaml_content)

                    build["pipeline_recipe"] = pipeline_recipe
                    if yaml_content is not None:
                        build["yaml_url"] = yaml_url_results.get(build.get("id"))
                        if not self.sast_service._skip:
                            regex_results = self.sast_service._scan_content(yaml_content, snapshot_id=build["k_key"], engine="regex")
                            if regex_results:
                                build["cicd_sast"].append({"engine": "regex", "scope": "pipeline_yaml", "results": regex_results})
                        enriched_build_definition["builds"]["builds"].append(str(build.get("id")))
                    processed_builds.append(build)
                except Exception as err:
                    logger.warning(f"Could not parse YAML for build {build.get('id')} for build definition {build_definition.get('name')}: {err}")
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
                        branch_result = {"is_yaml_preview_available": False, "cicd_sast": [], "yaml_url": None, "pipeline_recipe": None}

                        if build_definition.get("queueStatus") == "disabled":
                            branch_result["yaml_url"] = None
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

                        yaml_url = f"https://dev.azure.com/{self.manager.organization}/{project}/{manager_pipeline['build_definitions']['api_endpoint']}/{enriched_build_definition['id']}/yaml?{manager_pipeline['build_definitions']['api_version']}"
                        if preview is not None:
                            if preview == {}:
                                yaml_preview = self.http_ops.fetch_data(yaml_url, qret=True)
                                try:
                                    should_parse_yaml = isinstance(yaml_preview, str) or (
                                        isinstance(yaml_preview, dict)
                                        and yaml_preview.get("message", "") != f"Build pipeline {str(enriched_build_definition['id'])} is not designer."
                                    )
                                    if should_parse_yaml and yaml_preview:
                                        yaml_preview_json = json.loads(yaml_preview) if isinstance(yaml_preview, str) else yaml_preview
                                        yaml_content = yaml_preview_json.get("yaml", "")
                                        branch_result["yaml_url"] = yaml_url
                                        branch_result["pipeline_recipe"] = self.parse_pipeline_yaml(yaml_content)
                                        if not self.sast_service._skip:
                                            regex_results = self.sast_service._scan_content(yaml_content, snapshot_id=branch_name, engine="regex")
                                            if regex_results:
                                                branch_result["cicd_sast"].append({"engine": "regex", "scope": "pipeline_yaml", "results": regex_results})

                                        branch_result["is_yaml_preview_available"] = True
                                    else:
                                        branch_result["yaml_url"] = yaml_url
                                        branch_result["pipeline_recipe"] = self.parse_pipeline_yaml(preview)
                                except Exception as err:
                                    branch_result["yaml_url"] = yaml_url
                            else:
                                branch_result["yaml_url"] = yaml_url
                                # To enable structured parsing for preview:
                                metadata = self._build_pipeline_metadata(enriched_build_definition, project, branch_name)
                                final_yaml = preview.get("finalYaml")
                                preview_recipe = self.parse_pipeline_yaml(final_yaml, metadata=metadata)
                                # if preview_recipe and isinstance(preview_recipe, dict) and preview_recipe.get("snapshotId"):
                                #     preview_recipe = AzureDevOpsPipelineParser.finalize_recipe(preview_recipe, self.pir_registry, yaml_content=final_yaml)
                                if not self.sast_service._skip:
                                    regex_results = self.sast_service._scan_content(final_yaml, snapshot_id=branch_name, engine="regex")
                                    if regex_results:
                                        branch_result["cicd_sast"].append({"engine": "regex", "scope": "pipeline_yaml", "results": regex_results})
                                
                                branch_result["pipeline_recipe"] = preview_recipe
                                branch_result["is_yaml_preview_available"] = True
   
                        else:
                            branch_result["yaml_url"] = yaml_url
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
                            {"is_yaml_preview_available": False, "yaml_url": None, "pipeline_recipe": None},
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
