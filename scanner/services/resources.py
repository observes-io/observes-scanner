#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

import urllib.parse
from datetime import datetime, timedelta, timezone

from scanner.services.runtime import extract_owner_project_id, normalize_to_list, ordered_dedupe


class ResourcesService:
    def __init__(self, manager, http_ops, logger):
        self.manager = manager
        self.http_ops = http_ops
        self.logger = logger

    def attach_endpoint_last_used(self, inventory):
        """
        For each endpoint in the inventory, fetch and attach all executions and last-used info from execution history.
        Adds:
          - 'executions': list of all execution records (may be empty)
          - 'last_used': ISO8601 string or None
          - 'last_execution': latest execution record or None
        """
        for protected_resource in inventory.get("endpoint", {}).get("protected_resources", []):
            resource = protected_resource.get("resource", {})
            endpoint_id = resource.get("id")
            # Try to get project id from k_project or serviceEndpointProjectReferences
            project_id = None
            if "k_project" in resource and isinstance(resource["k_project"], dict):
                project_id = resource["k_project"].get("id")
            elif resource.get("serviceEndpointProjectReferences"):
                # Use the first project reference if available
                refs = resource["serviceEndpointProjectReferences"]
                if refs and isinstance(refs, list) and "projectReference" in refs[0]:
                    project_id = refs[0]["projectReference"].get("id")
            if endpoint_id:
                history = self.manager.get_endpoint_execution_history(endpoint_id, project_id)
                resource["executions"] = history or []
                if history:
                    # Sort by finishTime descending, fallback to startTime
                    history_sorted = sorted(history, key=lambda h: h["data"].get("finishTime") or h["data"].get("startTime"), reverse=True)
                    latest = history_sorted[0]["data"] if history_sorted else None
                    resource["last_execution"] = latest
                    resource["last_used"] = latest.get("finishTime") if latest and "finishTime" in latest else (
                        latest.get("startTime") if latest and "startTime" in latest else None
                    )
                else:
                    resource["last_execution"] = None
                    resource["last_used"] = None
                    resource["executions"] = []
        return inventory

    def attach_used_service_connections_to_builds(self, builds={}, endpoints=[]):
        """
        For each build, attach a list of used service connection IDs (endpoint IDs).
        Matches build['id'] to execution['data']['owner']['id'] in each endpoint's executions where planType == 'Build'.
        """
        # Build a map of endpoint_id -> executions for fast lookup
        endpoint_exec_map = {}
        for protected_resource in endpoints:
            resource = protected_resource.get("resource", {})
            endpoint_id = resource.get("id")
            executions = resource.get("executions", [])
            if endpoint_id:
                endpoint_exec_map[endpoint_id] = executions

        for build in builds:
            used_endpoints = set()
            build_id = build.get("id")
            for endpoint_id, executions in endpoint_exec_map.items():
                for execution in executions:
                    data = execution.get("data", {})
                    if data.get("planType") == "Build":
                        owner = data.get("owner", {})
                        if owner.get("id") == build_id:
                            used_endpoints.add(endpoint_id)
            build["used_service_connections"] = list(used_endpoints)
        return builds

    def calculate_pool_pipeline_permissions(self, queues):
        dedup_permissions = []
        for queue in queues:
            if "pipelinepermissions" in queue:
                for permission in queue["pipelinepermissions"]:
                    if permission not in dedup_permissions:
                        dedup_permissions.append(permission)
        return dedup_permissions

    def merge_pools_and_queues(self, pools, queues):
        queue_map = {}
        for q in queues:
            q = q["resource"]
            pool_id = q.get("pool", {}).get("id")
            if pool_id not in queue_map:
                queue_map[pool_id] = []
            queue_map[pool_id].append(q)

        for p in pools:
            p = p["resource"]
            pid = p["id"]
            p["queues"] = queue_map.get(pid, [])
            p["pipelinepermissions"] = self.calculate_pool_pipeline_permissions(p["queues"])
        return pools

    def enrich_resource_protection_and_cross_project(self, inventory):
        for inventory_key, inventory_value in inventory.items():
            for protected_resource in inventory_value["protected_resources"]:
                resource = protected_resource["resource"]

                if "checks" in resource and isinstance(resource["checks"], list) and len(resource["checks"]) > 0:
                    resource["protectedState"] = "protected"
                else:
                    resource["protectedState"] = "unprotected"

                if inventory_key == "endpoint" and "serviceEndpointProjectReferences" in resource:
                    resource["isCrossProject"] = len(resource["serviceEndpointProjectReferences"]) > 1
                else:
                    if "queues" in resource and isinstance(resource["queues"], list) and len(resource["queues"]) > 1:
                        resource["isCrossProject"] = True
                    elif "k_projects" in resource and isinstance(resource["k_projects"], list) and len(resource["k_projects"]) > 1:
                        resource["isCrossProject"] = True
                    elif "pipelinepermissions" in resource and isinstance(resource["pipelinepermissions"], list):
                        unique_projectids = set()
                        for permission in resource["pipelinepermissions"]:
                            proj_id = str(permission).split("_")[0]
                            unique_projectids.add(proj_id)
                        resource["isCrossProject"] = len(unique_projectids) > 1
                    else:
                        resource["isCrossProject"] = False
        return inventory

    def enrich_k_project(self, curr_project_id, self_attribute=None, current_project_name=None):
        if curr_project_id in self.manager.projects:
            proj = self.manager.projects[curr_project_id]
            k_proj = {"type": "project", "id": curr_project_id, "name": proj.get("name")}
            if self_attribute:
                k_proj["self_attribute"] = self_attribute
            return k_proj
        if current_project_name:
            k_proj = {"type": "project", "id": curr_project_id, "name": current_project_name}
            if self_attribute:
                k_proj["self_attribute"] = self_attribute
            return k_proj
        return {}

    def enrich_protected_resources_projectinfo(self, resource_type, resource, curr_project_id):
        match resource_type:
            case "pools":
                org = {
                    "type": "org",
                    "name": self.manager.organization,
                    "id": resource["scope"],
                    "self_attribute": f"https://dev.azure.com/{self.manager.organization}/_settings/agentpools?poolId={resource['id']}&view=agents",
                }
                resource["k_project"] = {resource["scope"]: org}
                return resource
            case "queue":
                resource["k_project"] = self.enrich_k_project(
                    resource["projectId"],
                    f"https://dev.azure.com/{self.manager.organization}/{resource['projectId']}/_settings/agentqueues?queueId={resource['id']}&view=agents",
                )
                return resource
            case "endpoint":
                resource["k_project"] = self.enrich_k_project(
                    curr_project_id,
                    f"https://dev.azure.com/{self.manager.organization}/{curr_project_id}/_settings/adminservices?resourceId={resource['id']}",
                )
                resource["k_projects_refs"] = []
                for project_reference in resource["serviceEndpointProjectReferences"]:
                    pid = project_reference["projectReference"]["id"]
                    self_attribute = f"https://dev.azure.com/{self.manager.organization}/{pid}/_settings/adminservices?resourceId={resource['id']}"
                    resource["k_projects_refs"].append(
                        self.enrich_k_project(pid, self_attribute, project_reference["projectReference"]["name"])
                    )
                if resource["isShared"]:
                    resource["k_project_shared_from"] = self.get_k_shared_from_endpoint(resource)
                return resource
            case "variablegroup":
                resource["k_project"] = self.enrich_k_project(
                    curr_project_id,
                    f"https://dev.azure.com/{self.manager.organization}/{curr_project_id}/_library?itemType=VariableGroups&view=VariableGroupView&variableGroupId={resource['id']}",
                )
                return resource
            case "securefile":
                resource["k_project"] = self.enrich_k_project(
                    curr_project_id,
                    f"https://dev.azure.com/{self.manager.organization}/{curr_project_id}/_library?itemType=SecureFiles&view=SecureFileView&secureFileId={resource['id']}",
                )
                return resource
            case "repository":
                resource["k_project"] = self.enrich_k_project(curr_project_id, resource.get("webUrl"))
                return resource
            case "environment":
                resource["k_project"] = self.enrich_k_project(
                    curr_project_id,
                    f"https://dev.azure.com/{self.manager.organization}/{curr_project_id}/_environments?view=resources&resourceId={resource['id']}",
                )
                return resource
            case "deploymentgroups":
                resource["k_project"] = self.enrich_k_project(
                    curr_project_id,
                    f"https://dev.azure.com/{self.manager.organization}/{curr_project_id}/_machinegroup?view=MachineGroupView&mgid={resource['id']}&tab=Details",
                )
                return resource
            case _:
                return resource

    def get_deployment_group_details(self, project_id, deployment_group):
        url = f"https://dev.azure.com/{self.manager.organization}/{project_id}/_apis/distributedtask/deploymentgroups/{deployment_group['id']}?api-version=7.1"
        try:
            data = self.http_ops.fetch_data(url)
            if not isinstance(data, dict):
                return deployment_group
            deployment_group["machines"] = data.get("machines", [])
            deployment_group["tags"] = data.get("tags", [])
            deployment_group["createdBy"] = data.get("createdBy", {})
            deployment_group["modifiedBy"] = data.get("modifiedBy", {})
            deployment_group["createdOn"] = data.get("createdOn", "")
            deployment_group["modifiedOn"] = data.get("modifiedOn", "")
            return deployment_group
        except Exception as err:
            self.logger.warning(f"Error fetching deployment group details: {err}")
            return deployment_group

    def get_k_shared_from_endpoint(self, resource):
        url = f"https://dev.azure.com/{self.manager.organization}/_apis/securityroles/scopes/distributedtask.collection.serviceendpointrole/roleassignments/resources/collection_{resource['id']}?api-version=7.1-preview.1"
        try:
            result = self.http_ops.fetch_data(url)
            matches = []
            references = resource.get("serviceEndpointProjectReferences", [])
            for ref in references:
                project_name = ref.get("projectReference", {}).get("name", "")
                entries = result.get("value", []) if isinstance(result, dict) else result
                for entry in entries:
                    identity_name = entry.get("identity", {}).get("displayName", "")
                    if project_name and project_name in identity_name:
                        matches.append({"Id": ref.get("projectReference", {}).get("id", ""), "name": project_name})
            return matches
        except Exception as err:
            self.logger.warning(f"An error occurred while retrieving shared info for resource {resource['id']}: {err}")
        return None

    def get_checks_approvals(self, inventory):
        self.logger.debug("Checking checks & approvals")
        for inventory_key, inventory_value in inventory.items():
            for protected_resource in inventory_value["protected_resources"]:
                actual_resource = protected_resource["resource"]
                project_id = actual_resource.get("k_project", {}).get("id", None)

                if inventory_value["level"] == "org":
                    protected_resource["resource"]["checks"] = []
                    continue

                url = (
                    f"https://dev.azure.com/{self.manager.organization}/{str(project_id)}/_apis/pipelines/checks/configurations?"
                    f"resourceType={inventory_key}&$expand=settings&resourceId={str(actual_resource['id'])}&api-version=7.1-preview.1"
                )
                if inventory_key == "repository":
                    url = (
                        f"https://dev.azure.com/{self.manager.organization}/{str(project_id)}/_apis/pipelines/checks/configurations?"
                        f"resourceType={inventory_key}&$expand=settings&resourceId={str(project_id)}.{str(actual_resource['id'])}&api-version=7.1-preview.1"
                    )

                new_checks = self.http_ops.fetch_data(url)
                if new_checks is None:
                    new_checks = []
                    continue

                self.logger.debug(f"{len(new_checks)} checks for {inventory_key} {actual_resource['name']} ({actual_resource['id']})")
                protected_resource["resource"]["checks"] = new_checks
        return inventory

    def get_permissions(self, inventory, all_definitions, builds):
        self.logger.debug("Checking permissioned pipelines")
        idx = self.manager._build_runtime_indexes(all_definitions, builds)
        wellformed_projects = idx.wellformed_project_ids

        for inventory_key, inventory_value in inventory.items():
            for protected_resource in inventory_value["protected_resources"]:
                actual_resource = protected_resource["resource"]
                actual_resource.setdefault("pipelinepermissions", [])

                if inventory_key == "endpoint":
                    for current_project_reference in actual_resource.get("serviceEndpointProjectReferences", []):
                        project_id = current_project_reference.get("projectReference", {}).get("id")
                        if not project_id:
                            continue
                        url = f"https://dev.azure.com/{self.manager.organization}/{project_id}/_apis/pipelines/pipelinepermissions/{inventory_key}/{actual_resource['id']}?api-version=7.1-preview.1"
                        data = self.http_ops.fetch_data(url)
                        data = data if isinstance(data, dict) else {}
                        current_project_reference.setdefault("projectReference", {}).setdefault("pipelinepermissions", [])
                        if "allPipelines" in data.keys():
                            perms = idx.definition_keys_by_project_id.get(project_id, [])
                        elif "pipelines" in data.keys():
                            perms = [f"{project_id}_{definition['id']}" for definition in data.get("pipelines", [])]
                        else:
                            perms = []
                            current_project_reference["projectReference"][
                                "warning"
                            ] = f"Project ID {project_id} not found in scope or permissions were insufficient."
                        actual_resource["pipelinepermissions"].extend(perms)
                        current_project_reference["projectReference"]["pipelinepermissions"].extend(perms)
                        actual_resource["pipelinepermissions"] = ordered_dedupe(actual_resource["pipelinepermissions"])
                        current_project_reference["projectReference"]["pipelinepermissions"] = ordered_dedupe(
                            current_project_reference["projectReference"]["pipelinepermissions"]
                        )
                    continue

                if inventory_key == "repository":
                    owner_project = actual_resource.get("project", {}).get("id")
                    repo_id = actual_resource.get("id")
                    # Query repository permissions from every well-formed project.
                    # Cross-project grants can exist even when local indexes do not
                    # yet show definitions/builds referencing this repository.
                    for project in wellformed_projects:
                        url = f"https://dev.azure.com/{self.manager.organization}/{project}/_apis/pipelines/pipelinepermissions/{inventory_key}/{actual_resource['project']['id']}.{actual_resource['id']}?api-version=7.1-preview.1"
                        data = self.http_ops.fetch_data(url)
                        data = data if isinstance(data, dict) else {}
                        if "allPipelines" in data.keys():
                            actual_resource["pipelinepermissions"].extend(idx.definition_keys_by_project_id.get(project, []))
                        else:
                            actual_resource["pipelinepermissions"].extend(
                                [f"{project}_{definition['id']}" for definition in data.get("pipelines", [])]
                            )

                    for build in idx.builds_by_repo_id.get(repo_id, []):
                        project_id = build.get("k_project", {}).get("id")
                        definition_id = build.get("definition", {}).get("id")
                        if project_id and definition_id is not None:
                            actual_resource["pipelinepermissions"].append(f"{project_id}_{definition_id}")

                    for definition in idx.definitions_by_repo_id.get(repo_id, []):
                        def_key = definition.get("k_key")
                        if def_key:
                            actual_resource["pipelinepermissions"].append(def_key)
                    if owner_project:
                        actual_resource["pipelinepermissions"].extend(idx.definition_keys_by_project_id.get(owner_project, []))

                    actual_resource["pipelinepermissions"] = ordered_dedupe(actual_resource["pipelinepermissions"])
                    continue

                if inventory_value.get("level") == "org":
                    actual_resource["pipelinepermissions"] = []
                    continue

                owner_project = extract_owner_project_id(actual_resource)
                query_projects = [owner_project] if owner_project else list(wellformed_projects)
                query_projects = [project for project in query_projects if project]
                for project in query_projects:
                    try:
                        url = f"https://dev.azure.com/{self.manager.organization}/{project}/_apis/pipelines/pipelinepermissions/{inventory_key}/{actual_resource['id']}?api-version=7.1-preview.1"
                        data = self.http_ops.fetch_data(url)
                        data = data if isinstance(data, dict) else {}
                        if "allPipelines" in data.keys():
                            actual_resource["pipelinepermissions"].extend(idx.definition_keys_by_project_id.get(project, []))
                        else:
                            actual_resource["pipelinepermissions"].extend(
                                [f"{project}_{definition['id']}" for definition in data.get("pipelines", [])]
                            )
                    except Exception as e:
                        print(
                            f"Error fetching pipeline permissions for {inventory_key} {actual_resource.get('name')} ({actual_resource.get('id')}) in project {project}: {e}"
                        )
                actual_resource["pipelinepermissions"] = ordered_dedupe(actual_resource["pipelinepermissions"])
        return inventory

    def get_protected_resources(self, inventory):
        wellformed_projects = self.manager._wellformed_project_ids()
        seen_ids = {
            key: {resource["resource"]["id"] for resource in value["protected_resources"] if resource.get("resource")}
            for key, value in inventory.items()
        }

        org_inventory = [(k, v) for k, v in inventory.items() if v.get("level") == "org"]
        project_inventory = [(k, v) for k, v in inventory.items() if v.get("level") != "org"]

        if wellformed_projects and org_inventory:
            project = wellformed_projects[0]
            for inventory_key, inventory_value in org_inventory:
                url = f"https://dev.azure.com/{self.manager.organization}/_apis/{inventory_value['api_endpoint']}"
                if inventory_value.get("query_params"):
                    url = f"{url}?{inventory_value['query_params']}"
                try:
                    self.logger.debug(f"Discovering {inventory_key} @ organisation level")
                    new_resources = normalize_to_list(self.http_ops.fetch_data(url))
                    self.logger.debug(f"{len(new_resources)} {inventory_key} found")
                    for new_resource in new_resources:
                        resource_id = new_resource.get("id")
                        if resource_id in seen_ids[inventory_key]:
                            continue
                        pname = urllib.parse.quote(self.manager.projects[project]["name"])
                        if inventory_key == "pools":
                            new_resource["k_url"] = f"https://dev.azure.com/{self.manager.organization}/_settings/agentpools?poolId={new_resource['id']}"
                        elif inventory_key == "queue":
                            new_resource["k_url"] = f"https://dev.azure.com/{self.manager.organization}/{pname}/_settings/agentqueues?queueId={new_resource['id']}"
                        elif inventory_key == "endpoint":
                            new_resource["k_url"] = f"https://dev.azure.com/{self.manager.organization}/{pname}/_settings/adminservices?resourceId={new_resource['id']}"
                        elif inventory_key == "repository":
                            new_resource["k_url"] = f"https://dev.azure.com/{self.manager.organization}/{pname}/_git/{new_resource['name']}"
                        elif inventory_key == "securefile":
                            new_resource["k_url"] = f"https://dev.azure.com/{self.manager.organization}/{pname}/_library?itemType=SecureFiles&view=SecureFileView&secureFileId={new_resource['id']}"
                        elif inventory_key == "variablegroup":
                            new_resource["k_url"] = f"https://dev.azure.com/{self.manager.organization}/{pname}/_library?itemType=VariableGroups&view=VariableGroupView&variableGroupId={new_resource['id']}"
                        elif inventory_key == "environment":
                            new_resource["k_url"] = f"https://dev.azure.com/{self.manager.organization}/{pname}/_environments/{new_resource['id']}?view=deployments"
                        elif inventory_key == "deploymentgroups":
                            new_resource["k_url"] = f"https://dev.azure.com/{self.manager.organization}/{pname}/_machinegroup?view=MachineGroupView&mgid={new_resource['id']}&tab=Details"

                        new_resource = self.enrich_protected_resources_projectinfo(inventory_key, new_resource, project)
                        if inventory_key == "deploymentgroups":
                            new_resource = self.get_deployment_group_details(project, new_resource)
                        inventory_value["protected_resources"].append({"resourceType": inventory_key, "resource": new_resource})
                        seen_ids[inventory_key].add(resource_id)
                except Exception as err:
                    self.logger.warning(f"Error discovering org-level {inventory_key}: {err}")

        for project in wellformed_projects:
            for inventory_key, inventory_value in project_inventory:
                url = f"https://dev.azure.com/{self.manager.organization}/{project}/_apis/{inventory_value['api_endpoint']}"
                if inventory_value.get("query_params"):
                    url = f"{url}?{inventory_value['query_params']}"
                try:
                    self.logger.debug(f"Discovering {inventory_key} @ {self.manager.projects[project]['name']}")
                    new_resources = normalize_to_list(self.http_ops.fetch_data(url))
                    self.logger.debug(f"{len(new_resources)} {inventory_key} found")

                    for new_resource in new_resources:
                        resource_id = new_resource.get("id")
                        if resource_id in seen_ids[inventory_key]:
                            continue

                        if inventory_key in ["environment", "deploymentgroups"]:
                            details_url = f"https://dev.azure.com/{self.manager.organization}/{project}/_apis/{inventory_value['api_endpoint']}/{resource_id}?{inventory_value.get('query_params', '')}"
                            self.logger.debug(f"Enriching {inventory_key} details for {new_resource.get('name')} @ {self.manager.projects[project]['name']}")
                            details = self.http_ops.fetch_data(details_url)
                            if isinstance(details, dict):
                                new_resource = details

                        pname = urllib.parse.quote(self.manager.projects[project]["name"])
                        if inventory_key == "pools":
                            new_resource["k_url"] = f"https://dev.azure.com/{self.manager.organization}/_settings/agentpools?poolId={new_resource['id']}"
                        elif inventory_key == "queue":
                            new_resource["k_url"] = f"https://dev.azure.com/{self.manager.organization}/{pname}/_settings/agentqueues?queueId={new_resource['id']}"
                        elif inventory_key == "endpoint":
                            new_resource["k_url"] = f"https://dev.azure.com/{self.manager.organization}/{pname}/_settings/adminservices?resourceId={new_resource['id']}"
                        elif inventory_key == "repository":
                            new_resource["k_url"] = f"https://dev.azure.com/{self.manager.organization}/{pname}/_git/{new_resource['name']}"
                        elif inventory_key == "securefile":
                            new_resource["k_url"] = f"https://dev.azure.com/{self.manager.organization}/{pname}/_library?itemType=SecureFiles&view=SecureFileView&secureFileId={new_resource['id']}"
                        elif inventory_key == "variablegroup":
                            new_resource["k_url"] = f"https://dev.azure.com/{self.manager.organization}/{pname}/_library?itemType=VariableGroups&view=VariableGroupView&variableGroupId={new_resource['id']}"
                        elif inventory_key == "environment":
                            new_resource["k_url"] = f"https://dev.azure.com/{self.manager.organization}/{pname}/_environments/{new_resource['id']}?view=deployments"
                        elif inventory_key == "deploymentgroups":
                            new_resource["k_url"] = f"https://dev.azure.com/{self.manager.organization}/{pname}/_machinegroup?view=MachineGroupView&mgid={new_resource['id']}&tab=Details"

                        new_resource = self.enrich_protected_resources_projectinfo(inventory_key, new_resource, project)
                        if inventory_key == "deploymentgroups":
                            new_resource = self.get_deployment_group_details(project, new_resource)
                        inventory_value["protected_resources"].append({"resourceType": inventory_key, "resource": new_resource})
                        seen_ids[inventory_key].add(resource_id)
                except Exception as err:
                    self.logger.warning(f"Error discovering {inventory_key}: {err}")

        try:
            inventory["pools"]["protected_resources"] = self.merge_pools_and_queues(
                inventory["pools"]["protected_resources"], inventory["queue"]["protected_resources"]
            )
        except Exception as e:
            self.logger.warning(f"Failed to merge pools and queues: {e}")

        for repository in inventory["repository"]["protected_resources"]:
            repo = repository["resource"]
            branches, branches_names = self.manager.get_repository_branches(
                repo["project"]["id"], repo["id"], repo["project"]["name"], repo["name"], -1, ""
            )
            repo["branches"] = branches

            first_commit_date, last_commit_date = self.manager.get_repository_commit_dates(repo["project"]["id"], repo["id"])
            repo["stats"] = {}
            repo["stats"]["firstCommitDate"] = (
                first_commit_date.isoformat() if isinstance(first_commit_date, datetime) and first_commit_date else first_commit_date
            )
            repo["stats"]["lastCommitDate"] = (
                last_commit_date.isoformat() if isinstance(last_commit_date, datetime) and last_commit_date else last_commit_date
            )
            repo["stats"]["age"] = (datetime.now(timezone.utc) - last_commit_date).days if last_commit_date else None
            repo["stats"]["branches"] = len(branches_names)
            repo["stats"]["pullRequests"] = self.manager.get_repository_pull_requests_count(repo["project"]["id"], repo["id"])
            if last_commit_date:
                now = datetime.now(timezone.utc)
                if last_commit_date > now - timedelta(days=90):
                    repo["stats"]["state"] = "active"
                elif last_commit_date > now - timedelta(days=365):
                    repo["stats"]["state"] = "stale"
                else:
                    repo["stats"]["state"] = "dormant"
            else:
                repo["stats"]["state"] = "unknown"

        # Attach last-used info to endpoints
        inventory = self.attach_endpoint_last_used(inventory)
        return inventory

    def get_enriched_build_definitions(self, definitions, resource_inventory):
        self.logger.debug("Enriching build definitions with protected resources")
        definitions_map = {}
        for resource_type in resource_inventory:
            for protected_resource in resource_inventory[resource_type]["protected_resources"]:
                actual_resource = protected_resource["resource"]
                if protected_resource["resourceType"] == "pools":
                    for queue in actual_resource["queues"]:
                        if "pipelinepermissions" not in queue:
                            continue
                        for pipelinepermission in queue["pipelinepermissions"]:
                            # Extract project_id from pipelinepermission key (format: project_id_definition_id)
                            pipeline_project_id = pipelinepermission.split("_")[0]
                            queue_project_id = queue.get("projectId", "")
                            
                            # Only associate the queue if it's in the same project as the pipeline
                            if pipeline_project_id == queue_project_id:
                                if pipelinepermission not in definitions_map:
                                    definitions_map[pipelinepermission] = []
                                definitions_map[pipelinepermission].append(f"pool_merged_{actual_resource['id']}")
                                definitions_map[pipelinepermission].append(f"queue_{queue['id']}")
                elif protected_resource["resourceType"] == "deploymentgroups":
                    continue
                else:
                    if "pipelinepermissions" not in actual_resource:
                        actual_resource["pipelinepermissions"] = []
                    for pipelinepermission in actual_resource["pipelinepermissions"]:
                        if pipelinepermission not in definitions_map:
                            definitions_map[pipelinepermission] = []
                        definitions_map[pipelinepermission].append(f"{protected_resource['resourceType']}_{actual_resource['id']}")

        for key, def_resourcepermissions in definitions_map.items():
            def_obj = next((d for d in definitions if d.get("k_key") == key), None)
            if def_obj is None:
                continue
            if "resourcepermissions" not in def_obj:
                def_obj["resourcepermissions"] = {}
            for res_permission in def_resourcepermissions:
                res_permission_type, res_permission_id = res_permission.rsplit("_", 1)
                if res_permission_type not in def_obj["resourcepermissions"]:
                    def_obj["resourcepermissions"][res_permission_type] = []
                if res_permission_id not in def_obj["resourcepermissions"][res_permission_type]:
                    def_obj["resourcepermissions"][res_permission_type].append(res_permission_id)
        return definitions
