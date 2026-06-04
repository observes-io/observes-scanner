#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, TYPE_CHECKING

logger = logging.getLogger(__name__)


class AzureDevOpsPipelineParser:
    """
    Parser for Azure DevOps pipeline YAML that transforms it into a structured
    graph representation with nodes, edges, and resources.
    """
    
    def __init__(self, organization: str):
        self.organization = organization
        self.resources = []
        self.nodes = []
        self.edges = []
        self.diagnostics = []
        self.resource_index = {}  # Track resources by ID to avoid duplicates
        self.node_index = {}  # Track nodes by ID to avoid duplicates
    
    def parse(self, yaml_content: str, parsed_yaml: Dict, metadata: Dict, commit: str = None, ref: str = None) -> Dict:
        # Compute content hash
        content_hash = hashlib.sha256(yaml_content.encode('utf-8')).hexdigest()
        snapshot_id = f"sha256:{content_hash}"
        
        # Build the structured representation of the build or potential build
        structured = {
            "schemaVersion": "0.1.0",
            "platform": "azure-devops",
            "capturedAt": datetime.now(timezone.utc).isoformat() + "Z",
            "snapshotId": snapshot_id,
            "snapshotHashOf": "resources+nodes+edges",
            # Scope includes organization and project context - specific to Azure DevOps
            "scope": {
                "organization": self.organization,
                "projectId": metadata.get("project_id"),
                "project": metadata.get("project_name")
            },
            "pipeline": self._extract_pipeline_info(metadata, commit, ref, content_hash),
            "resources": [],
            "nodes": [],
            "edges": [],
            "diagnostics": []
        }
        
        # Reset tracking for this parse
        self.resources = []
        self.nodes = []
        self.edges = []
        self.diagnostics = []
        self.resource_index = {}
        self.node_index = {}
        
        try:
            # Extract and build nodes and edges from parsed YAML
            if parsed_yaml:
                self._extract_pipeline_structure(parsed_yaml, metadata)
            
            structured["resources"] = self.resources
            structured["nodes"] = self.nodes
            structured["edges"] = self.edges
            structured["diagnostics"] = self.diagnostics
            
        except Exception as e:
            logger.warning(f"Error parsing pipeline structure: {e}")
            self.diagnostics.append({
                "severity": "error",
                "message": f"Failed to parse pipeline structure: {str(e)}"
            })
            structured["diagnostics"] = self.diagnostics
        
        return structured
    
    def _extract_pipeline_info(self, metadata: Dict, commit: str, ref: str, content_hash: str) -> Dict:
        """Extract pipeline metadata."""
        pipeline_info = {
            "pipelineId": metadata.get("pipeline_id"),
            "contentHash": f"sha256:{content_hash}",
            "name": metadata.get("pipeline_name"),
            "url": metadata.get("pipeline_url"),
            "type": metadata.get("pipeline_type", "yaml"),
            "status": metadata.get("pipeline_status", "enabled")
        }
        
        # Add source information if available
        if metadata.get("repository"):
            pipeline_info["source"] = {
                "file": metadata.get("yaml_path", "azure-pipelines.yml"),
                "repository": metadata["repository"],
                "commit": commit,
                "ref": ref
            }
        
        return pipeline_info
    
    def _extract_self_repository(self, metadata: Dict, root_node: Dict):
        """Add the pipeline's own repository as an implicit resource."""
        repo_info = metadata.get("repository", {})
        
        repo_name = repo_info.get("name", "self")
        repo_id = "resource.repo.self"
        
        resource = {
            "id": repo_id,
            "kind": "resource",
            "subtype": "repository",
            "name": repo_name,
            "provider": repo_info.get("type", "git"),
            "repository": repo_name,
            "ref": repo_info.get("ref"),
            "commit": repo_info.get("commit"),
            "url": repo_info.get("url"),
            "implicit": True
        }
        
        node = {
            "id": repo_id,
            "kind": "resource",
            "subtype": "repository",
            "name": repo_name,
            "resourceProvider": repo_info.get("type", "git"),
            "repository": repo_name,
            "ref": repo_info.get("ref"),
            "commit": repo_info.get("commit"),
            "url": repo_info.get("url"),
            "implicit": True
        }
        
        self._add_resource(resource)
        self._add_node(node)
        root_node["children"].append(repo_id)
        self._add_edge("contains", root_node["id"], repo_id, "pipeline.repo.self")
    
    def _extract_pipeline_structure(self, parsed_yaml: Dict, metadata: Dict):
        """Extract the pipeline structure: stages, jobs, steps, and resources."""
        
        # Create root pipeline node
        root_node = {
            "id": "pipeline.root",
            "kind": "pipeline",
            "triggers": self._extract_triggers(parsed_yaml),
            "children": []
        }
        
        # The pipeline's own repository is always an implicit resource
        self._extract_self_repository(metadata, root_node)
        
        # Extract resources (repositories, containers, pipelines, etc.)
        resources_section = parsed_yaml.get("resources", {})
        if resources_section:
            self._extract_resources(resources_section, root_node)
        
        # Extract variables (they become part of pipeline context)
        variables = parsed_yaml.get("variables", [])
        if variables:
            root_node["variables"] = self._normalize_variables(variables)
            self._extract_variable_groups(variables, root_node)
        
        # Extract pool if defined at pipeline level
        pipeline_pool = parsed_yaml.get("pool")
        if pipeline_pool:
            pool_id = self._extract_pool(pipeline_pool, "pipeline.root")
            root_node["executionContext"] = {"pool": pool_id}
        
        # Extract stages, jobs, or steps
        if "stages" in parsed_yaml:
            self._extract_stages(parsed_yaml["stages"], root_node)
        elif "jobs" in parsed_yaml:
            # Implicit single stage
            stage_node = self._create_implicit_stage("stage.default", root_node["id"])
            self._extract_jobs(parsed_yaml["jobs"], stage_node)
            self._add_node(stage_node)
            root_node["children"].append(stage_node["id"])
            self._add_edge("contains", root_node["id"], stage_node["id"], "pipeline.stage")
        elif "steps" in parsed_yaml:
            # Implicit single stage and single job
            stage_node = self._create_implicit_stage("stage.default", root_node["id"])
            job_node = self._create_implicit_job("job.default", stage_node["id"])
            self._extract_steps(parsed_yaml["steps"], job_node)
            self._add_node(job_node)
            self._add_node(stage_node)
            stage_node["children"].append(job_node["id"])
            root_node["children"].append(stage_node["id"])
            self._add_edge("contains", stage_node["id"], job_node["id"], "stage.job")
            self._add_edge("contains", root_node["id"], stage_node["id"], "pipeline.stage")
        
        self._add_node(root_node)
    
    def _extract_triggers(self, parsed_yaml: Dict) -> List[Dict]:
        """Extract trigger configuration."""
        triggers = []
        
        trigger = parsed_yaml.get("trigger")
        if trigger is not None:
            if trigger == "none" or trigger is False:
                triggers.append({"type": "ci", "enabled": False})
            else:
                triggers.append({"type": "ci", "enabled": True, "config": trigger})
        
        pr = parsed_yaml.get("pr")
        if pr is not None:
            if pr == "none" or pr is False:
                triggers.append({"type": "pr", "enabled": False})
            else:
                triggers.append({"type": "pr", "enabled": True, "config": pr})
        
        schedules = parsed_yaml.get("schedules")
        if schedules:
            triggers.append({"type": "schedule", "enabled": True, "config": schedules})
        
        return triggers if triggers else [{"type": "trigger", "enabled": False}]
    
    def _extract_resources(self, resources_section: Dict, root_node: Dict):
        """Extract resources like repositories, pipelines, containers."""
        
        # Repositories
        repositories = resources_section.get("repositories", [])
        for repo in repositories:
            repo_id = self._extract_repository_resource(repo)
            if repo_id:
                root_node["children"].append(repo_id)
                self._add_edge("contains", root_node["id"], repo_id, "pipeline.repo")
        
        # Pipelines
        pipelines = resources_section.get("pipelines", [])
        for pipeline in pipelines:
            pipeline_id = self._extract_pipeline_resource(pipeline)
            if pipeline_id:
                root_node["children"].append(pipeline_id)
                self._add_edge("contains", root_node["id"], pipeline_id, "pipeline.pipeline_resource")
        
        # Containers
        containers = resources_section.get("containers", [])
        for container in containers:
            container_id = self._extract_container_resource(container)
            if container_id:
                root_node["children"].append(container_id)
                self._add_edge("contains", root_node["id"], container_id, "pipeline.container")
    
    def _extract_repository_resource(self, repo: Dict) -> Optional[str]:
        """Extract a repository resource."""
        name = repo.get("repository") or repo.get("name")
        if not name:
            return None
        
        repo_type = repo.get("type", "git")
        endpoint = repo.get("endpoint")
        
        resource_id = f"resource.repo.{name}"
        node_id = resource_id
        
        resource = {
            "id": resource_id,
            "kind": "resource",
            "subtype": "repository",
            "name": name,
            "provider": repo_type,
            "repository": repo.get("name", name),
            "ref": repo.get("ref", "main")
        }
        
        node = {
            "id": node_id,
            "kind": "resource",
            "subtype": "repository",
            "name": name,
            "resourceProvider": repo_type,
            "repository": repo.get("name", name),
            "ref": repo.get("ref", "main")
        }
        
        if endpoint:
            sc_id = f"serviceConnection.{endpoint}"
            resource["serviceConnection"] = {
                "id": sc_id,
                "name": endpoint
            }
            node["serviceConnection"] = sc_id
            # Create service connection if not exists
            self._ensure_service_connection(sc_id, endpoint, repo_type)
            self._add_edge("uses_service_connection", node_id, sc_id, "repo.serviceconnection")
        
        self._add_resource(resource)
        self._add_node(node)
        
        return node_id
    
    def _extract_pipeline_resource(self, pipeline: Dict) -> Optional[str]:
        """Extract a pipeline resource."""
        name = pipeline.get("pipeline")
        if not name:
            return None
        
        resource_id = f"resource.pipeline.{name}"
        
        resource = {
            "id": resource_id,
            "kind": "resource",
            "subtype": "pipeline",
            "name": name,
            "source": pipeline.get("source")
        }
        
        node = {
            "id": resource_id,
            "kind": "resource",
            "subtype": "pipeline",
            "name": name,
            "source": pipeline.get("source")
        }
        
        self._add_resource(resource)
        self._add_node(node)
        
        return resource_id
    
    def _extract_container_resource(self, container: Dict) -> Optional[str]:
        """Extract a container resource."""
        name = container.get("container")
        if not name:
            return None
        
        resource_id = f"resource.container.{name}"
        
        resource = {
            "id": resource_id,
            "kind": "resource",
            "subtype": "container",
            "name": name,
            "image": container.get("image")
        }
        
        node = {
            "id": resource_id,
            "kind": "resource",
            "subtype": "container",
            "name": name,
            "image": container.get("image")
        }
        
        endpoint = container.get("endpoint")
        if endpoint:
            sc_id = f"serviceConnection.{endpoint}"
            resource["serviceConnection"] = {"id": sc_id, "name": endpoint}
            node["serviceConnection"] = sc_id
            self._ensure_service_connection(sc_id, endpoint, "container_registry")
            self._add_edge("uses_service_connection", resource_id, sc_id, "container.serviceconnection")
        
        self._add_resource(resource)
        self._add_node(node)
        
        return resource_id
    
    def _extract_stages(self, stages: List[Dict], parent_node: Dict):
        """Extract stages from pipeline."""
        for idx, stage in enumerate(stages):
            stage_name = stage.get("stage")
            if not stage_name:
                continue
            
            stage_id = f"stage.{stage_name}"
            
            stage_node = {
                "id": stage_id,
                "kind": "stage",
                "name": stage_name,
                "displayName": stage.get("displayName", stage_name),
                "parent": parent_node["id"],
                "children": [],
                "order": idx + 1
            }
            
            condition = stage.get("condition")
            if condition:
                stage_node["condition"] = condition
            
            # Check for depends_on
            depends_on = stage.get("dependsOn")
            if depends_on:
                stage_node["dependsOn"] = depends_on if isinstance(depends_on, list) else [depends_on]
                for dep in stage_node["dependsOn"]:
                    self._add_edge("depends_on", stage_id, f"stage.{dep}", f"stage.dependency.{dep}")
            
            # Extract stage-level variables and variable groups
            stage_variables = stage.get("variables", [])
            if stage_variables:
                self._extract_variable_groups(stage_variables, stage_node)
            
            # Extract pool if specified at stage level
            stage_pool = stage.get("pool")
            if stage_pool:
                pool_id = self._extract_pool(stage_pool, stage_id)
                stage_node["executionContext"] = {"pool": pool_id}
                self._add_edge("uses_pool", stage_id, pool_id, f"stage.pool")
            
            # Extract jobs
            jobs = stage.get("jobs", [])
            self._extract_jobs(jobs, stage_node)
            
            self._add_node(stage_node)
            parent_node["children"].append(stage_id)
            self._add_edge("contains", parent_node["id"], stage_id, f"pipeline.stage.{stage_name}")
    
    def _extract_jobs(self, jobs: List[Dict], parent_node: Dict):
        """Extract jobs from stage."""
        for idx, job in enumerate(jobs):
            job_name = job.get("job") or job.get("deployment") or job.get("template")
            if not job_name:
                continue
            
            job_type = "job"
            if "deployment" in job:
                job_type = "deployment"
            elif "template" in job:
                job_type = "template"
            
            job_id = f"job.{job_name}.{parent_node['id']}"
            
            job_node = {
                "id": job_id,
                "kind": "job",
                "subtype": job_type,
                "name": job_name,
                "displayName": job.get("displayName", job_name),
                "parent": parent_node["id"],
                "children": [],
                "order": idx + 1
            }
            
            condition = job.get("condition")
            if condition:
                job_node["condition"] = condition
            
            # Check for depends_on
            depends_on = job.get("dependsOn")
            if depends_on:
                job_node["dependsOn"] = depends_on if isinstance(depends_on, list) else [depends_on]
                for dep in job_node["dependsOn"]:
                    self._add_edge("depends_on", job_id, f"job.{dep}.{parent_node['id']}", f"job.dependency.{dep}")
            
            # Extract pool
            job_pool = job.get("pool")
            if job_pool:
                pool_id = self._extract_pool(job_pool, job_id)
                job_node["overridePool"] = True
                job_node["executionContext"] = {"pool": pool_id}
                self._add_edge("uses_pool", job_id, pool_id, f"job.pool")
            else:
                job_node["overridePool"] = False
            
            # For deployment jobs, extract environment
            if job_type == "deployment":
                environment = job.get("environment")
                if environment:
                    env_id = self._extract_environment(environment, job_id)
                    if env_id:
                        job_node["environmentRefs"] = [env_id]
                        job_node["children"].append(env_id)
                        self._add_edge("targets_environment", job_id, env_id, f"job.environment")
                
                strategy = job.get("strategy")
                if strategy:
                    job_node["strategy"] = {"type": next(iter(strategy.keys()))}
            
            # Extract steps
            steps = job.get("steps", [])
            self._extract_steps(steps, job_node)
            
            self._add_node(job_node)
            parent_node["children"].append(job_id)
            self._add_edge("contains", parent_node["id"], job_id, f"stage.job.{job_name}")
    
    def _extract_steps(self, steps: List, parent_node: Dict):
        """Extract steps from job."""
        for idx, step in enumerate(steps):
            if not isinstance(step, dict):
                continue
            
            # Determine step type and name
            step_type = None
            step_name = None
            
            if "task" in step:
                step_type = "task"
                step_name = step.get("displayName") or step.get("task")
            elif "script" in step or "bash" in step or "pwsh" in step or "powershell" in step:
                step_type = "task"
                if "bash" in step:
                    step_name = step.get("displayName", "Bash")
                elif "pwsh" in step:
                    step_name = step.get("displayName", "PowerShell")
                elif "powershell" in step:
                    step_name = step.get("displayName", "PowerShell")
                else:
                    step_name = step.get("displayName", "Script")
            elif "checkout" in step:
                step_type = "task"
                step_name = f"Checkout {step.get('checkout', 'self')}"
            elif "download" in step:
                step_type = "task"
                step_name = f"Download {step.get('download', 'current')}"
            elif "publish" in step:
                step_type = "task"
                step_name = f"Publish"
            elif "template" in step:
                step_type = "template"
                step_name = step.get("template")
            else:
                step_name = f"step_{idx + 1}"
                step_type = "unknown"
            
            # Generate step ID
            parent_id = parent_node["id"]
            step_id = f"step.{idx + 1}.{self._sanitize_id(step_name)}.{parent_id}"
            
            step_node = {
                "id": step_id,
                "kind": "step",
                "subtype": step_type,
                "name": step_name,
                "parent": parent_id,
                "order": idx + 1
            }
            
            condition = step.get("condition")
            if condition:
                step_node["condition"] = condition
            
            # Extract task information
            if "task" in step:
                task_name = step["task"]
                task_parts = task_name.split("@")
                task_id = task_parts[0]
                task_version = task_parts[1] if len(task_parts) > 1 else "0"
                
                step_node["uses"] = {
                    "type": "task",
                    "name": task_id,
                    "id": task_id,
                    "version": task_version
                }
            
            # Extract inputs
            if "inputs" in step:
                step_node["inputs"] = step["inputs"]
            
            # Detect secure file references (DownloadSecureFile task)
            if "task" in step:
                task_ref = step["task"]
                if task_ref.startswith("DownloadSecureFile"):
                    secure_file_name = step.get("inputs", {}).get("secureFile")
                    if secure_file_name:
                        sf_id = self._extract_secure_file(secure_file_name)
                        step_node.setdefault("resourceRefs", []).append(sf_id)
                        self._add_edge("references_resource", step_id, sf_id, f"step.securefile.{self._sanitize_id(secure_file_name)}")
            
            # Detect service connections referenced in task inputs
            if "inputs" in step:
                self._extract_task_service_connections(step["inputs"], step_id, step_node)
            
            # Extract environment variables
            env = step.get("env", {})
            if env:
                step_node["environmentVariables"] = env
            
            # Extract display name
            if "displayName" in step:
                step_node["displayName"] = step["displayName"]
            
            # Check for specific patterns (checkout, publish, download)
            if "checkout" in step:
                checkout_repo = step["checkout"]
                if checkout_repo != "self" and checkout_repo != "none":
                    repo_id = f"resource.repo.{checkout_repo}"
                    step_node["resourceRefs"] = [repo_id]
                    self._add_edge("references_resource", step_id, repo_id, f"step.checkout.{checkout_repo}")
            
            # Link steps with execution order
            if idx > 0:
                prev_parent_id = parent_node["id"]
                prev_step_id = f"step.{idx}.{self._sanitize_id(self._get_step_name(steps[idx-1], idx))}.{prev_parent_id}"
                self._add_edge("executes_before", prev_step_id, step_id, f"step.execution.{idx}")
            
            self._add_node(step_node)
            parent_node["children"].append(step_id)
            self._add_edge("contains", parent_id, step_id, f"job.step.{idx + 1}")
    
    def _extract_pool(self, pool: Any, context_id: str) -> str:
        """Extract pool information and return pool ID."""
        if isinstance(pool, str):
            pool_name = pool
            pool_dict = {"name": pool_name}
        elif isinstance(pool, dict):
            pool_dict = pool
            pool_name = pool_dict.get("name", pool_dict.get("vmImage", "default"))
        else:
            pool_name = "default"
            pool_dict = {}
        
        pool_id = f"pool.{self._sanitize_id(pool_name)}"
        
        # Check if this pool already exists
        if pool_id in self.resource_index:
            return pool_id
        
        is_hosted = "vmImage" in pool_dict or pool_name in ["Azure Pipelines", "Hosted"]
        
        resource = {
            "id": pool_id,
            "kind": "pool",
            "name": pool_name,
            "platformManaged": is_hosted,
            "poolType": "hosted" if is_hosted else "self-hosted",
            "vmImage": pool_dict.get("vmImage"),
            "platformId": None,
            "queueId": None
        }
        
        node = {
            "id": pool_id,
            "kind": "pool",
            "name": pool_name,
            "platformManaged": is_hosted,
            "poolType": "hosted" if is_hosted else "self-hosted",
            "vmImage": pool_dict.get("vmImage"),
            "platformId": None,
            "queueId": None
        }
        
        self._add_resource(resource)
        self._add_node(node)
        
        return pool_id
    
    def _extract_environment(self, environment: Any, context_id: str) -> Optional[str]:
        """Extract environment information and return environment ID."""
        if isinstance(environment, str):
            env_name = environment
            env_dict = {"name": env_name}
        elif isinstance(environment, dict):
            env_dict = environment
            env_name = env_dict.get("name")
        else:
            return None
        
        if not env_name:
            return None
        
        env_id = f"environment.{self._sanitize_id(env_name)}"
        
        # Check if this environment already exists
        if env_id in self.resource_index:
            return env_id
        
        resource = {
            "id": env_id,
            "kind": "environment",
            "name": env_name,
            "platformId": None
        }
        
        node = {
            "id": env_id,
            "kind": "environment",
            "name": env_name,
            "platformId": None
        }
        
        self._add_resource(resource)
        self._add_node(node)
        
        return env_id
    
    def _ensure_service_connection(self, sc_id: str, name: str, connection_type: str):
        """Ensure a service connection resource/node exists."""
        if sc_id in self.resource_index:
            return
        
        resource = {
            "id": sc_id,
            "kind": "serviceConnection",
            "name": name,
            "connectionType": connection_type,
            "platformId": None
        }
        
        node = {
            "id": sc_id,
            "kind": "serviceConnection",
            "name": name,
            "connectionType": connection_type,
            "platformId": None
        }
        
        self._add_resource(resource)
        self._add_node(node)
    
    _SERVICE_CONNECTION_INPUT_KEYS = {
        "azureSubscription",
        "connectedServiceName",
        "connectedServiceNameARM",
        "connectedServiceNameSelector",
        "containerRegistry",
        "customEndpoint",
        "dockerRegistryEndpoint",
        "endpoint",
        "externalEndpoints",
        "kubernetesServiceEndpoint",
        "nuGetServiceConnections",
        "serverEndpoint",
        "serviceEndpoint",
    }
    
    def _extract_task_service_connections(self, inputs: Dict, step_id: str, step_node: Dict):
        """Detect service connection references in task inputs."""
        if not isinstance(inputs, dict):
            return
        for key, value in inputs.items():
            if key in self._SERVICE_CONNECTION_INPUT_KEYS and value and isinstance(value, str):
                sc_id = f"serviceConnection.{value}"
                self._ensure_service_connection(sc_id, value, key)
                self._add_edge("uses_service_connection", step_id, sc_id,
                               f"step.serviceconnection.{self._sanitize_id(value)}")
    
    def _extract_variable_groups(self, variables: Any, context_node: Dict):
        """Extract variable group references as resources."""
        if not isinstance(variables, list):
            return
        
        for var in variables:
            if isinstance(var, dict) and "group" in var:
                group_name = var["group"]
                group_id = f"variableGroup.{self._sanitize_id(group_name)}"
                
                if group_id not in self.resource_index:
                    resource = {
                        "id": group_id,
                        "kind": "variableGroup",
                        "name": group_name,
                        "platformId": None
                    }
                    node = {
                        "id": group_id,
                        "kind": "variableGroup",
                        "name": group_name,
                        "platformId": None
                    }
                    self._add_resource(resource)
                    self._add_node(node)
                
                self._add_edge("uses_variable_group", context_node["id"], group_id,
                               f"{context_node['id']}.vargroup.{self._sanitize_id(group_name)}")
    
    def _extract_secure_file(self, secure_file_name: str) -> str:
        """Extract a secure file reference as a resource. Returns the resource ID."""
        sf_id = f"secureFile.{self._sanitize_id(secure_file_name)}"
        
        if sf_id not in self.resource_index:
            resource = {
                "id": sf_id,
                "kind": "secureFile",
                "name": secure_file_name,
                "platformId": None
            }
            node = {
                "id": sf_id,
                "kind": "secureFile",
                "name": secure_file_name,
                "platformId": None
            }
            self._add_resource(resource)
            self._add_node(node)
        
        return sf_id
    
    def _create_implicit_stage(self, stage_id: str, parent_id: str) -> Dict:
        """Create an implicit stage node when stages aren't explicitly defined."""
        return {
            "id": stage_id,
            "kind": "stage",
            "name": "default",
            "displayName": "Default Stage",
            "parent": parent_id,
            "children": [],
            "order": 1,
            "implicit": True
        }
    
    def _create_implicit_job(self, job_id: str, parent_id: str) -> Dict:
        """Create an implicit job node when jobs aren't explicitly defined."""
        return {
            "id": job_id,
            "kind": "job",
            "subtype": "job",
            "name": "default",
            "displayName": "Default Job",
            "parent": parent_id,
            "children": [],
            "order": 1,
            "implicit": True,
            "overridePool": False
        }
    
    def _normalize_variables(self, variables: Any) -> Dict:
        """Normalize variables to a consistent format."""
        if isinstance(variables, dict):
            return variables
        elif isinstance(variables, list):
            result = {}
            for var in variables:
                if isinstance(var, dict):
                    for key, value in var.items():
                        result[key] = value
            return result
        return {}
    
    def _get_step_name(self, step: Dict, idx: int) -> str:
        """Get step name for ID generation."""
        if "task" in step:
            return step.get("displayName") or step.get("task")
        elif "bash" in step:
            return step.get("displayName", "Bash")
        elif "pwsh" in step or "powershell" in step:
            return step.get("displayName", "PowerShell")
        elif "script" in step:
            return step.get("displayName", "Script")
        elif "checkout" in step:
            return f"Checkout {step.get('checkout', 'self')}"
        return f"step_{idx}"
    
    def _sanitize_id(self, name: str) -> str:
        """Sanitize a name for use in an ID."""
        import re
        # Replace spaces and special chars with underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_-]', '_', name)
        # Remove consecutive underscores
        sanitized = re.sub(r'_+', '_', sanitized)
        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')
        return sanitized.lower()
    
    def _add_resource(self, resource: Dict):
        """Add a resource if it doesn't already exist."""
        resource_id = resource["id"]
        if resource_id not in self.resource_index:
            self.resources.append(resource)
            self.resource_index[resource_id] = resource
    
    def _add_node(self, node: Dict):
        """Add a node if it doesn't already exist."""
        node_id = node["id"]
        if node_id not in self.node_index:
            self.nodes.append(node)
            self.node_index[node_id] = node
    
    def _add_edge(self, edge_type: str, from_id: str, to_id: str, context: str):
        """Add an edge between two nodes."""
        edge_id = f"edge.{edge_type}.{context}"
        edge = {
            "id": edge_id,
            "type": edge_type,
            "from": from_id,
            "to": to_id
        }
        self.edges.append(edge)

    # ------------------------------------------------------------------
    # Timeline execution enrichment (from Timeline API)
    # ------------------------------------------------------------------

    def enrich_with_timeline(self, structured: Dict, timeline_data: Dict, template_parameters: Dict = None) -> Dict:
        """
        Enrich a parsed pipeline with execution status from the build timeline.

        Annotates stage, job, and step nodes with ``executionResult``
        (succeeded / failed / skipped / canceled) and timing information
        from the Timeline API response.

        Timeline hierarchy:
            Stage (parentId=null) → Phase (parentId=stage) →
            Job (parentId=phase) → Task (parentId=job)

        The Phase ``refName`` maps to the YAML job name.  The Job record
        carries the agent ``workerName`` and actual execution times.
        """
        if not timeline_data or not isinstance(timeline_data, dict):
            if template_parameters:
                structured["templateParameters"] = template_parameters
            return structured

        records = timeline_data.get("records", [])
        if not records:
            if template_parameters:
                structured["templateParameters"] = template_parameters
            return structured

        # ---- index all records by id first (order-independent) ----
        rec_by_id: Dict[str, Dict] = {}
        stages: List[Dict] = []
        phases: List[Dict] = []
        jobs: List[Dict] = []
        tasks: List[Dict] = []

        for rec in records:
            rec_by_id[rec["id"]] = rec
            rtype = rec.get("type")
            if rtype == "Stage":
                stages.append(rec)
            elif rtype == "Phase":
                phases.append(rec)
            elif rtype == "Job":
                jobs.append(rec)
            elif rtype == "Task":
                tasks.append(rec)

        # ---- stage lookup: refName → record ----
        stages_by_ref: Dict[str, Dict] = {}
        for s in stages:
            ref = s.get("refName") or ""
            stages_by_ref[ref] = s

        # ---- phase lookup: (stage_refName, phase_refName) → record ----
        # Resolve parentId → Stage via rec_by_id (order-independent)
        phases_by_key: Dict[tuple, Dict] = {}
        for phase in phases:
            parent = rec_by_id.get(phase.get("parentId", ""), {})
            stage_ref = parent.get("refName", "") if parent.get("type") == "Stage" else ""
            phase_ref = phase.get("refName", "")
            phases_by_key[(stage_ref, phase_ref)] = phase

        # ---- job lookup: (stage_refName, phase_refName) → job record ----
        # The Job record has workerName, actual start/finish, result.
        jobs_by_key: Dict[tuple, Dict] = {}
        for job in jobs:
            phase = rec_by_id.get(job.get("parentId", ""), {})
            if phase.get("type") != "Phase":
                continue
            stage_rec = rec_by_id.get(phase.get("parentId", ""), {})
            stage_ref = stage_rec.get("refName", "") if stage_rec.get("type") == "Stage" else ""
            phase_ref = phase.get("refName", "")
            jobs_by_key[(stage_ref, phase_ref)] = job

        # ---- task lookup: parent job id → [task records sorted by order] ----
        tasks_by_job: Dict[str, List[Dict]] = {}
        for task in tasks:
            tasks_by_job.setdefault(task.get("parentId"), []).append(task)
        for pid in tasks_by_job:
            tasks_by_job[pid].sort(key=lambda t: t.get("order", 0))

        # ---- helpers ----
        def _timeline_info(rec: Dict) -> Dict:
            """Build a compact execution record from a timeline record."""
            info: Dict[str, Any] = {
                "result": rec.get("result"),
                "state": rec.get("state"),
            }
            if rec.get("startTime"):
                info["startTime"] = rec["startTime"]
            if rec.get("finishTime"):
                info["finishTime"] = rec["finishTime"]
            if rec.get("errorCount"):
                info["errorCount"] = rec["errorCount"]
            if rec.get("warningCount"):
                info["warningCount"] = rec["warningCount"]
            if rec.get("issues"):
                info["issues"] = rec["issues"]
            if rec.get("resultCode"):
                info["resultCode"] = rec["resultCode"]
            if rec.get("workerName"):
                info["workerName"] = rec["workerName"]
            if rec.get("task"):
                info["task"] = rec["task"]
            if rec.get("identifier"):
                info["identifier"] = rec["identifier"]
            return info

        def _match_step_to_task(step_node: Dict, job_tasks: List[Dict],
                                already_matched: set) -> Optional[Dict]:
            """
            Match a recipe step to a timeline Task record.

            Tries in order:
            1. Exact display-name match
            2. task.name match (uses.name == timeline task.name),
               picking the first unmatched one (preserves order for
               duplicate task types like CmdLine1, CmdLine2 …)
            3. refName prefix match (refName starts with uses.name)
            """
            step_display = (step_node.get("displayName")
                            or step_node.get("name", "")).lower()
            step_uses_name = step_node.get("uses", {}).get("name", "").lower()

            # 1. exact display-name match
            for t in job_tasks:
                if t["id"] in already_matched:
                    continue
                if step_display and t.get("name", "").lower() == step_display:
                    return t

            # 2. task.name match (first-unmatched with same task type)
            if step_uses_name:
                for t in job_tasks:
                    if t["id"] in already_matched:
                        continue
                    tinfo = t.get("task") or {}
                    if tinfo.get("name", "").lower() == step_uses_name:
                        return t

            # 3. refName prefix match
            if step_uses_name:
                for t in job_tasks:
                    if t["id"] in already_matched:
                        continue
                    ref = (t.get("refName") or "").lower()
                    if ref and (ref == step_uses_name
                                or ref.rstrip("0123456789") == step_uses_name):
                        return t

            return None

        # ---- annotate recipe nodes ----
        nodes = structured.get("nodes", [])
        # Track matched task ids per job so we don't double-match
        # (e.g. CmdLine1 matched to first script step,
        #  CmdLine2 to second, etc.)
        matched_task_ids: set = set()

        # Process steps after their parent jobs so we can resolve the
        # job's timeline Job record first.  Stages and jobs are
        # processed in-order; steps are collected and processed per-job.
        steps_by_parent: Dict[str, List[Dict]] = {}
        for node in nodes:
            kind = node.get("kind")
            name = node.get("name")
            if not name:
                continue

            if kind == "stage":
                lookup = name if name != "default" else "__default"
                rec = stages_by_ref.get(lookup) or stages_by_ref.get(name)
                if rec:
                    node["executionResult"] = rec.get("result")
                    node["timelineRecord"] = _timeline_info(rec)

            elif kind == "job":
                parent_id = node.get("parent", "")
                stage_name = (parent_id.replace("stage.", "", 1)
                              if parent_id.startswith("stage.") else "")
                phase_ref = name if name != "default" else "__default"

                # Prefer the Job record (has workerName / agent info),
                # fall back to Phase
                job_rec = jobs_by_key.get((stage_name, phase_ref))
                phase_rec = phases_by_key.get((stage_name, phase_ref))
                match_rec = job_rec or phase_rec
                if match_rec:
                    node["executionResult"] = match_rec.get("result")
                    node["timelineRecord"] = _timeline_info(match_rec)

            elif kind == "step":
                steps_by_parent.setdefault(node.get("parent", ""), []).append(node)

        # Match steps to timeline Task records per job
        for parent_id, step_nodes in steps_by_parent.items():
            parts = parent_id.split(".")
            if len(parts) < 4 or parts[0] != "job" or parts[2] != "stage":
                continue
            job_name = parts[1]
            stage_name = ".".join(parts[3:])
            phase_ref = job_name if job_name != "default" else "__default"

            job_rec = jobs_by_key.get((stage_name, phase_ref))
            if not job_rec:
                continue
            job_tasks = tasks_by_job.get(job_rec["id"], [])
            if not job_tasks:
                continue

            # Process steps in recipe order so that order-dependent
            # matching (CmdLine1→first script, CmdLine2→second) works.
            per_job_matched: set = set()
            for step_node in sorted(step_nodes,
                                    key=lambda s: s.get("order", 0)):
                matched = _match_step_to_task(step_node, job_tasks,
                                              per_job_matched)
                if matched:
                    step_node["executionResult"] = matched.get("result")
                    step_node["timelineRecord"] = _timeline_info(matched)
                    per_job_matched.add(matched["id"])

        # ---- execution summary ----
        execution_summary: Dict[str, Any] = {
            "stageCount": len(stages),
            "stages": [],
        }
        for sr in sorted(stages, key=lambda r: r.get("order", 0)):
            stage_ref = sr.get("refName", "")
            stage_entry: Dict[str, Any] = {
                "name": sr.get("name"),
                "refName": stage_ref,
                "result": sr.get("result"),
                "state": sr.get("state"),
                "order": sr.get("order"),
            }
            if sr.get("startTime"):
                stage_entry["startTime"] = sr["startTime"]
            if sr.get("finishTime"):
                stage_entry["finishTime"] = sr["finishTime"]

            # Per-stage job records (agent-level detail)
            stage_jobs = []
            for (s_ref, p_ref), job_rec in jobs_by_key.items():
                if s_ref != stage_ref:
                    continue
                phase_rec = phases_by_key.get((s_ref, p_ref), {})
                job_entry: Dict[str, Any] = {
                    "name": job_rec.get("name"),
                    "phaseRefName": p_ref,
                    "result": job_rec.get("result"),
                    "state": job_rec.get("state"),
                    "workerName": job_rec.get("workerName"),
                }
                if job_rec.get("startTime"):
                    job_entry["startTime"] = job_rec["startTime"]
                if job_rec.get("finishTime"):
                    job_entry["finishTime"] = job_rec["finishTime"]
                if job_rec.get("errorCount"):
                    job_entry["errorCount"] = job_rec["errorCount"]
                if job_rec.get("warningCount"):
                    job_entry["warningCount"] = job_rec["warningCount"]
                if job_rec.get("identifier"):
                    job_entry["identifier"] = job_rec["identifier"]
                stage_jobs.append(job_entry)
            if stage_jobs:
                stage_entry["jobs"] = stage_jobs

            execution_summary["stages"].append(stage_entry)

        structured["timelineExecution"] = execution_summary

        # ---- attach templateParameters ----
        if template_parameters:
            structured["templateParameters"] = template_parameters

        return structured

    # ------------------------------------------------------------------
    # Template evaluation enrichment (from /2 log)
    # ------------------------------------------------------------------

    def enrich_with_template_evaluation(self, structured: Dict, eval_summary) -> Dict:
        """
        Enrich a parsed pipeline with template evaluation data from the /2 log.

        Adds ``templateEvaluation`` metadata and annotates stage/job nodes that
        have a ``condition`` field with ``conditionEvaluation`` and
        ``executionStatus`` (``skipped`` or ``executed``) when the condition can
        be statically resolved.
        """
        if eval_summary is None:
            return structured

        # Serialise the evaluation summary
        eval_data = {
            "rootTemplate": eval_summary.root_template,
            "fileCount": eval_summary.file_count,
            "maxDepth": eval_summary.max_depth,
            "loadTime": eval_summary.load_time,
            "memoryEstimate": eval_summary.memory_estimate,
            "templates": [],
            "conditionEvaluations": [],
        }

        for tpl in (eval_summary.templates_used or []):
            eval_data["templates"].append({
                "path": tpl.path,
                "repoAlias": tpl.repo_alias,
                "loadType": tpl.load_type,
                "stepCount": tpl.step_count,
                "parameters": tpl.parameters,
            })

        # Collect condition strings produced by format() calls
        for trace in (eval_summary.evaluations or []):
            if not trace.expression.startswith("format("):
                continue
            result_str = str(trace.result).strip("'\"") if trace.result is not None else ""
            result_str = result_str.replace("''", "'")
            _COND_KEYWORDS = ("succeeded", "failed", "eq(", "ne(", "and(", "or(", "in(")
            if any(kw in result_str.lower() for kw in _COND_KEYWORDS):
                static_result = self._try_static_condition_eval(result_str)
                eval_data["conditionEvaluations"].append({
                    "formatExpression": trace.expression,
                    "resolvedCondition": result_str,
                    "staticResult": static_result,
                })

        # Match conditions on nodes to their evaluated forms
        nodes = structured.get("nodes", [])
        cond_evals = eval_data["conditionEvaluations"]
        for node in nodes:
            if "condition" not in node:
                continue
            node_cond = self._normalize_condition(node["condition"])
            matched = False
            for ce in cond_evals:
                resolved_norm = self._normalize_condition(ce["resolvedCondition"])
                if node_cond == resolved_norm:
                    node["conditionEvaluation"] = {
                        "resolvedCondition": ce["resolvedCondition"],
                        "staticResult": ce["staticResult"],
                    }
                    if ce["staticResult"] is True:
                        node["executionStatus"] = "executed"
                    elif ce["staticResult"] is False:
                        node["executionStatus"] = "skipped"
                    matched = True
                    break

            # If no /2 log match, try static evaluation directly on the
            # rendered condition from the /1 YAML (already parameter-resolved)
            if not matched:
                static_result = self._try_static_condition_eval(node["condition"])
                node["conditionEvaluation"] = {
                    "resolvedCondition": node["condition"],
                    "staticResult": static_result,
                }
                if static_result is True:
                    node["executionStatus"] = "executed"
                elif static_result is False:
                    node["executionStatus"] = "skipped"

        structured["templateEvaluation"] = eval_data
        return structured

    @staticmethod
    def _normalize_condition(condition_str: str) -> str:
        """Normalise a condition string for comparison."""
        import re as _re
        s = condition_str.strip().strip("'\"")
        s = s.replace("''", "'")
        s = _re.sub(r"\s+", " ", s).strip()
        return s.lower()

    @staticmethod
    def _try_static_condition_eval(condition_str: str):
        """
        Attempt to statically evaluate a condition string.

        Handles the common Azure DevOps expression functions:
        ``eq``, ``ne``, ``and``, ``or``, ``succeeded``, ``failed``,
        ``true``, ``false``.

        Returns ``True``, ``False``, or ``None`` (indeterminate).
        """
        import re as _re

        s = condition_str.strip().strip("'\"").replace("''", "'")

        def _eval(expr: str):
            expr = expr.strip()

            # Boolean literals
            if expr.lower() in ("true", "1"):
                return True
            if expr.lower() in ("false", "0"):
                return False

            # succeeded() / failed() – assume succeeded for static analysis
            if expr.lower().startswith("succeeded("):
                return True
            if expr.lower().startswith("failed("):
                return False

            # eq('a', 'b')
            m = _re.match(r"eq\(\s*'([^']*)'\s*,\s*'([^']*)'\s*\)", expr, _re.I)
            if m:
                return m.group(1).lower() == m.group(2).lower()

            # ne('a', 'b')
            m = _re.match(r"ne\(\s*'([^']*)'\s*,\s*'([^']*)'\s*\)", expr, _re.I)
            if m:
                return m.group(1).lower() != m.group(2).lower()

            # and(expr1, expr2, ...)
            m = _re.match(r"and\(\s*(.+)\s*\)", expr, _re.I | _re.S)
            if m:
                args = _split_args(m.group(1))
                results = [_eval(a) for a in args]
                if False in results:
                    return False
                if all(r is True for r in results):
                    return True
                return None

            # or(expr1, expr2, ...)
            m = _re.match(r"or\(\s*(.+)\s*\)", expr, _re.I | _re.S)
            if m:
                args = _split_args(m.group(1))
                results = [_eval(a) for a in args]
                if True in results:
                    return True
                if all(r is False for r in results):
                    return False
                return None

            # Anything containing runtime variables – indeterminate
            return None

        def _split_args(args_str: str) -> list:
            """Split top-level comma-separated arguments respecting parentheses."""
            parts = []
            depth = 0
            current: list[str] = []
            for ch in args_str:
                if ch == "(":
                    depth += 1
                    current.append(ch)
                elif ch == ")":
                    depth -= 1
                    current.append(ch)
                elif ch == "," and depth == 0:
                    parts.append("".join(current).strip())
                    current = []
                else:
                    current.append(ch)
            tail = "".join(current).strip()
            if tail:
                parts.append(tail)
            return parts

        try:
            return _eval(s)
        except Exception:
            return None
