#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

def filter_user_fields(user):
    if not isinstance(user, dict):
        return user
    keys = ["displayName", "url", "id", "uniqueName"]
    return {k: user[k] for k in keys if k in user}

def filter_repository(repo):
    if not isinstance(repo, dict):
        return repo
    filtered = {k: repo[k] for k in ["id", "url", "defaultBranch"] if k in repo}
    if "properties" in repo and isinstance(repo["properties"], dict):
        filtered["properties"] = {"cloneUrl": repo["properties"].get("cloneUrl")}
    return filtered


def _normalize_project(record, organization=None):
    """Replace fat project object and k_project with a project_id reference."""
    project_id = ""
    k_project = record.get("k_project")
    project = record.get("project")
    if isinstance(k_project, dict) and k_project.get("id"):
        project_id = k_project["id"]
    elif isinstance(project, dict) and project.get("id"):
        project_id = project["id"]

    if project_id:
        record["project_id"] = project_id
    record.pop("k_project", None)
    record.pop("project", None)
    return project_id


def filter_builds(builds, organization=None):
    filtered_builds = []
    for build in builds:
        if isinstance(build, dict):
            for field in ["requestedBy", "lastChangedBy", "requestedFor"]:
                if field in build:
                    build[field] = filter_user_fields(build[field])

            project_id = _normalize_project(build, organization)

            # Normalize the fat definition object to a pipeline reference.
            definition = build.get("definition")
            if isinstance(definition, dict) and definition.get("id") is not None:
                def_id = str(definition["id"])
                pipeline_ref = f"{organization}/{project_id}/{def_id}" if organization else f"{project_id}/{def_id}"
                build["k_pipeline_ref"] = pipeline_ref
                build["definition"] = {
                    "id": definition["id"],
                    "name": definition.get("name"),
                }

            filtered_builds.append(build)

    return filtered_builds

def filter_definitions(definitions, organization=None):
    filtered_definitions = []
    for defn in definitions:
        if isinstance(defn, dict):
            if "repository" in defn:
                defn["repository"] = filter_repository(defn["repository"])
            if "authoredBy" in defn:
                defn["authoredBy"] = filter_user_fields(defn["authoredBy"])

            project_id = _normalize_project(defn, organization)

            # Add canonical pipeline reference ID
            def_id = str(defn.get("id", ""))
            defn["k_pipeline_ref"] = f"{organization}/{project_id}/{def_id}" if organization else f"{project_id}/{def_id}"

            filtered_definitions.append(defn)
    return filtered_definitions



def filter_protected_resources(resources, organization=None):
    filtered_resources = []
    for resource in resources:
        if isinstance(resource, dict):
            for field in ["createdBy", "modifiedBy", "owner"]:
                if field in resource:
                    resource[field] = filter_user_fields(resource[field])
            inner = resource.get("resource")
            if isinstance(inner, dict):
                _normalize_project(inner, organization)
            filtered_resources.append(resource)
    return filtered_resources
