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


def filter_builds(builds):
    filtered_builds = []
    for build in builds:
        if isinstance(build, dict):
            for field in ["requestedBy", "lastChangedBy", "requestedFor"]:
                if field in build:
                    build[field] = filter_user_fields(build[field])
            filtered_builds.append(build)

    return filtered_builds

def filter_definitions(definitions):
    filtered_definitions = []
    for defn in definitions:
        if isinstance(defn, dict):
            if "repository" in defn:
                defn["repository"] = filter_repository(defn["repository"])
            if "authoredBy" in defn:
                defn["authoredBy"] = filter_user_fields(defn["authoredBy"])
            filtered_definitions.append(defn)
    return filtered_definitions



def filter_protected_resources(resources):
    filtered_resources = []
    for resource in resources:
        if isinstance(resource, dict):
            for field in ["createdBy", "modifiedBy", "owner"]:
                if field in resource:
                    resource[field] = filter_user_fields(resource[field])
            filtered_resources.append(resource)
    return filtered_resources
