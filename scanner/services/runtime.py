#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

from collections import defaultdict
from dataclasses import dataclass, field
from threading import Lock
from typing import Any


@dataclass
class RuntimeIndexes:
    wellformed_project_ids: list[str] = field(default_factory=list)
    definitions_by_project_id: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    definition_keys_by_project_id: dict[str, list[str]] = field(default_factory=dict)
    builds_by_definition_key: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    builds_by_repo_id: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    definitions_by_repo_id: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    project_id_by_project_name: dict[str, str] = field(default_factory=dict)


@dataclass
class PerfCounters:
    get_total: int = 0
    post_total: int = 0
    by_family_get: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    by_family_post: dict[str, int] = field(default_factory=lambda: defaultdict(int))


@dataclass
class ScanRuntimeState:
    indexes: RuntimeIndexes = field(default_factory=RuntimeIndexes)
    perf: PerfCounters = field(default_factory=PerfCounters)
    regex_patterns_cache: dict[str, list[Any]] = field(default_factory=dict)
    regex_patterns_loaded: bool = False
    branch_cache: dict[tuple, tuple] = field(default_factory=dict)
    perf_lock: Lock = field(default_factory=Lock)
    branch_cache_lock: Lock = field(default_factory=Lock)


def endpoint_family(url: str) -> str:
    if not isinstance(url, str):
        return "other"
    if "/_apis/projects" in url:
        return "projects"
    if "/_apis/build/definitions" in url:
        return "build_definitions"
    if "/_apis/build/builds" in url:
        return "builds"
    if "/_apis/pipelines/pipelinepermissions/" in url:
        return "pipelinepermissions"
    if "/_apis/pipelines/checks/" in url:
        return "checks"
    if "/_apis/git/" in url:
        return "repos"
    if "feeds.dev.azure.com" in url or "pkgs.dev.azure.com" in url:
        return "artifacts"
    if "vssps.dev.azure.com" in url:
        return "graph"
    return "other"


def ordered_dedupe(items):
    seen = set()
    result = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def normalize_to_list(data):
    if data is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        value = data.get("value")
        if isinstance(value, list):
            return value
    return []


def extract_owner_project_id(resource):
    k_project = resource.get("k_project")
    if isinstance(k_project, dict):
        if "id" in k_project:
            return k_project.get("id")
        if len(k_project) == 1:
            first_value = next(iter(k_project.values()))
            if isinstance(first_value, dict):
                return first_value.get("id")
    if resource.get("projectId"):
        return resource.get("projectId")
    project_info = resource.get("project", {})
    if isinstance(project_info, dict):
        return project_info.get("id")
    return None
