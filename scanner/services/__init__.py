#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

from scanner.services.azuredevops.artifacts import ArtifactsService
from scanner.services.azuredevops.identities import IdentitiesService
from scanner.services.azuredevops.pipelines import PipelinesService
from scanner.services.azuredevops.projects import ProjectsService
from scanner.services.azuredevops.repositories import RepositoriesService
from scanner.services.azuredevops.resources import ResourcesService
from scanner.services.azuredevops.stats import StatsService
from scanner.services.azuredevops.tasks import TasksService
from scanner.services.azuredevops.users import UsersService

__all__ = [
    "ArtifactsService",
    "IdentitiesService",
    "PipelinesService",
    "ProjectsService",
    "RepositoriesService",
    "ResourcesService",
    "StatsService",
    "TasksService",
    "UsersService",
]
