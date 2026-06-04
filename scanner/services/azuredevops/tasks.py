#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

import logging

logger = logging.getLogger(__name__)


class TasksService:
    def __init__(self, manager, http_ops):
        self.manager = manager
        self.http_ops = http_ops

    def get_task_list(self):
        url = f"https://dev.azure.com/{self.manager.organization}/_apis/distributedtask/tasks?api-version=7.1"
        try:
            tasks = self.http_ops.fetch_data(url)
            if tasks is None:
                logger.warning("Failed to fetch task list")
                return []
            return tasks
        except Exception as err:
            logger.error(f"Error retrieving task list: {err}")
            return []
