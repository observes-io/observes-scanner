#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

class IdentitiesService:
    def __init__(self, manager, http_ops):
        self.manager = manager
        self.http_ops = http_ops

    def get_all_build_service_accounts(self):
        url = f"https://vssps.dev.azure.com/{self.manager.organization}/_apis/graph/users?api-version=7.1-preview.1&subjectTypes=svc"
        users = self.http_ops.fetch_data(url)
        results = []
        if not users:
            return results
        for user in users:
            if user.get("domain") != "Build":
                continue
            principal_name = user.get("principalName", "")
            display_name = user.get("displayName", "")
            suffix = f" Build Service ({self.manager.organization})"
            if display_name.endswith(suffix):
                project_name = display_name[: -len(suffix)].strip()
            else:
                project_name = ""
            description = f"A build service account for project {project_name}."
            results.append({"id": f"Build\\{principal_name}", "name": display_name, "description": description})
        return results
