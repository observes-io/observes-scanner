#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

import logging
from scanner.services.runtime import normalize_to_list

logger = logging.getLogger(__name__)


class ArtifactsService:
    def __init__(self, manager, http_ops, logger=None):
        self.manager = manager
        self.http_ops = http_ops
        self.logger = logger or logging.getLogger(__name__)

    def get_feed_packages(self, feed_id, project_id=None):
        if project_id:
            packages_url = f"https://feeds.dev.azure.com/{self.manager.organization}/{project_id}/_apis/packaging/feeds/{feed_id}/packages?api-version=7.1-preview.1&includeUrls=false"
        else:
            packages_url = f"https://feeds.dev.azure.com/{self.manager.organization}/_apis/packaging/feeds/{feed_id}/packages?api-version=7.1-preview.1&includeUrls=false"
        try:
            packages = self.http_ops.fetch_data(packages_url)
            package_list = normalize_to_list(packages)
            for pkg in package_list:
                protocol = (pkg.get("protocolType") or "").lower()
                if protocol in ("maven", "nuget", "npm", "python"):
                    if project_id:
                        base_url = f"https://pkgs.dev.azure.com/{self.manager.organization}/{project_id}/_apis/packaging/feeds/{feed_id}"
                    else:
                        base_url = f"https://pkgs.dev.azure.com/{self.manager.organization}/_apis/packaging/feeds/{feed_id}"
                    if protocol == "maven":
                        upstream_url = f"{base_url}/maven/packages/{pkg.get('name')}/upstreaming"
                    elif protocol == "nuget":
                        upstream_url = f"{base_url}/nuget/packages/{pkg.get('name')}/upstreaming"
                    elif protocol == "npm":
                        upstream_url = f"{base_url}/npm/packages/{pkg.get('name')}/upstreaming"
                    else:
                        upstream_url = f"{base_url}/python/packages/{pkg.get('name')}/upstreaming"
                    try:
                        behaviour = self.http_ops.fetch_data(upstream_url)
                        if isinstance(behaviour, dict) and "versionsFromExternalUpstreams" in behaviour:
                            pkg["versionsFromExternalUpstreams"] = behaviour["versionsFromExternalUpstreams"]
                        else:
                            pkg["versionsFromExternalUpstreams"] = behaviour
                    except Exception as e:
                        pkg["versionsFromExternalUpstreams"] = {"error": str(e)}
            return package_list
        except Exception as e:
            self.logger.warning(f"Failed to fetch packages for feed {feed_id} (project: {project_id}): {e}")
            return []

    def get_feed_views(self, feed_id, project_id=None):
        if project_id:
            views_url = f"https://feeds.dev.azure.com/{self.manager.organization}/{project_id}/_apis/packaging/feeds/{feed_id}/views?api-version=7.1-preview.1"
        else:
            views_url = f"https://feeds.dev.azure.com/{self.manager.organization}/_apis/packaging/feeds/{feed_id}/views?api-version=7.1-preview.1"
        try:
            views = self.http_ops.fetch_data(views_url)
            return views if isinstance(views, list) else views.get("value", views) if views else []
        except Exception as e:
            self.logger.warning(f"Failed to fetch views for feed {feed_id} (project: {project_id}): {e}")
            return []

    def get_artifacts_feeds(self):
        feeds = {"active": [], "recyclebin": []}
        org_url = f"https://feeds.dev.azure.com/{self.manager.organization}/_apis/packaging/feeds?api-version=7.1"
        org_recyclebin_url = (
            f"https://feeds.dev.azure.com/{self.manager.organization}/_apis/packaging/feedRecycleBin?api-version=7.1-preview.1"
        )
        try:
            org_feeds = self.http_ops.fetch_data(org_url)
            org_feeds_list = normalize_to_list(org_feeds)
            for feed in org_feeds_list:
                feed["k_enabled"] = True
                feed_id = feed.get("id") or feed.get("name")
                project = feed.get("project")
                if project:
                    feed["k_feed_type"] = "project"
                    feed["k_project"] = self.manager.enrich_k_project(
                        project.get("id"),
                        f"https://dev.azure.com/{self.manager.organization}/{project.get('name')}/_artifacts/feed/{feed.get('name')}",
                    )
                else:
                    feed["k_feed_type"] = "organization"
                feed["views"] = self.get_feed_views(feed_id, project_id=project.get("id") if project else None)
                feed["packages"] = self.get_feed_packages(feed_id, project_id=project.get("id") if project else None)
            if org_feeds:
                feeds["active"] = org_feeds_list
        except Exception as e:
            self.logger.warning(f"Failed to fetch organization feeds: {e}")

        try:
            org_recyclebin = self.http_ops.fetch_data(org_recyclebin_url)
            org_recyclebin_list = normalize_to_list(org_recyclebin)
            for feed in org_recyclebin_list:
                feed["k_feed_type"] = "recyclebin"
                project = feed.get("project")
                if project:
                    feed["k_project"] = self.manager.enrich_k_project(
                        project.get("id"),
                        f"https://dev.azure.com/{self.manager.organization}/{project.get('name')}/_artifacts/feed/{feed.get('name')}",
                    )
                feed["k_enabled"] = False
            if org_recyclebin:
                feeds["recyclebin"] = org_recyclebin_list
        except Exception as e:
            self.logger.warning(f"Failed to fetch organization recycle bin feeds: {e}")

        return feeds
