#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


class RepositoriesService:
    def __init__(self, manager, http_ops, runtime_state, logger=None):
        self.manager = manager
        self.http_ops = http_ops
        self.runtime_state = runtime_state
        self.logger = logger or logging.getLogger(__name__)

    def enrich_repositories_with_committer_stats(self, protected_resources, commits):
        repo_committers = {}
        commits_count = {}
        for commit in commits:
            repo_id = commit.get("repositoryId")
            committer_email = commit.get("committerEmail")
            if not repo_id or not committer_email:
                continue
            if repo_id not in repo_committers:
                repo_committers[repo_id] = set()
            if repo_id not in commits_count:
                commits_count[repo_id] = 0
            repo_committers[repo_id].add(committer_email)
            commits_count[repo_id] += 1

        for repo_resource in protected_resources:
            repo = repo_resource["resource"]
            repo_id = repo.get("id")
            unique_committers = sorted(list(repo_committers.get(repo_id, set())))
            if "stats" not in repo:
                repo["stats"] = {}
            repo["stats"]["committers"] = {
                "totalCommits": commits_count.get(repo_id, 0),
                "count": len(unique_committers),
                "uniqueCommitters": unique_committers,
            }
        return protected_resources

    def get_committer_stats(self, commits, build_service_accounts):
        stats = {}
        project_commits = {}
        build_service_account_map = {acc["id"]: acc["name"] for acc in build_service_accounts}
        special_prs_email = "00000002-0000-8888-8000-000000000000@2c895908-04e0-4952-89fd-54b0046d6288"
        for commit in commits:
            committer_email = commit.get("committerEmail")
            repo_id = commit.get("repositoryId")
            project_id = commit.get("projectId")
            project_name = commit.get("k_project", {}).get("name") if commit.get("k_project") else None
            author_email = commit.get("authorEmail")
            push_email = commit.get("pushEmail")
            change_counts = commit.get("changeCounts", {})
            if not committer_email:
                continue
            if committer_email not in stats:
                stats[committer_email] = {
                    "commitCount": 0,
                    "repos": set(),
                    "totalChangeCounts": {"add": 0, "edit": 0, "delete": 0},
                    "authorEmails": set(),
                    "pusherEmails": set(),
                    "projects": set(),
                    "projectStats": [],
                    "prs_merged": 0,
                }
            entry = stats[committer_email]
            entry["commitCount"] += 1
            if repo_id:
                entry["repos"].add(repo_id)
            if project_id:
                entry["projects"].add(project_id)
            entry["totalChangeCounts"]["add"] += change_counts.get("add", 0)
            entry["totalChangeCounts"]["edit"] += change_counts.get("edit", 0)
            entry["totalChangeCounts"]["delete"] += change_counts.get("delete", 0)
            if author_email:
                entry["authorEmails"].add(author_email)
            if push_email:
                entry["pusherEmails"].add(push_email)
                if push_email == special_prs_email:
                    entry["prs_merged"] += 1
            if committer_email not in project_commits:
                project_commits[committer_email] = {}
            if project_id:
                if project_id not in project_commits[committer_email]:
                    project_commits[committer_email][project_id] = {
                        "repo_ids": set(),
                        "commit_count": 0,
                        "project_name": project_name,
                    }
                project_commits[committer_email][project_id]["commit_count"] += 1
                if repo_id:
                    project_commits[committer_email][project_id]["repo_ids"].add(repo_id)

        for committer_email, entry in stats.items():
            entry["repoCount"] = len(entry["repos"])
            entry["projectCount"] = len(entry["projects"])
            entry["authorEmails"] = list(entry["authorEmails"])
            new_pusher_emails = []
            uses_build_service_account = False
            for email in entry["pusherEmails"]:
                if email == special_prs_email:
                    continue
                if email.startswith("Build\\") and email in build_service_account_map:
                    uses_build_service_account = True
                    new_pusher_emails.append(build_service_account_map[email])
                else:
                    new_pusher_emails.append(email)
            entry["pusherEmails"] = new_pusher_emails
            entry["usesBuildServiceAccount"] = 1 if uses_build_service_account else 0
            entry["hasMultipleAuthors"] = 1 if len(entry["authorEmails"]) > 1 else 0
            entry["hasMultiplePushers"] = 1 if len(entry["pusherEmails"]) > 1 else 0
            entry["projectStats"] = []
            for project_id, proj_stats in project_commits.get(committer_email, {}).items():
                entry["projectStats"].append(
                    {
                        "projectId": project_id,
                        "projectName": proj_stats["project_name"],
                        "repoCount": len(proj_stats["repo_ids"]),
                        "commitCount": proj_stats["commit_count"],
                    }
                )
            del entry["repos"]
            del entry["projects"]
        return stats

    def get_commits_per_repository(self, protected_resources):
        all_commits = []
        now = datetime.utcnow()
        since = now - timedelta(days=90)
        since_iso = since.strftime("%Y-%m-%dT%H:%M:%SZ")
        batch_size = 100
        for repo_resource in protected_resources:
            repo = repo_resource["resource"]
            project_id = repo["project"]["id"]
            repo_id = repo["id"]
            k_project = repo.get("k_project")
            skip = 0
            while True:
                url = (
                    f"https://dev.azure.com/{self.manager.organization}/{project_id}/_apis/git/repositories/{repo_id}/commits"
                    f"?searchCriteria.fromDate={since_iso}"
                    f"&$top={batch_size}"
                    f"&$skip={skip}"
                    f"&searchCriteria.includePushData=true"
                    f"&api-version=7.1"
                )
                commits = self.http_ops.fetch_data(url)
                if not commits:
                    break
                for commit in commits:
                    author = commit.get("author", {})
                    committer = commit.get("committer", {})
                    push = commit.get("push", {})
                    change_counts = commit.get("changeCounts", {})
                    author_email = author.get("email")
                    author_name = author.get("name")
                    committer_email = committer.get("email")
                    committer_name = committer.get("name")
                    committer_date = committer.get("date")
                    push_email = push.get("pushedBy", {}).get("uniqueName")
                    push_name = push.get("pushedBy", {}).get("displayName")
                    push_id = push.get("pushId")
                    push_date = push.get("date")
                    add_count = change_counts.get("Add", 0)
                    edit_count = change_counts.get("Edit", 0)
                    delete_count = change_counts.get("Delete", 0)
                    committer_author_match = 1 if committer_name == author_name else 0
                    committer_pusher_match = 1 if committer_name == push_name else 0
                    commit_by_ado = 1 if push_email == "00000002-0000-8888-8000-000000000000@2c895908-04e0-4952-89fd-54b0046d6288" else 0
                    all_commits.append(
                        {
                            "repositoryId": repo_id,
                            "repositoryName": repo.get("name"),
                            "projectId": project_id,
                            "k_project": k_project,
                            "commitId": commit.get("commitId"),
                            "authorEmail": author_email,
                            "authorName": author_name,
                            "committerEmail": committer_email,
                            "committerName": committer_name,
                            "committerDate": committer_date,
                            "changeCounts": {"add": add_count, "edit": edit_count, "delete": delete_count},
                            "pushEmail": push_email,
                            "pushId": push_id,
                            "pushDate": push_date,
                            "committerAuthorMatch": committer_author_match,
                            "committerPusherMatch": committer_pusher_match,
                            "commitByAdo": commit_by_ado,
                        }
                    )
                if len(commits) < batch_size:
                    break
                skip += batch_size
        return all_commits

    def get_repository_pull_requests_count(self, project_id, repo_id):
        counts = {"active": 0, "abandoned": 0, "completed": 0, "other": 0, "all": 0}
        batch_size = 100
        skip = 0
        try:
            while True:
                url = f"https://dev.azure.com/{self.manager.organization}/{project_id}/_apis/git/repositories/{repo_id}/pullrequests?searchCriteria.status=all&$top={batch_size}&$skip={skip}&api-version=7.1"
                pr_list = self.http_ops.fetch_data(url)
                if not pr_list:
                    break
                for pr in pr_list:
                    status = (pr.get("status") or "").lower()
                    if status == "active":
                        counts["active"] += 1
                    elif status == "abandoned":
                        counts["abandoned"] += 1
                    elif status == "completed":
                        counts["completed"] += 1
                    elif status:
                        counts["other"] += 1
                    counts["all"] += 1
                if len(pr_list) < batch_size:
                    break
                skip += batch_size
            return counts
        except Exception as e:
            self.logger.warning(f"Error fetching pull requests for repository {repo_id}: {e}")
            return counts

    def get_repository_commit_dates(self, project_id, repo_id):
        latest_commit_url = f"https://dev.azure.com/{self.manager.organization}/{project_id}/_apis/git/repositories/{repo_id}/commits?searchCriteria.$top=1&api-version=7.1"
        latest_commit_list = self.http_ops.fetch_data(latest_commit_url)
        last_commit_date = None
        first_commit_date = None
        if latest_commit_list and len(latest_commit_list) > 0:
            last_commit_date_str = latest_commit_list[0].get("committer", {}).get("date")
            if last_commit_date_str:
                last_commit_date = datetime.fromisoformat(last_commit_date_str.replace("Z", "+00:00"))
        first_commit_url = f"https://dev.azure.com/{self.manager.organization}/{project_id}/_apis/git/repositories/{repo_id}/commits?searchCriteria.$top=1&searchCriteria.showOldestCommitsFirst=true&api-version=7.1"
        first_commit_list = self.http_ops.fetch_data(first_commit_url)
        if first_commit_list and len(first_commit_list) > 0:
            first_commit_date_str = first_commit_list[0].get("committer", {}).get("date")
            if first_commit_date_str:
                first_commit_date = datetime.fromisoformat(first_commit_date_str.replace("Z", "+00:00"))
        return (first_commit_date, last_commit_date)

    def _get_repository_branches_uncached(
        self, source_project_id, repo_id, project_name, repo_name, top_branches_to_scan, default_branch_name
    ):
        all_branches = None
        continuation_token = None

        if top_branches_to_scan is None:
            top_branches_to_scan = 0

        total_branches_to_fetch = top_branches_to_scan

        if top_branches_to_scan <= -1:
            total_branches_to_fetch = 1000
            top = 100
        elif top_branches_to_scan == 0:
            top = 1
        else:
            top = top_branches_to_scan if top_branches_to_scan < 100 else 100

        while True:
            if all_branches is None:
                all_branches = []
            elif total_branches_to_fetch >= len(all_branches):
                break
            elif total_branches_to_fetch - len(all_branches) < top:
                top = total_branches_to_fetch - len(all_branches)

            if top == 1 and continuation_token is None:
                branches_url = f"https://dev.azure.com/{self.manager.organization}/{source_project_id}/_apis/git/repositories/{repo_id}/refs?filter=heads%2F{default_branch_name}&api-version=7.1"
            elif continuation_token:
                branches_url = f"https://dev.azure.com/{self.manager.organization}/{source_project_id}/_apis/git/repositories/{repo_id}/refs?$top={top}&api-version=7.1&continuationToken={continuation_token}"
            else:
                branches_url = f"https://dev.azure.com/{self.manager.organization}/{source_project_id}/_apis/git/repositories/{repo_id}/refs?$top={top}&api-version=7.1"

            branches, headers = self.http_ops.fetch_data_with_headers(branches_url)
            if branches is None:
                self.logger.warning(f"Failed to fetch branches for {project_name}/{repo_name}")
                return [], []

            all_branches.extend(branches)
            continuation_token = headers.get("x-ms-continuationtoken") or headers.get("X-Ms-Continuationtoken")
            if not continuation_token:
                break

        default_branch_found = any(branch["name"].endswith("/" + default_branch_name) for branch in all_branches)
        if not default_branch_found:
            self.logger.debug(f"Default branch '{default_branch_name}' not found in repository {project_name}/{repo_name}")
            default_branch_url = f"https://dev.azure.com/{self.manager.organization}/{source_project_id}/_apis/git/repositories/{repo_id}/refs?filter=heads%2F{default_branch_name}&api-version=7.1"
            default_branch_data = self.http_ops.fetch_data(default_branch_url)
            if default_branch_data and len(default_branch_data) > 0:
                all_branches.extend(default_branch_data)

        branches_only = [b for b in all_branches if b.get("name", "").startswith("refs/heads/")]
        self.logger.debug(f"Branches for {project_name}/{repo_name}: {len(branches_only)} total")
        if not branches_only:
            return [], []
        return branches_only, [branch["name"].split("/")[-1] for branch in branches_only]

    def get_repository_branches(
        self, source_project_id, repo_id, project_name, repo_name, top_branches_to_scan, default_branch_name
    ):
        cache_key = (
            source_project_id,
            repo_id,
            project_name,
            repo_name,
            top_branches_to_scan,
            default_branch_name,
        )
        with self.runtime_state.branch_cache_lock:
            if cache_key in self.runtime_state.branch_cache:
                return self.runtime_state.branch_cache[cache_key]

        branches = self._get_repository_branches_uncached(
            source_project_id,
            repo_id,
            project_name,
            repo_name,
            top_branches_to_scan,
            default_branch_name,
        )
        with self.runtime_state.branch_cache_lock:
            self.runtime_state.branch_cache[cache_key] = branches
        return branches
