#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

import json, base64, requests
import urllib.parse
import subprocess, os, re, yaml
from datetime import datetime, timedelta
import logging

from urllib3.util.retry import Retry

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def requests_session_with_retries(total=6, backoff_factor=1, status_forcelist=(500, 502, 503, 504)):
    # Retry strategy for requests
    session = requests.Session()
    retry_strategy = Retry(
        total=total,
        backoff_factor=backoff_factor,  # exponential backoff
        status_forcelist=status_forcelist,
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session

# create a global session
http = requests_session_with_retries()

def fetch_data(url, token, qret=False):

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {token}'
    }
    
    try:
        print(f"\tData fetched from {url}")
        try:
            response = http.get(url=url, headers=headers)
        except ConnectionResetError as cre:
            print(f"\tConnection reset error occurred: {cre}")
            return None

        if qret:
            return response.text

        response.raise_for_status()
        data = response.json()

        return data['value'] if 'value' in data.keys() else data
    except requests.exceptions.HTTPError as http_err:
        print(f"\tHTTP error occurred: {http_err}")
    except Exception as err:
        print(f"\tAn error occurred: {err}")
        return None

def fetch_data_with_headers(url, token):
    """Fetch data and return both the data and response headers (for continuation tokens)."""
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {token}'
    }
    
    try:
        print(f"\tData fetched from {url}")
        response = http.get(url=url, headers=headers)
        response.raise_for_status()
        data = response.json()
        result_data = data['value'] if 'value' in data.keys() else data
        return result_data, response.headers
    except requests.exceptions.HTTPError as http_err:
        print(f"\tHTTP error occurred: {http_err}")
        return None, None
    except Exception as err:
        print(f"\tAn error occurred: {err}")
        return None, None

def post_data(url, payload, token):
            
    """
    Posts data to the given URL. Returns (response_json, None) on success, (None, error_message) on error.
    Usage:
        resp, err = post_data(...)
        if err:
            # handle error
        else:
            # use resp
    """
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {token}',
    }
    try:
        response = http.post(url=url, headers=headers, data=payload)
        response.raise_for_status()
        print(f"\tData posted to {url}")
        return response.json(), None
    except requests.exceptions.HTTPError as http_err:
        try:
            error_message = response.json().get('message', str(http_err))
        except Exception:
            error_message = str(http_err)
        return None, error_message
    except Exception as err:
        return None, str(err)

class AzureDevOpsManager:
    def get_feed_packages(self, feed_id, project_id=None):
        """
        Fetches packages for a given feed. If project_name is None, fetches org-level feed packages.
        Returns a list of packages (may be empty).
        """
        if project_id:
            packages_url = f"https://feeds.dev.azure.com/{self.organization}/{project_id}/_apis/packaging/feeds/{feed_id}/packages?api-version=7.1-preview.1&includeUrls=false"
        else:
            packages_url = f"https://feeds.dev.azure.com/{self.organization}/_apis/packaging/feeds/{feed_id}/packages?api-version=7.1-preview.1&includeUrls=false"
        try:
            packages = fetch_data(packages_url, self.token)
            package_list = packages if isinstance(packages, list) else packages.get('value', packages) if packages else []
            # For each package, fetch upstreaming behaviour if protocolType is supported
            for pkg in package_list:
                protocol = (pkg.get('protocolType') or '').lower()
                if protocol in ('maven', 'nuget', 'npm', 'python'):
                    # Build the correct upstreaming behaviour URL
                    if project_id:
                        base_url = f"https://pkgs.dev.azure.com/{self.organization}/{project_id}/_apis/packaging/feeds/{feed_id}"
                    else:
                        base_url = f"https://pkgs.dev.azure.com/{self.organization}/_apis/packaging/feeds/{feed_id}"
                    if protocol == 'maven':
                        upstream_url = f"{base_url}/maven/packages/{pkg.get('name')}/upstreaming"
                    elif protocol == 'nuget':
                        upstream_url = f"{base_url}/nuget/packages/{pkg.get('name')}/upstreaming"
                    elif protocol == 'npm':
                        upstream_url = f"{base_url}/npm/packages/{pkg.get('name')}/upstreaming"
                    elif protocol == 'python':
                        upstream_url = f"{base_url}/python/packages/{pkg.get('name')}/upstreaming"
                    else:
                        upstream_url = None
                    if upstream_url:
                        try:
                            behaviour = fetch_data(upstream_url, self.token)
                            if isinstance(behaviour, dict) and 'versionsFromExternalUpstreams' in behaviour:
                                pkg['versionsFromExternalUpstreams'] = behaviour['versionsFromExternalUpstreams']
                            else:
                                pkg['versionsFromExternalUpstreams'] = behaviour
                        except Exception as e:
                            pkg['versionsFromExternalUpstreams'] = {'error': str(e)}
            return package_list
        except Exception as e:
            print(f"Failed to fetch packages for feed {feed_id} (project: {project_id}): {e}")
            return []
    def get_feed_views(self, feed_id, project_id=None):
        """
        Fetches views for a given feed. If project_name is None, fetches org-level feed views.
        Returns a list of views (may be empty).
        """
        if project_id:
            views_url = f"https://feeds.dev.azure.com/{self.organization}/{project_id}/_apis/packaging/feeds/{feed_id}/views?api-version=7.1-preview.1"
        else:
            views_url = f"https://feeds.dev.azure.com/{self.organization}/_apis/packaging/feeds/{feed_id}/views?api-version=7.1-preview.1"
        try:
            views = fetch_data(views_url, self.token)
            return views if isinstance(views, list) else views.get('value', views) if views else []
        except Exception as e:
            print(f"Failed to fetch views for feed {feed_id} (project: {project_id}): {e}")
            return []
    def get_artifacts_feeds(self):
        """
        Fetches Azure Artifacts feeds for the organization and for each project, including feeds in the recycle bin.
        Returns a dict with 'organization', 'organization_recyclebin', 'projects', and 'projects_recyclebin' keys.
        Each project feed will include a 'k_project' field for consistency.
        """
        feeds = {'active': [], 'recyclebin': []}
        # Fetch org-level feeds
        org_url = f"https://feeds.dev.azure.com/{self.organization}/_apis/packaging/feeds?api-version=7.1"
        org_recyclebin_url = f"https://feeds.dev.azure.com/{self.organization}/_apis/packaging/feedRecycleBin?api-version=7.1-preview.1"
        try:
            org_feeds = fetch_data(org_url, self.token)
            org_feeds_list = org_feeds if isinstance(org_feeds, list) else org_feeds.get('value', []) if org_feeds else []
            for feed in org_feeds_list:
                
                feed['k_enabled'] = True
                feed_id = feed.get('id') or feed.get('name')
                project = feed.get('project')
                if project:
                    feed['k_feed_type'] = 'project'
                    feed['k_project'] = self.enrich_k_project(project.get('id'), f"https://dev.azure.com/{self.organization}/{project.get('name')}/_artifacts/feed/{feed.get('name')}")
                else:
                    feed['k_feed_type'] = 'organization'
                feed['views'] = self.get_feed_views(feed_id, project_id=project.get('id') if project else None)
                feed['packages'] = self.get_feed_packages(feed_id, project_id=project.get('id') if project else None)
            if org_feeds:
                feeds['active'] = org_feeds_list
        except Exception as e:
            print(f"Failed to fetch organization feeds: {e}")

        # Fetch org-level feeds in recycle bin
        try:
            org_recyclebin = fetch_data(org_recyclebin_url, self.token)
            org_recyclebin_list = org_recyclebin if isinstance(org_recyclebin, list) else org_recyclebin.get('value', []) if org_recyclebin else []
            for feed in org_recyclebin_list:
                feed['k_feed_type'] = 'recyclebin'
                if project:
                    feed['k_project'] = self.enrich_k_project(project.get('id'), f"https://dev.azure.com/{self.organization}/{project.get('name')}/_artifacts/feed/{feed.get('name')}")
                feed['k_enabled'] = False
            if org_recyclebin:
                feeds['recyclebin'] = org_recyclebin_list
        except Exception as e:
            print(f"Failed to fetch organization recycle bin feeds: {e}")

        return feeds
    def enrich_repositories_with_committer_stats(self, protected_resources, commits):
        """
        For each repository in protected_resources, add stats['committers']['count'] and stats['committers']['uniqueCommitters'] (list of unique committer emails).
        """
        # Build a mapping: repo_id -> set of committer emails
        repo_committers = {}
        commits_count = {}
        for commit in commits:
            repo_id = commit.get('repositoryId')
            committer_email = commit.get('committerEmail')
            if not repo_id or not committer_email:
                continue
            if repo_id not in repo_committers:
                repo_committers[repo_id] = set()
            if repo_id not in commits_count:
                commits_count[repo_id] = 0
            repo_committers[repo_id].add(committer_email)
            commits_count[repo_id] += 1

        for repo_resource in protected_resources:
            repo = repo_resource['resource']
            repo_id = repo.get('id')
            unique_committers = sorted(list(repo_committers.get(repo_id, set())))
            if 'stats' not in repo:
                repo['stats'] = {}
            repo['stats']['committers'] = {
                'totalCommits': commits_count.get(repo_id, 0),
                'count': len(unique_committers),
                'uniqueCommitters': unique_committers
            }
        return protected_resources
    def get_committer_stats(self, commits, build_service_accounts):
        """
        Aggregates commit stats by committer email.
        Returns a dict: committer_email -> {
            'commitCount': int,
            'repoCount': int,
            'totalChangeCounts': {'add': int, 'edit': int, 'delete': int},
            'authorEmails': set,
            'pusherEmails': set
        }
        """
        stats = {}
        # project_commits: committer_email -> project_id -> {repo_ids: set, commit_count: int, project_name: str}
        project_commits = {}
        build_service_account_map = {acc['id']: acc['name'] for acc in build_service_accounts}
        special_prs_email = '00000002-0000-8888-8000-000000000000@2c895908-04e0-4952-89fd-54b0046d6288'
        # Track PRs merged count per committer
        prs_merged_count = {}
        for commit in commits:
            committer_email = commit.get('committerEmail')
            repo_id = commit.get('repositoryId')
            project_id = commit.get('projectId')
            project_name = commit.get('k_project', {}).get('name') if commit.get('k_project') else None
            author_email = commit.get('authorEmail')
            push_email = commit.get('pushEmail')
            change_counts = commit.get('changeCounts', {})
            if not committer_email:
                continue
            if committer_email not in stats:
                stats[committer_email] = {
                    'commitCount': 0,
                    'repos': set(),
                    'totalChangeCounts': {'add': 0, 'edit': 0, 'delete': 0},
                    'authorEmails': set(),
                    'pusherEmails': set(),
                    'projects': set(),
                    'projectStats': [],
                    'prs_merged': 0
                }
            entry = stats[committer_email]
            entry['commitCount'] += 1
            if repo_id:
                entry['repos'].add(repo_id)
            if project_id:
                entry['projects'].add(project_id)
            entry['totalChangeCounts']['add'] += change_counts.get('add', 0)
            entry['totalChangeCounts']['edit'] += change_counts.get('edit', 0)
            entry['totalChangeCounts']['delete'] += change_counts.get('delete', 0)
            if author_email:
                entry['authorEmails'].add(author_email)
            if push_email:
                entry['pusherEmails'].add(push_email)
                if push_email == special_prs_email:
                    entry['prs_merged'] += 1
            # Track per-project stats
            if committer_email not in project_commits:
                project_commits[committer_email] = {}
            if project_id:
                if project_id not in project_commits[committer_email]:
                    project_commits[committer_email][project_id] = {
                        'repo_ids': set(),
                        'commit_count': 0,
                        'project_name': project_name
                    }
                project_commits[committer_email][project_id]['commit_count'] += 1
                if repo_id:
                    project_commits[committer_email][project_id]['repo_ids'].add(repo_id)

        for committer_email, entry in stats.items():
            entry['repoCount'] = len(entry['repos'])
            entry['projectCount'] = len(entry['projects'])
            entry['authorEmails'] = list(entry['authorEmails'])
            # Map and filter pusherEmails, set flags
            new_pusher_emails = []
            uses_build_service_account = False
            for email in entry['pusherEmails']:
                if email == special_prs_email:
                    continue
                if email.startswith('Build\\') and email in build_service_account_map:
                    uses_build_service_account = True
                    new_pusher_emails.append(build_service_account_map[email])
                else:
                    new_pusher_emails.append(email)
            entry['pusherEmails'] = new_pusher_emails
            if uses_build_service_account:
                entry['usesBuildServiceAccount'] = 1
            else:
                entry['usesBuildServiceAccount'] = 0
            entry['hasMultipleAuthors'] = 1 if len(entry['authorEmails']) > 1 else 0
            entry['hasMultiplePushers'] = 1 if len(entry['pusherEmails']) > 1 else 0
            # Add per-project stats
            entry['projectStats'] = []
            for project_id, proj_stats in project_commits.get(committer_email, {}).items():
                entry['projectStats'].append({
                    'projectId': project_id,
                    'projectName': proj_stats['project_name'],
                    'repoCount': len(proj_stats['repo_ids']),
                    'commitCount': proj_stats['commit_count']
                })
            del entry['repos']
            del entry['projects']
        return stats
    def get_commits_per_repository(self, protected_resources):
        """
        Fetches commits for each repository in the last 90 days, paginated, with includePushData enabled.
        Returns a list of commit dicts with required fields and calculated match fields.
        """
        import datetime
        all_commits = []
        now = datetime.datetime.utcnow()
        since = now - datetime.timedelta(days=90)
        since_iso = since.strftime('%Y-%m-%dT%H:%M:%SZ')
        batch_size = 100
        for repo_resource in protected_resources:
            repo = repo_resource['resource']
            project_id = repo['project']['id']
            repo_id = repo['id']
            k_project = repo.get('k_project')
            skip = 0
            while True:
                url = (
                    f"https://dev.azure.com/{self.organization}/{project_id}/_apis/git/repositories/{repo_id}/commits"
                    f"?searchCriteria.fromDate={since_iso}"
                    f"&$top={batch_size}"
                    f"&$skip={skip}"
                    f"&searchCriteria.includePushData=true"
                    f"&api-version=7.1"
                )
                commits = fetch_data(url, self.token)
                if not commits:
                    break
                for commit in commits:
                    commit_id = commit.get('commitId')
                    author = commit.get('author', {})
                    committer = commit.get('committer', {})
                    push = commit.get('push', {})
                    change_counts = commit.get('changeCounts', {})
                    # Extract fields
                    author_email = author.get('email')
                    author_name = author.get('name')
                    committer_email = committer.get('email')
                    committer_name = committer.get('name')
                    committer_date = committer.get('date')
                    push_email = push.get('pushedBy', {}).get('uniqueName')
                    push_name = push.get('pushedBy', {}).get('displayName')
                    push_id = push.get('pushId')
                    push_date = push.get('date')
                    add_count = change_counts.get('Add', 0)
                    edit_count = change_counts.get('Edit', 0)
                    delete_count = change_counts.get('Delete', 0)
                    # Calculated fields
                    committer_author_match = 1 if committer_name == author_name else 0
                    committer_pusher_match = 1 if committer_name == push_name else 0
                    commit_by_ado = 1 if push_email == "00000002-0000-8888-8000-000000000000@2c895908-04e0-4952-89fd-54b0046d6288" else 0
                    all_commits.append({
                        'repositoryId': repo_id,
                        'repositoryName': repo.get('name'),
                        'projectId': project_id,
                        'k_project': k_project,
                        'commitId': commit_id,
                        'authorEmail': author_email,
                        'authorName': author_name,
                        'committerEmail': committer_email,
                        'committerName': committer_name,
                        'committerDate': committer_date,
                        'changeCounts': {
                            'add': add_count,
                            'edit': edit_count,
                            'delete': delete_count
                        },
                        'pushEmail': push_email,
                        'pushId': push_id,
                        'pushDate': push_date,
                        'committerAuthorMatch': committer_author_match,
                        'committerPusherMatch': committer_pusher_match,
                        'commitByAdo': commit_by_ado
                    })
                if len(commits) < batch_size:
                    break
                skip += batch_size
        return all_commits
    def get_repository_pull_requests_count(self, project_id, repo_id):
        """
        Returns a dict of pull request counts for a given repository.
        Fetches in batches of 100 using $skip for pagination, tallying by status.
        Statuses include: active, abandoned, completed, notSet, all (total).
        Assumes fetch_data returns a list of pull request objects, not a response JSON.
        """
        counts = {
            'active': 0,
            'abandoned': 0,
            'completed': 0,
            'other': 0,
            'all': 0
        }
        batch_size = 100
        skip = 0
        try:
            while True:
                url = f"https://dev.azure.com/{self.organization}/{project_id}/_apis/git/repositories/{repo_id}/pullrequests?searchCriteria.status=all&$top={batch_size}&$skip={skip}&api-version=7.1"
                pr_list = fetch_data(url, self.token)
                if not pr_list:
                    break
                for pr in pr_list:
                    status = (pr.get('status') or '').lower()
                    if status == 'active':
                        counts['active'] += 1
                    elif status == 'abandoned':
                        counts['abandoned'] += 1
                    elif status == 'completed':
                        counts['completed'] += 1
                    elif status:
                        counts['other'] += 1
                    counts['all'] += 1
                if len(pr_list) < batch_size:
                    break
                skip += batch_size
            return counts
        except Exception as e:
            print(f"Failed to fetch pull request counts for repo {repo_id} in project {project_id}: {e}")
            return counts
    def get_repository_commit_dates(self, project_id, repo_id):
        """
        Returns the first and last commit dates for a given repository.
        Returns (first_commit_date, last_commit_date) as datetime objects, or (None, None) if not found.
        Assumes fetch_data returns a list of commit objects, not a response JSON.
        """
        import datetime
        # Get the latest commit (most recent)
        latest_commit_url = f"https://dev.azure.com/{self.organization}/{project_id}/_apis/git/repositories/{repo_id}/commits?searchCriteria.$top=1&api-version=7.1"
        latest_commit_list = fetch_data(latest_commit_url, self.token)
        last_commit_date = None
        first_commit_date = None
        if latest_commit_list and len(latest_commit_list) > 0:
            last_commit_date_str = latest_commit_list[0].get('committer', {}).get('date')
            if last_commit_date_str:
                last_commit_date = datetime.datetime.fromisoformat(last_commit_date_str.replace('Z', '+00:00'))
        # Get the earliest commit (oldest)
        first_commit_url = f"https://dev.azure.com/{self.organization}/{project_id}/_apis/git/repositories/{repo_id}/commits?searchCriteria.$top=1&searchCriteria.showOldestCommitsFirst=true&api-version=7.1"
        first_commit_list = fetch_data(first_commit_url, self.token)
        if first_commit_list and len(first_commit_list) > 0:
            first_commit_date_str = first_commit_list[0].get('committer', {}).get('date')
            if first_commit_date_str:
                first_commit_date = datetime.datetime.fromisoformat(first_commit_date_str.replace('Z', '+00:00'))
        return (first_commit_date, last_commit_date)
    def __init__(self, organization, project_filter, pat_token,default_build_settings_expectations={}, branch_limit=5, 
                 #gitleaks_installed=True, 
                 exception_strings=False):
        self.organization = organization
        self.token = base64.b64encode(f":{pat_token}".encode()).decode()
        self.default_build_settings_expectations = default_build_settings_expectations
        self.exceptions = exception_strings if exception_strings else [f"/dev.azure.com/{organization}/"]
        self.scan_start_time = datetime.now()
        self.scan_finish_time = None
        self.projects = self.get_projects(project_filter=project_filter)
        self.repo_scan = {
            "top": branch_limit, 
        }
        #self.gitleaks_installed = gitleaks_installed
        # by default ignores same organization domains


    def get_projects(self, api_endpoint="projects", api_version="?api-version=7.1-preview.4", project_filter=None):

        url = f"https://dev.azure.com/{self.organization}/_apis/{api_endpoint}{api_version}"
        url_deleted = f"https://dev.azure.com/{self.organization}/_apis/{api_endpoint}{api_version}&stateFilter=deleted"

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Basic {self.token}',
        }

        
        try:
            
            # Make the GET request
            print(f"\tDISCOVERING projects")
            self.projects = {}
            
            response = http.get(url, headers=headers)
            response.raise_for_status()

            # Parse JSON response
            data = response.json()

            # Include deleted projects
            response_deleted = http.get(url_deleted, headers=headers)
            response_deleted.raise_for_status()
            data_deleted = response_deleted.json()
            if 'value' in data_deleted:
                data['value'].extend(data_deleted['value'])

            if project_filter:
                # Filter projects based on the provided project_filter & add general project settings
                pf_lc = set([p.lower() for p in project_filter])
                for project in data['value']:
                    if project['id'].lower() in pf_lc or project['name'].lower() in pf_lc:
                        # Project base with filter
                        self.projects[project['id']] = project
                        
                        if project['state'] == "deleted":
                            print(f"\t\tProject {project['name']} ({project['id']}) is DELETED")
                            continue
                        # Build Settings
                        build_general_settings = self.get_project_build_general_settings(project['id'])
                        build_metrics = self.get_project_build_metrics(project['id'])
                        self.projects[project['id']]['general_settings'] = {} 
                        self.projects[project['id']]['general_settings']['build_settings'] = build_general_settings
                        self.projects[project['id']]['general_settings']['build_metrics'] = build_metrics

                        for expected_key, expected_value in self.default_build_settings_expectations.items():
                            print(f"\t\tChecking {expected_key} for project {project['name']} ({project['id']})")
                            if expected_key in self.projects[project['id']]['general_settings']['build_settings']:
                                self.projects[project['id']]['general_settings']['build_settings'][expected_key] = {
                                    "expected": expected_value,
                                    "found": self.projects[project['id']]['general_settings']['build_settings'][expected_key]
                                }
                            else:
                                self.projects[project['id']]['general_settings']['build_settings'][expected_key] = {
                                    "expected": expected_value,
                                    "found": None
                                }

                        
                    else:
                        print(f"\tProject {project['name']} ({project['id']}) is NOT in the filter")
            else:
                for project in data['value']:
                    # Project base without filter
                    self.projects[project['id']] = project

                    if project['state'] == "deleted":
                        print(f"\t\tProject {project['name']} ({project['id']}) is DELETED")
                        continue

                    # Build Settings
                    build_general_settings = self.get_project_build_general_settings(project['id'])
                    build_metrics = self.get_project_build_metrics(project['id'])
                    self.projects[project['id']]['general_settings'] = {} 
                    self.projects[project['id']]['general_settings']['build_settings'] = build_general_settings
                    self.projects[project['id']]['general_settings']['build_metrics'] = build_metrics
            
                    for expected_key, expected_value in self.default_build_settings_expectations.items():
                        if expected_key in self.projects[project['id']]['general_settings']['build_settings']:
                            self.projects[project['id']]['general_settings']['build_settings'][expected_key] = {
                                "expected": expected_value,
                                "found": self.projects[project['id']]['general_settings']['build_settings'][expected_key]
                            }
                        else:
                            self.projects[project['id']]['general_settings']['build_settings'][expected_key] = {
                                "expected": expected_value,
                                "found": None
                            }

        except requests.exceptions.HTTPError as http_err:
            print(f"\tHTTP error occurred: {http_err}")
        except Exception as err:
            print(f"\tAn error occurred: {err}")
    
        return self.projects

    # APPROVALS & CHECKS
    def get_checks_approvals(self, inventory):
        print("CHECKING CHECKS & APPROVALS")
        for inventory_key,inventory_value in inventory.items():
            for protected_resource in inventory_value['protected_resources']:
                actual_resource = protected_resource['resource']
                # k_project is now a dict of project_id: k_project_element
                
                project_id = actual_resource.get('k_project', {}).get('id', None)

                if inventory_value['level'] == "org":
                    # For org level resources (pools), ignore approvals and checks as they are not queriable at this level
                    protected_resource['resource']['checks'] = []
                    continue
                else:  
                    # Approvals and Checks are always applied to the resource ID, so can pick any project
                    url = "https://dev.azure.com/"+self.organization +"/"+str(project_id)+"/_apis/pipelines/checks/configurations?resourceType="+inventory_key+"&$expand=settings&resourceId="+str(actual_resource['id'])+"&api-version=7.1-preview.1"

                # CORNER CASE FOR REPOS ID: The resource ID required is PROJECT_ID.REPO_ID
                if inventory_key == "repository":
                    url = "https://dev.azure.com/"+self.organization +"/"+str(project_id)+"/_apis/pipelines/checks/configurations?resourceType="+inventory_key+"&$expand=settings&resourceId="+str(project_id)+"."+str(actual_resource['id'])+"&api-version=7.1-preview.1"

                new_checks = fetch_data(url, self.token)
                if new_checks is None:
                    new_checks = []
                    continue

                print(f"\n\n{str(len(new_checks))} checks for {inventory_key} {actual_resource['name']} ({actual_resource['id']})\n\n")
                protected_resource['resource']['checks'] = new_checks

        # Return inventory updated with checks and approvals
        return inventory
    

    # CROSS PROJECT & PROTECTION
    def enrich_resource_protection_and_cross_project(self, inventory):
        for inventory_key, inventory_value in inventory.items():
            for protected_resource in inventory_value['protected_resources']:
                resource = protected_resource['resource']

                # Protection state
                if 'checks' in resource and isinstance(resource['checks'], list) and len(resource['checks']) > 0:
                    resource['protectedState'] = "protected"
                else:
                    resource['protectedState'] = "unprotected"


                # Cross-project flag
                if inventory_key == "endpoint" and 'serviceEndpointProjectReferences' in resource:
                    resource['isCrossProject'] = len(resource['serviceEndpointProjectReferences']) > 1
                else:
                    # If resource has queues, its a pool, and if a pool has multiple queues it is cross-project
                    if 'queues' in resource and isinstance(resource['queues'], list) and len(resource['queues']) > 1:
                        resource['isCrossProject'] = True

                    # @TODO - check for multiple projects in deployment groups
                    

                    # ELSE check k_projects
                    elif 'k_projects' in resource and isinstance(resource['k_projects'], list) and len(resource['k_projects']) > 1:
                        resource['isCrossProject'] = True

                    # ELSE check pipelinepermissions for multiple project IDs
                    elif 'pipelinepermissions' in resource and isinstance(resource['pipelinepermissions'], list):
                        unique_projectids = set()
                        for permission in resource['pipelinepermissions']:
                            proj_id = str(permission).split('_')[0]
                            unique_projectids.add(proj_id)
                        resource['isCrossProject'] = len(unique_projectids) > 1
                        
                    else:
                        resource['isCrossProject'] = False

        return inventory


    # PERMISSIONS

    def get_permissions(self, inventory, all_definitions, builds):

        # CHECKING PERMISSIONED PIPELINES
        print("CHECKING PERMISSIONED PIPELINES")

        for inventory_key,inventory_value in inventory.items():
            for protected_resource in inventory_value['protected_resources']:
                actual_resource = protected_resource['resource']
                # if endpoint, use project references to store the individual permissions across projects
                if inventory_key == "endpoint":
                    # pipeline permissions
                    for current_project_reference in actual_resource['serviceEndpointProjectReferences']:
                        project_id = current_project_reference['projectReference']['id']
                        url = f"https://dev.azure.com/{self.organization}/{project_id}/_apis/pipelines/pipelinepermissions/{inventory_key}/{actual_resource['id']}?api-version=7.1-preview.1"
                        data = fetch_data(url, self.token)
                        # permissions are per resource per project - adding to the pipelines object
                        if 'allPipelines' in data.keys():
                            if 'pipelinepermissions' not in protected_resource['resource']:
                                protected_resource['resource']['pipelinepermissions'] = []
                            protected_resource['resource']['pipelinepermissions'].extend([
                                build_def['k_key'] for build_def in all_definitions
                                if build_def['k_key'] not in protected_resource['resource']['pipelinepermissions'] and build_def['k_key'].startswith(project_id)
                            ])
                            if 'pipelinepermissions' not in current_project_reference['projectReference'].keys():
                                current_project_reference['projectReference']['pipelinepermissions'] = []
                            current_project_reference['projectReference']['pipelinepermissions'].extend([
                                build_def['k_key'] for build_def in all_definitions
                                if build_def['k_key'] not in current_project_reference['projectReference']['pipelinepermissions'] and build_def['k_key'].startswith(project_id)
                            ])
                        elif 'pipelines' in data.keys():
                            if 'pipelinepermissions' not in protected_resource['resource']:
                                protected_resource['resource']['pipelinepermissions'] = []
                            protected_resource['resource']['pipelinepermissions'].extend([project_id+"_"+str(definition['id']) for definition in data['pipelines'] if project_id+"_"+str(definition['id']) not in protected_resource['resource']['pipelinepermissions']])
                            # also add it to the proj reference of the resource
                            if 'pipelinepermissions' not in current_project_reference['projectReference'].keys():
                                current_project_reference['projectReference']['pipelinepermissions'] = []
                            current_project_reference['projectReference']['pipelinepermissions'].extend([project_id+"_"+str(definition['id']) for definition in data['pipelines'] if project_id+"_"+str(definition['id']) not in current_project_reference['projectReference']['pipelinepermissions']])
                        else:
                            print(f"Data may be missing if the PAT token is not correctly scoped! Project ID {project_id} not found in self.projects. Do you need to increase the scope of the observability in config?")
                            current_project_reference['projectReference']['pipelinepermissions'] = []
                            current_project_reference['projectReference']['warning'] = f"Project ID {project_id} not found in self.projects. Do you need to increase the scope of the observability in config? PAT token may not have necessary permissions"
                        
                        # @TODO - add the user permissions to the projectReference

                        # Ensure uniqueness for both resource and projectReference pipelinepermissions
                        if 'pipelinepermissions' in protected_resource['resource']:
                            protected_resource['resource']['pipelinepermissions'] = list(set(protected_resource['resource']['pipelinepermissions']))
                        if 'pipelinepermissions' in current_project_reference['projectReference']:
                            current_project_reference['projectReference']['pipelinepermissions'] = list(set(current_project_reference['projectReference']['pipelinepermissions']))


                elif inventory_key == "repository":
                    for project in self.projects:
                        # Only process wellFormed projects
                        if isinstance(self.projects[project], dict):
                            if self.projects[project].get('state', '').lower() != 'wellformed':
                                continue
                        # repos need to access the projectid.repoid and needs to be retrieved at a build level - and still not great visibility, because its a mix between pipeline + user + service account / group permissions - RBAC settings matter
                        url = f"https://dev.azure.com/{self.organization}/{project}/_apis/pipelines/pipelinepermissions/{inventory_key}/{actual_resource['project']['id']}.{actual_resource['id']}"
                        data = fetch_data(url, self.token)
                        # permissions are per resource per project
                        # pipelines may not be in the same project though - and the build service account has access (RBAC)
                        

                        # if all pipelines, it's all pipelines within the project;
                        if 'allPipelines' in data.keys():
                            if 'pipelinepermissions' not in protected_resource['resource']:
                                protected_resource['resource']['pipelinepermissions'] = []
                            protected_resource['resource']['pipelinepermissions'].extend([definition_id for definition_id in all_definitions if definition_id not in protected_resource['resource']['pipelinepermissions'] and definition_id.startswith(project)])
                        else:
                            if 'pipelinepermissions' not in protected_resource['resource']:
                                protected_resource['resource']['pipelinepermissions'] = []
                            protected_resource['resource']['pipelinepermissions'].extend([project+"_"+str(definition['id']) for definition in data['pipelines'] if project+"_"+str(definition['id']) not in protected_resource['resource']['pipelinepermissions']])
                        

                        # TODO - Check true repository access permissions from across projects, pipelines (and also users...)
                        # 0. Look at the RBAC for all build service accounts for all projects
                        # 1. Is ORG setting on? If yes -> Stop. Looking at the RBAC for all build service accounts for all projects is enough
                        # 2. If not on, this means some projects may have full access by default. Look at project level settings
                        # 2. If the ORG level setting allows for cross project access, assume all pipelines in that project can access all repos in all projects
                        
                        
                        # looking at each build & build definition to see if it tried reaching the repo (reverse lookup - instead of querying the pipeline permissions endpoint)
                        for build in builds:
                            project, build_id = build['k_key'].split("_")
                            # append pipelines that have accessed the repo
                            if build['repository']['id'] == actual_resource['id'] or build['definition']['project']['id'] == actual_resource['project']['id']:
                                if build['definition']['id'] not in protected_resource['resource']['pipelinepermissions']:
                                    if 'pipelinepermissions' not in protected_resource['resource']:
                                        protected_resource['resource']['pipelinepermissions'] = []
                                    protected_resource['resource']['pipelinepermissions'].append(project+"_"+str(build['definition']['id']))
                        for build_def in all_definitions:
                            build_def_id = build_def['k_key']
                            # with repos, the boundary is the project - same project, youve got access
                            if build_def['repository']['id'] == actual_resource['id'] or build_def['project']['id'] == actual_resource['project']['id']:
                                if build_def_id not in protected_resource['resource']['pipelinepermissions']:
                                    if 'pipelinepermissions' not in protected_resource['resource']:
                                        protected_resource['resource']['pipelinepermissions'] = []
                                    protected_resource['resource']['pipelinepermissions'].append(build_def_id)
                        # Ensure uniqueness for resource pipelinepermissions
                        if 'pipelinepermissions' in protected_resource['resource']:
                            protected_resource['resource']['pipelinepermissions'] = list(set(protected_resource['resource']['pipelinepermissions']))
                else:
                    for project in self.projects:
                        # Only process wellFormed projects
                        if isinstance(self.projects[project], dict):
                            if self.projects[project].get('state', '').lower() != 'wellformed':
                                continue

                        # for queues, variable groups, secure files. these are project scoped resources

                        if inventory_value['level'] == "org":
                            # For org level resources (pools), ignore pipeline permissions, because this is applied at a queue level
                            # Should put here all the projects that have access to the pool, but this can also be calculated @TODO
                            protected_resource['resource']['pipelinepermissions'] = []
                            continue
                        try:
                            url = f"https://dev.azure.com/{self.organization}/{project}/_apis/pipelines/pipelinepermissions/{inventory_key}/{actual_resource['id']}?api-version=7.1-preview.1"
                            data = fetch_data(url, self.token)

                            # permissions are per resource per project
                            if 'allPipelines' in data.keys():
                                if 'pipelinepermissions' not in protected_resource['resource']:
                                    protected_resource['resource']['pipelinepermissions'] = []
                                # Fix: all_definitions may be dicts, not strings
                                for definition in all_definitions:
                                    # If definition is a dict, get its k_key
                                    if isinstance(definition, dict):
                                        definition_id = definition.get('k_key')
                                    else:
                                        definition_id = definition
                                    if isinstance(definition_id, str) and definition_id.startswith(str(project)):
                                        if definition_id not in protected_resource['resource']['pipelinepermissions']:
                                            protected_resource['resource']['pipelinepermissions'].append(definition_id)
                            else:
                                if 'pipelinepermissions' not in protected_resource['resource']:
                                    protected_resource['resource']['pipelinepermissions'] = []
                                protected_resource['resource']['pipelinepermissions'].extend([project+"_"+str(definition['id']) for definition in data['pipelines'] if project+"_"+str(definition['id']) not in protected_resource['resource']['pipelinepermissions']])
                            # Ensure uniqueness for resource pipelinepermissions
                            if 'pipelinepermissions' in protected_resource['resource']:
                                protected_resource['resource']['pipelinepermissions'] = list(set(protected_resource['resource']['pipelinepermissions']))
                        except Exception as e:
                            print(f"Error fetching pipeline permissions for {inventory_key} {actual_resource['name']} ({actual_resource['id']}) in project {project}: {e}")
                            print(f"DATA: {data}")
                            continue
        # return inventory updated with permissions
        return inventory 

    def scan_string_with_regex(self, string, engine, source_of_data):

        regex_patterns = []
        try:
            with open("datastore/scanners/patterns/cicd_sast.json", "r") as file:
                patterns_data = json.load(file)
                if engine in patterns_data:
                    scope_data = patterns_data[engine]
                    regex_patterns = scope_data.get("patterns", [])
                else:
                    print(f"No patterns found for engine {engine}.")
        except FileNotFoundError:
            print("Regex patterns file not found. Please ensure 'patterns/cicd_sast.json' exists.")
        except json.JSONDecodeError:
            print("Failed to parse the regex patterns file as JSON.")
        except Exception as e:
            print(f"An error occurred while loading regex patterns: {e}")

        findings = []

        for pattern in regex_patterns:
            matches = re.finditer(pattern, string)
            for match in matches:
                for exception in self.exceptions:
                    if isinstance(match.group(), str) and exception in match.group():
                        print(f"Skipping match {match.group()} due to exception.")
                        break

                findings.append({
                    "source": source_of_data,
                    "match": match.group(),
                    "start": match.start(),
                    "end": match.end(),
                    "pattern": pattern
                })
                print(f"Found match: {match.group()} at {match.start()}-{match.end()}")

        return findings 


    def get_repository_branches(self, source_project_id, repo_id, project_name, repo_name, top_branches_to_scan, default_branch_name):
        """
        Fetches branches and sorts by last commit date.
        - -1 = all branches
        - 0 = default branch only
        - N (>= 1) = top N branches by last commit date, ensuring default is included
        """

        all_branches = None
        continuation_token = None

        if top_branches_to_scan is None:
            recent_branches_to_scan = 0

        totalBranchesToFetch = top_branches_to_scan
        
        if top_branches_to_scan <= -1:
            totalBranchesToFetch = 1000  # Arbitrary large number to fetch all branches
            top = 100
        elif top_branches_to_scan == 0:
            top = 1
        else:
            if top_branches_to_scan < 100:
                top = top_branches_to_scan
            else:
                top = 100
        
        # Fetch all branches using continuation tokens
        while True:
            # Check if we've fetched enough branches
            if all_branches is None:
                all_branches = []
            elif totalBranchesToFetch >= all_branches.__len__():
                break
            # Adjust top if nearing the totalBranchesToFetch limit
            elif totalBranchesToFetch - all_branches.__len__() < top:
                top = totalBranchesToFetch - all_branches.__len__()

            if top == 1 and continuation_token is None:
                # If only fetching default branch, no need for continuation token
                branches_url = f"https://dev.azure.com/{self.organization}/{source_project_id}/_apis/git/repositories/{repo_id}/refs?filter=heads%2F{default_branch_name}&api-version=7.1"
            elif continuation_token:
                branches_url = f"https://dev.azure.com/{self.organization}/{source_project_id}/_apis/git/repositories/{repo_id}/refs?$top={top}&api-version=7.1&continuationToken={continuation_token}"
            else:
                branches_url = f"https://dev.azure.com/{self.organization}/{source_project_id}/_apis/git/repositories/{repo_id}/refs?$top={top}&api-version=7.1"
            
            branches, headers = fetch_data_with_headers(branches_url, self.token)
            
            if branches is None:
                print(f"Failed to fetch branches for {project_name}/{repo_name}.")
                return [], []
            
            all_branches.extend(branches)
            
            # Check for continuation token in headers
            continuation_token = headers.get('x-ms-continuationtoken') or headers.get('X-Ms-Continuationtoken')
            if not continuation_token:
                break
        
        # check if default branch is in the list
        default_branch_found = any(branch['name'].endswith('/' + default_branch_name) for branch in all_branches)
        if not default_branch_found:
            print(f"\tWarning: Default branch '{default_branch_name}' not found in repository {project_name}/{repo_name} branches.")
            # Get default branch specifically
            default_branch_url = f"https://dev.azure.com/{self.organization}/{source_project_id}/_apis/git/repositories/{repo_id}/refs?filter=heads%2F{default_branch_name}&api-version=7.1"
            default_branch_data = fetch_data(default_branch_url, self.token)
            if default_branch_data and len(default_branch_data) > 0:
                all_branches.extend(default_branch_data)

        # Filter to only heads (branches), not tags
        branches_only = [b for b in all_branches if b.get('name', '').startswith('refs/heads/')]
        print(f"\tBranches for {project_name}/{repo_name}: {len(branches_only)} total")
        
        if not branches_only:
            return [], []
        
        return branches_only, [branch['name'].split('/')[-1] for branch in branches_only]
        
        # # Return all branches if recent_branches_to_scan is -1
        # if recent_branches_to_scan == -1:
        #     branches_names = [branch['name'].split('/')[-1] for branch in branches_only]
        #     return branches_only, branches_names
        
        # Get last commit date for each branch
        # print(f"\tFetching last commit dates for branches...")
        # for branch in branches_only:
        #     branch_name = branch['name']
        #     # Get last commit on this branch
        #     commit_url = f"https://dev.azure.com/{self.organization}/{source_project_id}/_apis/git/repositories/{repo_id}/commits?searchCriteria.itemVersion.version={branch_name}&searchCriteria.$top=1&api-version=7.1"
        #     commits = fetch_data(commit_url, self.token)
        #     if commits and len(commits) > 0:
        #         commit_date_str = commits[0].get('committer', {}).get('date', '')
        #         if commit_date_str:
        #             try:
        #                 branch['k_lastCommitDate'] = datetime.fromisoformat(commit_date_str.replace('Z', '+00:00'))
        #             except:
        #                 branch['k_lastCommitDate'] = datetime.min
        #         else:
        #             branch['k_lastCommitDate'] = datetime.min
        #     else:
        #         branch['k_lastCommitDate'] = datetime.min
        
        # # Sort by last commit date (most recent first)
        # branches_only.sort(key=lambda b: b.get('k_lastCommitDate', datetime.min), reverse=True)
        
        # # Return based on parameters
        # if recent_branches_to_scan == 0:
        #     # Return only the default branch
        #     result_branches = [branch for branch in branches_only if branch['name'].endswith('/' + default_branch_name)]
        #     if not result_branches and branches_only:
        #         # Fallback to most recent branch if default not found
        #         result_branches = [branches_only[0]]
        #     branches_names = [branch['name'].split('/')[-1] for branch in result_branches]
        #     return result_branches, branches_names
        # else:
        #     # Return top N recent branches, ensuring default is included
        #     result_branches = branches_only[:recent_branches_to_scan]
        #     branches_names = [branch['name'].split('/')[-1] for branch in result_branches]

        #     # If default branch is not in top N, replace the last branch with default
        #     if default_branch_name not in branches_names:
        #         for branch in branches_only:
        #             if branch['name'].endswith('/' + default_branch_name):
        #                 # Replace the last branch in result_branches with default
        #                 result_branches[-1] = branch
        #                 branches_names[-1] = default_branch_name
        #                 break
        #     return result_branches, branches_names


    def get_project_build_general_settings(self, project):
        url = f"https://dev.azure.com/{self.organization}/{project}/_apis/build/generalsettings?api-version=7.1"

        try:
            print(f"Fetching general settings for project {project}...")
            general_settings = fetch_data(url, self.token)
            if general_settings:
                print(f"Successfully retrieved general settings for project {project}.")
            else:
                print(f"Failed to retrieve general settings for project {project}.")
            return general_settings
        except Exception as e:
            print(f"An error occurred while fetching general settings for project {project}: {e}")
            return None
        
    def get_project_build_metrics(self, project, metric_aggregation_type="hourly"):
        url = f"https://dev.azure.com/{self.organization}/{project}/_apis/build/metrics/{metric_aggregation_type}?api-version=7.1-preview.1"

        try:
            print(f"Fetching build metrics for project {project} with aggregation type {metric_aggregation_type}...")
            build_metrics = fetch_data(url, self.token)
            if build_metrics:
                print(f"Successfully retrieved build metrics for project {project}.")
            else:
                print(f"Failed to retrieve build metrics for project {project}.")
            return build_metrics
        except Exception as e:
            print(f"An error occurred while fetching build metrics for project {project}: {e}")
            return None
    
    def get_build_definition_metrics(self, build_definition_id):

        project, definition_id = build_definition_id.split("_")
        try:
            url = f"https://dev.azure.com/{self.organization}/{project}/_apis/build/definitions/{definition_id}/metrics?api-version=7.1-preview.1"

            def_metrics = fetch_data(url, self.token)
            if def_metrics:
                print(f"Successfully retrieved def_metrics for project {project} / pipeline ID {definition_id}.")
            else:
                print(f"Failed to retrieve def_metrics for project {project} / pipeline ID {definition_id}.")
            
            return def_metrics
        
        except Exception as e:
            print(f"An error occurred while fetching def_metrics for project {project} / pipeline ID {definition_id}: {e}")
            return None

    # PIPELINES & PIPELINE RUNS

    def get_builds_per_definition_per_project(self, manager_pipeline={"preview":{"api_version": "api-version=7.1", "api_endpoint": "_apis/pipelines"}, "builds":{"api_version": "api-version=7.1", "api_endpoint": "_apis/build/builds"}, "build_definitions":{"api_version": "api-version=7.1", "api_endpoint": "_apis/build/definitions"}}, top_branches_to_scan=0):

        # Get PIPELINES by project / "For each project"
        print("DISCOVERING PIPELINES")

        # Now using arrays instead of dicts
        build_def_list = []
        builds_list = []

        # For each project, get the pipelines
        for project in self.projects:
            # Only process wellFormed projects
            if isinstance(self.projects[project], dict):
                if self.projects[project].get('state', '').lower() != 'wellformed':
                    continue

            url = f"https://dev.azure.com/{self.organization}/{project}/{manager_pipeline['build_definitions']['api_endpoint']}?{manager_pipeline['build_definitions']['api_version']}"
            build_definitions = fetch_data(url, self.token)
            if build_definitions is None:
                build_definitions = []
                continue
            print(f"\t{str(len(build_definitions))} build definitions for {self.projects[project]['name']}")
            # For each build definition, get the build definition individually
            for build_definition in build_definitions:
                enriched_build_definition = {}
                specific_url = f"https://dev.azure.com/{self.organization}/{project}/{manager_pipeline['build_definitions']['api_endpoint']}/{build_definition['id']}?{manager_pipeline['build_definitions']['api_version']}"
                enriched_build_definition = fetch_data(specific_url, self.token)
                if enriched_build_definition is None:
                    print(f"\tCould not get build definition {build_definition['name']} for project {self.projects[project]['name']}")
                    continue
                # Add k_project to definition
                enriched_build_definition['k_project'] = self.enrich_k_project(project)
                # Add k_key property
                enriched_build_definition['k_key'] = project+"_"+str(build_definition['id'])
                # ENRICH BUILD DEFINITION with BUILDS - POTENTIAL EXECUTION and EXECUTED BUILDS
                enriched_build_definition['builds'] = {
                    "metrics" : self.get_build_definition_metrics(build_definition_id=project+"_"+str(build_definition['id'])),
                    "preview": {},  # Current YAML, future pipeline runs
                    "builds": {},    # Past YAML, actual pipeline runs
                }
                url = f"https://dev.azure.com/{self.organization}/{project}/{manager_pipeline['builds']['api_endpoint']}?definitions={enriched_build_definition['id']}&{manager_pipeline['builds']['api_version']}"
                builds = fetch_data(url, self.token)
                if builds is None:
                    builds = []
                    pass

                print(f"\t{str(len(builds))} builds for build definition {build_definition['name']}")
                # For each build, scan pipeline YAML with regex and build results
                for build in builds:
                    print(f"\tBuild {build['id']} for Build Definition {build_definition['name']}")
                    

                    # Getting the YAML for the build
                    try:
                        yaml_url =  f"https://dev.azure.com/{self.organization}/{project}/{manager_pipeline['builds']['api_endpoint']}/{build['id']}/logs/1?{manager_pipeline['builds']['api_version']}"
                        yaml = fetch_data(yaml_url, self.token, True)
                        pipeline_recipe = self.parse_pipeline_yaml(yaml)
                        build['pipeline_recipe'] = pipeline_recipe
                        # Add k_project to build
                        build['k_project'] = self.enrich_k_project(project)
                        # Add k_key property
                        build['k_key'] = project+"_"+str(build['id'])
                        if yaml is not None:
                            scope = "pipeline_yaml"
                            engine = "regex"
                            build['yaml'] = yaml
                            regex_results = self.scan_string_with_regex(yaml, engine, build['_links']['self']['href'])
                            if 'cicd_sast' not in build.keys():
                                build['cicd_sast'] = []
                            if regex_results:
                                build['cicd_sast'].append({
                                    "engine": engine,
                                    "scope": scope,
                                    "results": regex_results,
                                })
                            if not isinstance(enriched_build_definition['builds']['builds'], list):
                                enriched_build_definition['builds']['builds'] = []
                            enriched_build_definition['builds']['builds'].append(str(build['id']))
                        # Add build to builds_list
                        builds_list.append(build)
                    except Exception as e:
                        print(f"\tCould not get YAML for build {build['id']} for build definition {build_definition['name']}")
                        yaml = None
                        continue


                    # Getting JSON for the pipeline run
                    # try:
                    #     # GET https://dev.azure.com/{organization}/{project}/_apis/pipelines/{pipelineId}/runs/{runId}?api-version=7.1
                    #     pipeline_run_url = f"https://dev.azure.com/{self.organization}/{project}/{manager_pipeline['preview']['api_endpoint']}/{build_definition['id']}/runs/{build['id']}?{manager_pipeline['preview']['api_version']}"
                    #     pipeline_recipe_finalYaml = fetch_data(pipeline_run_url, self.token)
                    #     print("======================================================================================")
                    #     print("pipeline_recipe_finalYaml")
                    #     print("pipeline_recipe_finalYaml")
                    #     print("pipeline_recipe_finalYaml")
                    #     print(pipeline_recipe_finalYaml)
                    #     print("pipeline_recipe_finalYaml")
                    #     print("pipeline_recipe_finalYaml")
                    #     print("======================================================================================")

                    # except Exception as e:
                    #     print(f"\tCould not get pipeline run for build {build['id']} for build definition {build_definition['name']}")
                    #     yaml = None
                    #     continue


                # Enrich enriched_build_definition with PREVIEW YAML 
                # limited support as it needs defaults on there (?) or not because I could get the YAML from the runs

                if manager_pipeline['preview']:


                    preview_url =  f"https://dev.azure.com/{self.organization}/{project}/{manager_pipeline['preview']['api_endpoint']}/{build_definition['id']}/preview?{manager_pipeline['preview']['api_version']}"
                    
                    # For the build definition, which is associated with a project
                    # where is the build definition
                    repo_id = enriched_build_definition['repository']['id']
                    repo_name = enriched_build_definition['repository']['name']
                    default_branch = enriched_build_definition['repository'].get('defaultBranch', 'refs/heads/main')
                    project_name = enriched_build_definition['repository']['url'].split('/')[4]
                    decoded_string = urllib.parse.unquote(urllib.parse.unquote(project_name))
                    source_project_id = None
                    for key, value in self.projects.items():
                        # Only process wellFormed projects
                        if isinstance(self.projects[project], dict):
                            if self.projects[project].get('state', '').lower() != 'wellformed':
                                continue
                        if value['name'] == decoded_string:
                            source_project_id = key
                            break
                    if source_project_id is None:
                        print(f"Project name {decoded_string} not found in self.projects. Do you need to increase the scope of the observability in config? Trying to access a project outside the scope of the current access/token")
                    else:
                        # check for branches to scan
                        if top_branches_to_scan == 0:
                            # only default branch
                            branches = [enriched_build_definition['repository']['defaultBranch']]
                            branches_names = [default_branch.split('/')[-1]]
                        else:
                            # Look at different branches of the repository where the configuration sits
                            branches, branches_names = self.get_repository_branches(source_project_id, repo_id, project_name, repo_name, top_branches_to_scan, default_branch.split('/')[-1])

                        for branches_name in branches_names:
                            if build_definition['queueStatus'] == "disabled":
                                # If the build definition is disabled, no preview available
                                enriched_build_definition['builds']['preview'][branches_name] = {}
                                enriched_build_definition['builds']['preview'][branches_name]['is_yaml_preview_available'] = False
                                enriched_build_definition['builds']['preview'][branches_name]['yaml'] = "Build Definition is disabled"
                                enriched_build_definition['builds']['preview'][branches_name]['pipeline_recipe'] = "Build Definition is disabled"
                                enriched_build_definition['builds']['preview'][branches_name]['cicd_sast'] = []

                                if enriched_build_definition['process']['type'] == 1:
                                    # Designer pipeline, no preview available
                                    # remove designer json from the process
                                    enriched_build_definition['process'].pop('phases', None)
                                    enriched_build_definition['process'].pop('target', None)
                                    preview = {}
                                continue
                            # If the build definition is enabled, get the preview
                            if enriched_build_definition['process']['type'] == 1:
                                # Designer pipeline, no preview available
                                # remove designer json from the process
                                enriched_build_definition['process'].pop('phases', None)
                                enriched_build_definition['process'].pop('target', None)
                                preview = {}
                            else:
                                # Enrich preview payload with last run template parameters and build definition variables
                                dict_payload = {
                                    "resources": {
                                        "pipelines": {},
                                        "repositories": {
                                            "self": {
                                                "refName": branches_name
                                            }
                                        },
                                        "builds": {},
                                        "containers": {},
                                        "packages": {}
                                    },
                                    "templateParameters": {},
                                    "previewRun": True,
                                    "yamlOverride": ""
                                }
                                # Find last run for this build definition and branch
                                # Use the already-fetched builds array for this definition
                                branch_builds = [b for b in builds if b.get('sourceBranch') == f"refs/heads/{branches_name}" and b.get('finishTime')]
                                if branch_builds:
                                    # Get the build with the latest finishTime
                                    latest_build = max(branch_builds, key=lambda b: b['finishTime'])
                                    template_params = latest_build.get('templateParameters', {})
                                    if isinstance(template_params, str):
                                        try:
                                            template_params = json.loads(template_params)
                                        except Exception:
                                            template_params = {}
                                    dict_payload['templateParameters'] = template_params
                                    build_vars = latest_build.get('variables', {})
                                    if build_vars:
                                        dict_payload['resources']['builds'] = {"variables": {k: v.get('value') for k, v in build_vars.items() if isinstance(v, dict) and 'value' in v}}
                                payload = json.dumps(dict_payload)
                                preview, errorMessage = post_data(preview_url, payload, self.token)
                            enriched_build_definition['builds']['preview'][branches_name] = {}
                            # Set is_yaml_preview_available to False by default
                            enriched_build_definition['builds']['preview'][branches_name]['is_yaml_preview_available'] = False
                            if 'cicd_sast' not in enriched_build_definition['builds']['preview'][branches_name].keys():
                                enriched_build_definition['builds']['preview'][branches_name]['cicd_sast'] = []
                            if preview is not None:
                                if preview == {}:
                                    # empty dictionary, no YAML preview available - may be a designer pipeline
                                    yaml_url = f"https://dev.azure.com/{self.organization}/{project}/{manager_pipeline['build_definitions']['api_endpoint']}/{enriched_build_definition['id']}/yaml?{manager_pipeline['build_definitions']['api_version']}"
                                    yaml_preview = fetch_data(yaml_url, self.token, True)
                                    if isinstance(yaml_preview, str) or (yaml_preview!={} and yaml_preview.get('message',"") != f"Build pipeline {str(enriched_build_definition['id'])} is not designer."):
                                        if yaml_preview:
                                            yaml_preview_json = json.loads(yaml_preview)
                                            enriched_build_definition['builds']['preview'][branches_name]['yaml'] = yaml_preview_json['yaml']
                                            pipeline_recipe = self.parse_pipeline_yaml(yaml_preview_json['yaml'])
                                            enriched_build_definition['builds']['preview'][branches_name]['pipeline_recipe'] = pipeline_recipe
                                            scope = "potential_pipeline_execution_yaml"
                                            engine = "regex"
                                            res = self.scan_string_with_regex(yaml_preview, engine, branches_name + " @ " + enriched_build_definition['_links']['self']['href'])
                                            enriched_build_definition['builds']['preview'][branches_name]['cicd_sast'].append({
                                                "engine": engine,
                                                "scope": scope,
                                                "results": res
                                            })
                                            enriched_build_definition['builds']['preview'][branches_name]['is_yaml_preview_available'] = True
                                    else:
                                        enriched_build_definition['builds']['preview'][branches_name]['yaml'] = "Empty YAML PREVIEW"
                                        pipeline_recipe = self.parse_pipeline_yaml(preview)
                                        enriched_build_definition['builds']['preview'][branches_name]['pipeline_recipe'] = pipeline_recipe
                                        scope = "potential_pipeline_execution_yaml"
                                        engine = "regex"
                                        res = self.scan_string_with_regex(yaml_preview, engine, branches_name + " @ " + enriched_build_definition['_links']['self']['href'])
                                        enriched_build_definition['builds']['preview'][branches_name]['cicd_sast'].append({
                                            "engine": engine,
                                            "scope": scope,
                                            "results": res
                                        })
                                        enriched_build_definition['builds']['preview'][branches_name]['is_yaml_preview_available'] = False
                                else:
                                    enriched_build_definition['builds']['preview'][branches_name]['yaml'] = preview['finalYaml']
                                    pipeline_recipe = self.parse_pipeline_yaml(preview['finalYaml'])
                                    enriched_build_definition['builds']['preview'][branches_name]['pipeline_recipe'] = pipeline_recipe
                                    enriched_build_definition['builds']['preview'][branches_name]['is_yaml_preview_available'] = True
                                    scope = "potential_pipeline_execution_yaml"
                                    engine = "regex"
                                    res = self.scan_string_with_regex(preview['finalYaml'], engine, branches_name+" @ "+enriched_build_definition['_links']['self']['href'])
                                    enriched_build_definition['builds']['preview'][branches_name]['cicd_sast'].append({
                                        "engine": engine,
                                        "scope": scope,
                                        "results": res
                                    })
                            else:
                                enriched_build_definition['builds']['preview'][branches_name]['yaml'] = f"Could not get YAML PREVIEW - {errorMessage}"
                                enriched_build_definition['builds']['preview'][branches_name]['cicd_sast'] = []
                                enriched_build_definition['builds']['preview'][branches_name]['is_yaml_preview_available'] = False
                # Add enriched_build_definition to build_def_list
                build_def_list.append(enriched_build_definition)
        return build_def_list, builds_list

    def get_build_definition_authorised_resources(self, build_definitions, manager_pipeline={"preview":{"api_version": "api-version=7.1", "api_endpoint": "_apis/pipelines"}, "builds":{"api_version": "api-version=7.1", "api_endpoint": "_apis/build/builds"}, "build_definitions":{"api_version": "api-version=7.1", "resources_api_version": "api-version=7.2-preview.1", "api_endpoint": "_apis/build/definitions"}}):

        for build_definition in build_definitions:

            project, build_definition_id = build_definition['k_key'].split("_")
            build_definition['resources'] = []

            url = f"https://dev.azure.com/{self.organization}/{project}/{manager_pipeline['build_definitions']['api_endpoint']}/{str(build_definition_id)}/resources?{manager_pipeline['build_definitions']['resources_api_version']}"

            authorised_resources = fetch_data(url, self.token)

            for authorised_resource in authorised_resources:
                build_definition['resources'].append(authorised_resource)

        return build_definitions

    # def get_enriched_build_with_log_secret_scan(self, build, secrets_engine="gitleaks", gitleaks_installed=True):
    #     if not gitleaks_installed:
    #         return build  # Skip secret scanning and do not print info

    #     secret_scanning_logs = []
    #     url = build['logs']['url']

    #     headers = {
    #         'Content-Type': 'application/json',
    #         'Authorization': f'Basic {self.token}',
    #     }

    #     try:
    #         response = http.get(url, headers=headers)
    #         response.raise_for_status()

    #         # Parse JSON response

    #         try:
    #             metadata_logs = response.json()
    #         except json.JSONDecodeError:
    #             print(f"\tCould not parse log JSON response for build {build['id']}")
    #             return secret_scanning_logs


    #         if 'value' not in metadata_logs.keys() or len(metadata_logs['value']) == 0:
    #             print(f"\tNo logs for build {build['id']}")
    #             return
            
    #         build_secret_results = []
    #         regex_results = []

    #         for metadata_log in metadata_logs['value']:
    #             url = metadata_log['url']

    #             headers = {
    #                 'Content-Type': 'application/json',
    #                 'Authorization': f'Basic {self.token}',
    #             }

    #             try:
    #                 response = http.get(url, headers=headers)
    #                 response.raise_for_status()

    #                 # Parse JSON response
    #                 logs = response.text

    #             except requests.exceptions.HTTPError as http_err:
    #                 print(f"\tHTTP error occurred while fetching logs: {http_err}")
                
    #             selectedEngine = secrets_engine
    #             results = {}

    #             if logs and selectedEngine == "gitleaks" and gitleaks_installed:
    #                 results = self.scan_text_gitleaks(logs, url)
    #             if results and results != []:
    #                 build_secret_results.append(results)
    #             else:
    #                 pass

    #             engine = "regex"
    #             scope = "pipeline_execution_logs"
    #             if metadata_log['id'] == 1 and logs:
    #                 regex_results = self.scan_string_with_regex(logs, engine, url)
                

    #         if 'cicd_sast' not in build.keys():
    #             build['cicd_sast'] = []
        
    #         build['cicd_sast'].append({
    #             "engine": engine,
    #             "scope": scope,
    #             "results": regex_results,
    #         })

    #         build['cicd_sast'].append({
    #             "engine": "gitleaks",
    #             "scope": "build_logs",
    #             "results": build_secret_results,
    #         })

    #         return build

    #     except requests.exceptions.HTTPError as http_err:
    #         print(f"\tHTTP error occurred while fetching logs: {http_err}")

    # def scan_text_gitleaks(self, data_to_scan, source_of_data):

    #     """Runs gitleaks scan and returns found secrets. Assumes gitleaks is installed if called."""

    #     gitleaks_args = ["gitleaks", "stdin", "--redact", "--verbose", "--no-banner"]
    #     process = subprocess.run(
    #         gitleaks_args,
    #         input=data_to_scan, text=True, capture_output=True
    #     )
    #     secrets_found = []
    #     # Extract findings using regex
    #     pattern = re.compile(r"Finding:\s+(.*)\nSecret:\s+(.*)\nRuleID:\s+(.*)\nEntropy:\s+([\d.]+)")
    #     matches = pattern.findall(process.stdout)
    #     for match in matches:
    #         finding, secret, rule_id, entropy = match
    #         secrets_found.append({
    #             "finding": finding,
    #             "secret": secret,
    #             "rule_id": rule_id,
    #             "entropy": entropy
    #         })

    #     # Filter out any non-normal characters from the secrets_found data
    #     sanitized_secrets = []
    #     for secret in secrets_found:
    #         sanitized_secret = {
    #             "source": source_of_data,
    #             "finding": re.sub(r'\x1B(?:[@-Z\\-_]|\\[[0-?]*[ -/]*[@-~])', '', secret["finding"]),
    #             "secret": re.sub(r'\x1B(?:[@-Z\\-_]|\\[[0-?]*[ -/]*[@-~])', '', secret["secret"]),
    #             "rule_id": re.sub(r'\x1B(?:[@-Z\\-_]|\\[[0-?]*[ -/]*[@-~])', '', secret["rule_id"]),
    #             "entropy": re.sub(r'\x1B(?:[@-Z\\-_]|\\[[0-?]*[ -/]*[@-~])', '', secret["entropy"]),
    #         }
    #         sanitized_secrets.append(sanitized_secret)

    #     return sanitized_secrets

    def get_enriched_stats(self, stats, resource_inventory, definitions, builds, commits, artifacts):
        print("Enriching stats with resource counts")

        for project in stats:
            print(f"Processing stats for project: {project}")
            if "resource_counts" not in stats[project]:
                stats[project]["resource_counts"] = {}
            # Count only definitions for this project (by k_project)
            stats[project]["resource_counts"]["pipelines"] = len([
                d for d in definitions
                if d.get("k_project") and project == d["k_project"]["id"]
            ])
            # Count only builds for this project (by k_project)
            stats[project]["resource_counts"]["builds"] = len([
                b for b in builds
                if b.get("k_project") and project == b["k_project"]["id"]
            ])
            stats[project]["resource_counts"]["endpoint"] = len([
                r for r in resource_inventory['endpoint']['protected_resources']
                if r['resource'].get('k_projects_refs') and any(
                    ref.get('id') == project for ref in r['resource']['k_projects_refs']
                )
            ])
            stats[project]["resource_counts"]["variablegroup"] = len([
                r for r in resource_inventory['variablegroup']['protected_resources']
                if r['resource'].get('k_project') and project == r['resource']['k_project']['id']
            ])
            stats[project]["resource_counts"]["securefile"] = len([
                r for r in resource_inventory['securefile']['protected_resources']
                if r['resource'].get('k_project') and project == r['resource']['k_project']['id']
            ])
            stats[project]["resource_counts"]["queue"] = len([
                r for r in resource_inventory['queue']['protected_resources']
                if r['resource'].get('k_project') and project == r['resource']['k_project']['id']
            ])
            stats[project]["resource_counts"]["repository"] = len([
                r for r in resource_inventory['repository']['protected_resources']
                if r['resource'].get('k_project') and project == r['resource']['k_project']['id']
            ])
            stats[project]["resource_counts"]["environment"] = len([
                r for r in resource_inventory['environment']['protected_resources']
                if r['resource'].get('k_project') and project == r['resource']['k_project']['id']
            ])
            stats[project]["resource_counts"]["commits"] = len([
                c for c in commits
                if c.get('k_project') and project == c['k_project']['id']
            ])
            # Count unique committers for this project
            unique_committers = {
                c['committerEmail']
                for c in commits
                if c.get('k_project') and c['k_project']['id'] == project and c.get('committerEmail')
            }
            stats[project]["resource_counts"]["unique_committers"] = len(unique_committers)

            # Artifact feeds: count feeds in active+recyclebin where k_project.id==project or no k_project (org-wide)
            feeds_count = 0
            for feed in artifacts.get('active', []):
                k_proj = feed.get('k_project')
                if (k_proj and k_proj.get('id') == project) or (not k_proj):
                    feeds_count += 1
            for feed in artifacts.get('recyclebin', []):
                k_proj = feed.get('k_project')
                if (k_proj and k_proj.get('id') == project) or (not k_proj):
                    feeds_count += 1
            stats[project]["resource_counts"]["artifacts_feeds"] = feeds_count

            # Artifact packages: sum of len(packages) for each active feed where k_project.id==project or no k_project (org-wide)
            packages_count = 0
            for feed in artifacts.get('active', []):
                k_proj = feed.get('k_project')
                if (k_proj and k_proj.get('id') == project) or (not k_proj):
                    packages = feed.get('packages', [])
                    if isinstance(packages, list):
                        packages_count += len(packages)
            stats[project]["resource_counts"]["artifacts_packages"] = packages_count

        return stats

    # PROTECTED RESOURCES

    def calculate_pool_pipeline_permissions(self, queues):
        dedup_permissions = []
        for queue in queues:
            if "pipelinepermissions" in queue:
                for permission in queue["pipelinepermissions"]:
                    if permission not in dedup_permissions:
                        dedup_permissions.append(permission)
        return dedup_permissions

    # Merge pools and queues (1 pool, multiple queues)
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
            p['pipelinepermissions'] = self.calculate_pool_pipeline_permissions(p["queues"])

        return pools

    def enrich_k_project(self, curr_project_id, self_attribute=None, current_project_name=None):
        """
        Returns a simple project object (id, name, self_attribute) for the given project id, as a dict keyed by project id.
        """
        if curr_project_id in self.projects:
            proj = self.projects[curr_project_id]
            k_proj = {
                "type": "project",
                "id": curr_project_id,
                "name": proj.get("name"),
            }
            if self_attribute:
                k_proj["self_attribute"] = self_attribute
            return k_proj
        elif current_project_name:
            # If the project is not in self.projects, we can still create a k_project object with the name
            k_proj = {
                "type": "project",
                "id": curr_project_id,
                "name": current_project_name,
            }
            if self_attribute:
                k_proj["self_attribute"] = self_attribute
            return k_proj
        return {}

    def enrich_protected_resources_projectinfo(self, resource_type, resource, curr_project_id):
        match resource_type:
            case "pools":
                resource['k_project'] = {}
                org = {
                    "type": "org",
                    "name": self.organization,
                    "id": resource['scope'],
                    "self_attribute": f"https://dev.azure.com/{self.organization}/_settings/agentpools?poolId={resource['id']}&view=agents"
                }
                resource['k_project'] = {resource['scope']: org}
                return resource
            case "queue":
                resource['k_project'] = self.enrich_k_project(resource['projectId'], f"https://dev.azure.com/{self.organization}/{resource['projectId']}/_settings/agentqueues?queueId={resource['id']}&view=agents")
                return resource
            case "endpoint":
                # current project
                resource['k_project'] = {}
                resource['k_project'] = self.enrich_k_project(curr_project_id, f"https://dev.azure.com/{self.organization}/{curr_project_id}/_settings/adminservices?resourceId={resource['id']}")
                # referenced projects
                resource['k_projects_refs'] = []
                for projectReference in resource['serviceEndpointProjectReferences']:
                    pid = projectReference['projectReference']['id']
                    self_attribute = f"https://dev.azure.com/{self.organization}/{pid}/_settings/adminservices?resourceId={resource['id']}"
                    resource['k_projects_refs'].append(self.enrich_k_project(pid, self_attribute, projectReference['projectReference']['name']))
                # Get the organisation permissions
                if resource['isShared']:
                    resource['k_project_shared_from'] = self.get_k_shared_from_endpoint(resource)
                return resource
            case "variablegroup":
                resource['k_project'] = self.enrich_k_project(curr_project_id, f"https://dev.azure.com/{self.organization}/{curr_project_id}/_library?itemType=VariableGroups&view=VariableGroupView&variableGroupId={resource['id']}")
                return resource
            case "securefile":
                resource['k_project'] = self.enrich_k_project(curr_project_id, f"https://dev.azure.com/{self.organization}/{curr_project_id}/_library?itemType=SecureFiles&view=SecureFileView&secureFileId={resource['id']}")
                return resource
            case "repository":
                resource['k_project'] = self.enrich_k_project(curr_project_id, resource.get('webUrl'))
                return resource
            case "environment":
                resource['k_project'] = self.enrich_k_project(curr_project_id, f"https://dev.azure.com/{self.organization}/{curr_project_id}/_environments/{resource['id']}")
                return resource
        return resource

    def get_deployment_group_details(self, project_id, deployment_group):

        # deployment groups are project specific (like queues) 
        # get deployment pool from it - deployment_group['pool']

        # to get all deploymentgroups associated with a single pool, is in the end merge of pools and deploymentgroups - for each pool, get all deploymentgroups associated with it
        
        # they link to a deploymentPool (like pools) which link to machines (targets)
        # they also link to environments (see based on the name)


        # get macghines and pool
        url = f"https://dev.azure.com/{self.organization}/{project_id}/_apis/distributedtask/deploymentgroups/{deployment_group['id']}"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Basic {self.token}',
        }

        try:
            response = http.get(url, headers=headers)
            response.raise_for_status()

            data = response.json()
            deployment_group['machines'] = data.get('machines', []) # targets
            deployment_group['pool'] = data.get('pool', {})         # org level concept - has permissions?

            deployment_group['tags'] = data.get('tags', [])
            deployment_group['createdBy'] = data.get('createdBy', {})
            deployment_group['modifiedBy'] = data.get('modifiedBy', {})
            deployment_group['createdOn'] = data.get('createdOn', "")
            deployment_group['modifiedOn'] = data.get('modifiedOn', "")

            return deployment_group

        except requests.exceptions.HTTPError as http_err:
            print(f"\tHTTP error occurred while fetching deployment group details: {http_err}")
        except Exception as err:
            print(f"\tAn error occurred while fetching deployment group details: {err}")

        return deployment_group

    def get_protected_resources(self, inventory):

        for project in self.projects:
            # Only process wellFormed projects
            if isinstance(self.projects[project], dict):
                if self.projects[project].get('state', '').lower() != 'wellformed':
                    continue

            for inventory_key, inventory_value in inventory.items():
                
                if inventory_value['level'] == "org":
                    url = f"https://dev.azure.com/{self.organization}/_apis/{inventory_value['api_endpoint']}"
                else:    
                    url = f"https://dev.azure.com/{self.organization}/{project}/_apis/{inventory_value['api_endpoint']}"

                if inventory_value.get('query_params'):
                    url = f"{url}?{inventory_value['query_params']}"

                headers = {
                    'Content-Type': 'application/json',
                    'Authorization': f'Basic {self.token}',
                }

                try:
                    print(f"\tDISCOVERING {inventory_key} @ {self.projects[project]['name']}")
                    
                    print(f"\t{url}")
                    response = http.get(url, headers=headers)
                    response.raise_for_status()

                    data = response.json()

                    new_resources = data['value']
                    curr_resources_ids = [curr_resource['resource']["id"] for curr_resource in inventory_value['protected_resources']]
                    
                    print(f"\t\t{str(len(new_resources))} {inventory_key} found")

                    for new_resource in new_resources:
                        if new_resource['id'] not in curr_resources_ids:

                            if inventory_key in ["environment", "deploymentgroups"]:
                                # query individually /id
                                url = f"https://dev.azure.com/{self.organization}/{project}/_apis/{inventory_value['api_endpoint']}/{new_resource['id']}?{inventory_value['query_params']}"
                                print(f"\t\tEnriching {inventory_key} details for {new_resource['name']} @ {self.projects[project]['name']}")
                                response = http.get(url, headers=headers)
                                response.raise_for_status()
                                try:
                                    new_resource = response.json()
                                except json.JSONDecodeError:
                                    print(f"\t\tFailed to parse JSON for {inventory_key} {new_resource['name']} @ {self.projects[project]['name']}")
                                    pass

                            pname = urllib.parse.quote(self.projects[project]['name'])
                            if inventory_key == "pools":
                                new_resource['k_url'] = f"https://dev.azure.com/{self.organization}/_settings/agentpools?poolId={new_resource['id']}"
                            elif inventory_key == "queue":
                                new_resource['k_url'] = f"https://dev.azure.com/{self.organization}/{pname}/_settings/agentqueues?queueId={new_resource['id']}"
                            elif inventory_key == "endpoint":
                                new_resource['k_url'] = f"https://dev.azure.com/{self.organization}/{pname}/_settings/adminservices?resourceId={new_resource['id']}"
                            elif inventory_key == "repository":
                                new_resource['k_url'] = f"https://dev.azure.com/{self.organization}/{pname}/_git/{new_resource['name']}"
                            elif inventory_key == "securefile":
                                new_resource['k_url'] = f"https://dev.azure.com/{self.organization}/{pname}/_library?itemType=SecureFiles&view=SecureFileView&secureFileId={new_resource['id']}"
                            elif inventory_key == "variablegroup":
                                new_resource['k_url'] = f"https://dev.azure.com/{self.organization}/{pname}/_library?itemType=VariableGroups&view=VariableGroupView&variableGroupId={new_resource['id']}"
                            elif inventory_key == "environment":
                                new_resource['k_url'] = f"https://dev.azure.com/{self.organization}/{pname}/_environments/{new_resource['id']}?view=deployments"
                            elif inventory_key == "deploymentgroups":
                                new_resource['k_url'] = f"https://dev.azure.com/{self.organization}/{pname}/_machinegroup?view=MachineGroupView&mgid={new_resource['id']}&tab=Details"

                            new_resource = self.enrich_protected_resources_projectinfo(inventory_key, new_resource, project)
                            if inventory_key == "deploymentgroups":
                                new_resource = self.get_deployment_group_details(project, new_resource)

                            enriched_resource = {
                                "resourceType": inventory_key,
                                "resource": new_resource
                            }

                            inventory_value['protected_resources'].append(enriched_resource)
                            curr_resources_ids.append(new_resource['id'])

                except requests.exceptions.HTTPError as http_err:
                    print(f"\tHTTP error occurred: {http_err}")
                except Exception as err:
                    print(f"\tAn error occurred: {err}")

        try:
            inventory['pools']['protected_resources'] = self.merge_pools_and_queues(inventory['pools']['protected_resources'], inventory['queue']['protected_resources'])

        except Exception as e:
            logging.getLogger('gunicorn.error').warning(
                f"Failed to merge pools and queues for project {self.projects[project]} ({project}): {e}"
            )

        # Add branches to each repository in the inventory
        for repository in inventory['repository']['protected_resources']:
            repo = repository['resource']

            branches, branches_names = self.get_repository_branches(repo['project']['id'], repo['id'], repo['project']['name'], repo['name'], -1, '')
            repo['branches'] = branches

            firstCommitDate, lastCommitDate = self.get_repository_commit_dates(repo['project']['id'], repo['id'])
            repo['stats'] = {}
            from datetime import datetime, timezone
            repo['stats']['firstCommitDate'] = firstCommitDate.isoformat() if isinstance(firstCommitDate, datetime) and firstCommitDate else firstCommitDate
            repo['stats']['lastCommitDate'] = lastCommitDate.isoformat() if isinstance(lastCommitDate, datetime) and lastCommitDate else lastCommitDate
            repo['stats']['age'] = (datetime.now(timezone.utc) - lastCommitDate).days if lastCommitDate else None
            repo['stats']['branches'] = len(branches_names)
            repo['stats']['pullRequests'] = self.get_repository_pull_requests_count(repo['project']['id'], repo['id'])
            if lastCommitDate:
                now = datetime.now(timezone.utc)
                if lastCommitDate > now - timedelta(days=90):
                    repo['stats']['state'] = "active"
                elif lastCommitDate > now - timedelta(days=365):
                    repo['stats']['state'] = "stale"
                else:
                    repo['stats']['state'] = "dormant"
            else:
                repo['stats']['state'] = "unknown"


        return inventory

    def get_enriched_build_definitions(self, definitions, resource_inventory):
        print("Enriching build definitions with protected resources")
        definitions_map = {}

        for resource_type in resource_inventory:
            for protected_resource in resource_inventory[resource_type]['protected_resources']:
                actually_protected_resource = protected_resource['resource']
                print(f"Processing {protected_resource['resourceType']} {actually_protected_resource['id']}")
                if protected_resource['resourceType'] == "pools":
                    for queue in actually_protected_resource['queues']:
                        if 'pipelinepermissions' not in actually_protected_resource:
                            print("No pipelinepermissions found for this resource")
                            continue
                        for pipelinepermission in queue['pipelinepermissions']:
                            if pipelinepermission not in definitions_map:
                                definitions_map[pipelinepermission] = []
                            definitions_map[pipelinepermission].append(f"pool_merged_{actually_protected_resource['id']}")
                            definitions_map[pipelinepermission].append(f"queue_{queue['id']}")
                elif protected_resource['resourceType'] == "deploymentgroups":
                    print("Skipping deployment groups for now - permissions should be gotten from the pools/queues - so I would... lookup")
                    print(actually_protected_resource)
                    continue
                    for pipelinepermission in actually_protected_resource['pipelinepermissions']:
                        pipelinepermission = str(pipelinepermission['id'])
                        if pipelinepermission not in definitions_map:
                            definitions_map[pipelinepermission] = []
                        definitions_map[pipelinepermission].append(f"{protected_resource['resourceType']}_{actually_protected_resource['id']}")
                else:
                    if 'pipelinepermissions' not in actually_protected_resource:
                        actually_protected_resource['pipelinepermissions'] = []
                    for pipelinepermission in actually_protected_resource['pipelinepermissions']:
                        if pipelinepermission not in definitions_map:
                            definitions_map[pipelinepermission] = []
                        definitions_map[pipelinepermission].append(f"{protected_resource['resourceType']}_{actually_protected_resource['id']}")

        # definitions is now a list, so we need to find the right object by k_key
        for key, def_resourcepermissions in definitions_map.items():
            def_obj = next((d for d in definitions if d.get('k_key') == key), None)
            if def_obj is None:
                continue
            if 'resourcepermissions' not in def_obj:
                def_obj['resourcepermissions'] = {}
            for res_permission in def_resourcepermissions:
                res_permission_type, res_permission_id = res_permission.rsplit("_", 1)
                if res_permission_type not in def_obj['resourcepermissions']:
                    def_obj['resourcepermissions'][res_permission_type] = []
                if res_permission_id not in def_obj['resourcepermissions'][res_permission_type]:
                    def_obj['resourcepermissions'][res_permission_type].append(res_permission_id)
        return definitions

    def parse_pipeline_yaml(self, yaml_content):
        """
        Parses the Azure DevOps pipeline YAML into a structured format with stages, jobs, and steps.
        """
        if not yaml_content:
            print("No YAML content provided.")
            return None
        try:
            pipeline_data = yaml.safe_load(yaml_content)
            return pipeline_data

        except yaml.YAMLError as e:
            print(f"Error parsing YAML: {e}")
            return None


    def get_project_language_metrics(self, projects):
        """
        Retrieves language metrics for each project in the organization.
        """
        headers = {
            "Authorization": f"Basic {self.token}",
            "Content-Type": "application/json"
        }

        stats = {}
        for project in projects:
            project_id = project.get('id')
            project_name = project.get('name')
            url = f"https://dev.azure.com/{self.organization}/{project_name}/_apis/projectanalysis/languagemetrics?api-version=6.0-preview.1"

            try:
                response = http.get(url, headers=headers)
                response.raise_for_status()
                language_stats = response.json()
                stats[project_id] = {
                    "language_stats": language_stats,
                    # "repositories": repos
                }

            except requests.exceptions.HTTPError as http_err:
                logging.getLogger('gunicorn.error').warning(
                    f"Failed to retrieve metrics for project {project_name}: {http_err}"
                )
            except Exception as err:
                logging.getLogger('gunicorn.error').warning(
                    f"An error occurred while retrieving metrics for project {project_name}: {err}"
                )

        return stats

    def get_k_shared_from_endpoint(self, resource):
        """
        Retrieves the original project the endpoint is shared from.
        """
        url = f"https://dev.azure.com/{self.organization}/_apis/securityroles/scopes/distributedtask.collection.serviceendpointrole/roleassignments/resources/collection_{resource['id']}?api-version=7.1-preview.1"
        try:
            result = fetch_data(url, self.token)
            matches = []
            references = resource.get('serviceEndpointProjectReferences', [])
            for ref in references:
                project_name = ref.get('projectReference', {}).get('name', '')
                # result may be a list or a dict with 'value' key
                entries = result.get('value', []) if isinstance(result, dict) else result
                for entry in entries:
                    identity_name = entry.get('identity', {}).get('displayName', '')
                    if project_name and project_name in identity_name:
                        matches.append({
                            'Id': ref.get('projectReference', {}).get('id', ''),
                            'name': project_name
                        })
            return matches
        except Exception as err:
            logging.getLogger('gunicorn.error').warning(
                f"An error occurred while retrieving shared info for resource {resource['id']}: {err}"
            )
        return None
    
    def get_task_list(self):
        """
        Retrieves the list of tasks (build/release tasks) available in the organization or a specific project.
        If project is None, gets all tasks at the org level.
        """
        url = f"https://dev.azure.com/{self.organization}/_apis/distributedtask/tasks?api-version=7.1"
        try:
            tasks = fetch_data(url, self.token)
            if tasks is None:
                print("Failed to fetch task list.")
                return []
            return tasks
        except Exception as err:
            print(f"An error occurred while retrieving task list: {err}")
            return []
    
    def get_all_build_service_accounts(self):
        url = f"https://vssps.dev.azure.com/{self.organization}/_apis/graph/users?api-version=7.1-preview.1&subjectTypes=svc"
        users = fetch_data(url, self.token)
        results = []
        if not users:
            return results
        for user in users:
            if user.get('domain') != 'Build':
                continue
            principal_name = user.get('principalName', '')
            display_name = user.get('displayName', '')
            # Remove ' Build Service (org)' from display_name to get project name
            suffix = f" Build Service ({self.organization})"
            if display_name.endswith(suffix):
                project_name = display_name[:-len(suffix)].strip()
            else:
                project_name = ''
            description = f"A build service account for project {project_name}."
            results.append({
                'id': f"Build\\{principal_name}",
                'name': display_name,
                'description': description
            })
        return results
