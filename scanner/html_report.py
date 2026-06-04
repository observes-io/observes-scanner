# HTML Report Generator for Scan Results
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0

import os
import json
from datetime import datetime
from typing import Any, Dict, Optional
from collections import defaultdict


def _contains_any(text: str, terms) -> bool:
    lowered = (text or "").lower()
    return any(term in lowered for term in terms)


def _is_admin_group(group: Dict[str, Any]) -> bool:
    group_text = " ".join([
        group.get("displayName", ""),
        group.get("principalName", ""),
        group.get("description", ""),
    ]).lower()
    return _contains_any(group_text, [
        "admin",
        "administrator",
        "project collection administrators",
    ])


def _classify_pat_scope(scope: str) -> str:
    scope_text = (scope or "").lower()

    very_high_terms = [
        "app_token",
    ]

    high_terms = [
        "vso.tokens",
        "vso.tokenadministration",
        "vso.pats",
        "vso.entitlements_manage",
        "vso.governance",
        "vso.security_manage",
        "vso.identity_manage",
        "vso.code_full",
        "vso.project_manage",
        "vso.build_execute",
    ]

    if _contains_any(scope_text, very_high_terms):
        return "very_high"

    if _contains_any(scope_text, high_terms):
        return "high"

    return "normal"


def _build_iam_rbac_summary(result: Dict[str, Any]) -> Dict[str, int]:
    users = result.get("users", {}) or {}
    groups = result.get("groups", {}) or {}

    users_list = list(users.values())
    groups_list = list(groups.values())

    total_users = len(users_list)
    aad_users = sum(1 for user in users_list if (user.get("origin") or "").lower() == "aad")
    service_users = max(total_users - aad_users, 0)

    total_groups = len(groups_list)
    admin_groups = sum(1 for group in groups_list if _is_admin_group(group))

    total_pats = 0
    active_pats = 0
    expired_pats = 0
    high_privileged_pats = 0
    very_high_privileged_pats = 0
    high_privilege_user_ids = set()

    for user_id, user in users.items():
        for token in user.get("patTokens", []) or []:
            total_pats += 1
            is_active = token.get("isValid") is True
            if is_active:
                active_pats += 1
            else:
                expired_pats += 1

            privilege = _classify_pat_scope(token.get("scope", ""))
            if is_active and privilege in ("high", "very_high"):
                high_privilege_user_ids.add(user_id)

            if is_active and privilege == "high":
                high_privileged_pats += 1
            if is_active and privilege == "very_high":
                very_high_privileged_pats += 1

    return {
        "total_users": total_users,
        "aad_users": aad_users,
        "service_users": service_users,
        "total_groups": total_groups,
        "admin_groups": admin_groups,
        "total_pats": total_pats,
        "active_pats": active_pats,
        "expired_pats": expired_pats,
        "high_privilege_users": len(high_privilege_user_ids),
        "high_privileged_pats": high_privileged_pats,
        "very_high_privileged_pats": very_high_privileged_pats,
    }

def write_html_report(result: Dict[str, Any], results_dir: str, job_id: str, config=None) -> str:
    """
    Generate a clean, filterable HTML report from scan results.
    Includes scanner metadata, organization info, resource counts, scan configuration,
    per-project breakdown, and regex scan findings.
    
    Args:
        result: The scan result dictionary
        results_dir: Directory to save the report
        job_id: Job ID for the scan
        config: ScannerConfig object with scan parameters (optional)
    """
    org = result.get("organisation", {})
    resource_counts = org.get("resource_counts", {})
    project_refs = org.get("projectRefs", [])
    stats = result.get("stats", {})
    builds = result.get("builds", [])
    iam_rbac = _build_iam_rbac_summary(result)
    
    # Create project name lookup
    project_names = {proj["id"]: proj["name"] for proj in project_refs}
    
    # Aggregate regex scan findings
    regex_findings = defaultdict(int)
    total_findings = 0
    
    for build in builds:
        cicd_sast = build.get("cicd_sast", [])
        for scan in cicd_sast:
            if scan.get("engine") == "regex":
                for finding in scan.get("results", []):
                    category = finding.get("category", "Unknown")
                    regex_findings[category] += 1
                    total_findings += 1
    
    # Load base64 encoded logo
    logo_base64_path = os.path.join(os.path.dirname(__file__), "..", "logo", "logo_base64.txt")
    try:
        with open(logo_base64_path, 'r') as f:
            logo_base64 = f.read().strip()
        logo_data_url = f"data:image/png;base64,{logo_base64}"
    except Exception:
        logo_data_url = ""  # Fallback to no logo if file not found
    
    # Parse scan times
    scan_start = result.get("scan_start", "")
    scan_end = result.get("scan_end", "")
    try:
        start_dt = datetime.fromisoformat(scan_start)
        end_dt = datetime.fromisoformat(scan_end)
        duration = end_dt - start_dt
        duration_str = str(duration).split('.')[0]  # Remove microseconds
        scan_start_formatted = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        scan_end_formatted = end_dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        scan_start_formatted = scan_start
        scan_end_formatted = scan_end
        duration_str = "N/A"
    
    # Build scan configuration from actual config object
    scan_config = []
    if config:
        # Organization and Job
        scan_config.append(f"Organization: {config.organization}")
        scan_config.append(f"Job ID: {config.job_id}")
        
        # Project filtering
        if config.projects:
            projects_str = ", ".join(config.projects[:3])
            if len(config.projects) > 3:
                projects_str += f" (+{len(config.projects) - 3} more)"
            scan_config.append(f"Project Filter: {projects_str}")
        else:
            scan_config.append("Scope: All Projects")
        
        # Branch scanning
        if config.top_branches_to_scan == -1:
            scan_config.append("Branches: All branches")
        elif config.top_branches_to_scan == 0:
            scan_config.append("Branches: Default branch only")
        else:
            scan_config.append(f"Branches: Default + top {config.top_branches_to_scan}")
        
        # Optional features
        if config.resolve_identities:
            mode = "with resolution" if config.identity_resolution_resolve else "extract only"
            scan_config.append(f"Identity Resolution: Enabled ({mode})")
        
        # Skip options
        if config.skip_builds:
            scan_config.append("⊘ Skipped: Builds & Pipeline Data")
        if config.skip_feeds:
            scan_config.append("⊘ Skipped: Artifact Feeds")
        if config.skip_committer_stats:
            scan_config.append("⊘ Skipped: Committer Statistics")
        
        # Results location
        if config.results_dir:
            scan_config.append(f"Results Directory: {config.results_dir}")
    
    if not scan_config:
        scan_config = ["Configuration information not available"]
    
    # Load regex pattern descriptions from cicd_sast.json
    regex_pattern_info = {}
    try:
        patterns_file = os.path.join(os.path.dirname(__file__), "..", "datastore", "scanners", "patterns", "cicd_sast.json")
        with open(patterns_file, 'r') as f:
            patterns_data = json.load(f)
            for category in patterns_data.get("regex", {}).get("categories", []):
                regex_pattern_info[category["name"]] = {
                    "severity": category.get("severity", "unknown"),
                    "description": category.get("description", "")
                }
    except Exception:
        pass  # If file doesn't exist, continue without descriptions
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Scan Report - {org.get('name', 'Unknown Org')}</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: #f5f5f5;
            color: #333;
            padding: 20px;
            line-height: 1.6;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: #fff;
            border: 1px solid #ddd;
        }}
        .header {{
            display: flex;
            align-items: center;
            background: #2c3e50;
            color: #fff;
            padding: 20px 30px;
            border-bottom: 3px solid #3498db;
        }}
        .header img {{ 
            height: 60px;
            margin-right: 20px;
            filter: brightness(0) invert(1);
        }}
        .header-content h1 {{ 
            font-size: 24px;
            font-weight: 600;
            margin-bottom: 5px;
        }}
        .header-content .subtitle {{
            color: #bdc3c7;
            font-size: 14px;
        }}
        
        .content {{
            padding: 30px;
        }}
        
        .info-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 15px;
            margin-bottom: 30px;
        }}
        .info-card {{
            background: #f8f9fa;
            padding: 15px;
            border: 1px solid #dee2e6;
            border-left: 3px solid #3498db;
        }}
        .info-card .label {{
            color: #6c757d;
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 5px;
        }}
        .info-card .value {{
            color: #212529;
            font-size: 16px;
            font-weight: 600;
        }}
        
        .section {{
            margin-bottom: 30px;
        }}
        .section-title {{
            font-size: 20px;
            font-weight: 600;
            color: #2c3e50;
            margin-bottom: 15px;
            padding-bottom: 8px;
            border-bottom: 2px solid #3498db;
        }}
        
        .config-list {{
            background: #f8f9fa;
            padding: 15px;
            border: 1px solid #dee2e6;
        }}
        .config-list ul {{
            list-style: none;
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 10px;
        }}
        .config-list li {{
            padding: 8px 12px;
            background: #fff;
            border: 1px solid #dee2e6;
            border-left: 3px solid #28a745;
            font-size: 13px;
        }}
        .config-list li[data-skipped="true"] {{
            border-left-color: #ffc107;
            background: #fff9e6;
        }}
        .config-list li::before {{
            content: "✓ ";
            color: #28a745;
            font-weight: bold;
            margin-right: 5px;
        }}
        .config-list li[data-skipped="true"]::before {{
            content: "⊘ ";
            color: #ffc107;
        }}
        
        .search-box {{
            margin-bottom: 15px;
        }}
        .search-box input {{
            width: 100%;
            max-width: 400px;
            padding: 10px 15px;
            border: 1px solid #ced4da;
            font-size: 14px;
        }}
        .search-box input:focus {{
            outline: none;
            border-color: #3498db;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
            border: 1px solid #dee2e6;
        }}
        table thead {{
            background: #343a40;
            color: #fff;
        }}
        table th {{
            padding: 12px;
            text-align: left;
            font-weight: 600;
            font-size: 13px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            border: 1px solid #495057;
        }}
        table td {{
            padding: 10px 12px;
            border: 1px solid #dee2e6;
            font-size: 13px;
        }}
        table tbody tr:nth-child(even) {{
            background: #f8f9fa;
        }}
        table tbody tr:hover {{
            background: #e9ecef;
        }}
        .count {{
            font-weight: 600;
            color: #3498db;
        }}
        
        .project-link {{
            color: #3498db;
            text-decoration: none;
            font-weight: 600;
        }}
        .project-link:hover {{
            text-decoration: underline;
        }}
        
        .severity-critical {{ color: #dc3545; font-weight: 600; }}
        .severity-high {{ color: #fd7e14; font-weight: 600; }}
        .severity-medium {{ color: #ffc107; font-weight: 600; }}
        .severity-low {{ color: #28a745; font-weight: 600; }}
        
        .footer {{
            background: #f8f9fa;
            padding: 20px 30px;
            text-align: center;
            color: #6c757d;
            border-top: 1px solid #dee2e6;
            font-size: 12px;
        }}

        .summary-cards {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 12px;
            margin-bottom: 14px;
        }}

        .summary-card {{
            border: 1px solid #dee2e6;
            border-left: 3px solid #17a2b8;
            background: #f8f9fa;
            padding: 14px;
        }}

        .summary-card .summary-value {{
            font-size: 26px;
            font-weight: 700;
            color: #212529;
            line-height: 1.1;
        }}

        .summary-card .summary-label {{
            margin-top: 6px;
            font-size: 13px;
            color: #495057;
            font-weight: 600;
        }}

        .summary-card .summary-sub {{
            margin-top: 4px;
            color: #6c757d;
            font-size: 12px;
        }}

        .alert-note {{
            margin-top: 10px;
            border: 1px solid #f5c6cb;
            background: #fff5f6;
            color: #721c24;
            padding: 10px 12px;
            font-size: 13px;
        }}

        .alert-note.very-high {{
            border-color: #f1b0b7;
            background: #fff0f1;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <img src="{logo_data_url}" alt="Observes Logo" />
            <div class="header-content">
                <h1>Scan Report</h1>
                <div class="subtitle">{org.get('name', 'Unknown Organization')} - {org.get('type', 'Unknown Type')}</div>
            </div>
        </div>
        
        <div class="content">
            <!-- Scanner Metadata -->
            <div class="info-grid">
                <div class="info-card">
                    <div class="label">Scanner Version</div>
                    <div class="value">{result.get('scanner_version', 'N/A')}</div>
                </div>
                <div class="info-card">
                    <div class="label">Organization ID</div>
                    <div class="value">{org.get('id', 'N/A')}</div>
                </div>
                <div class="info-card">
                    <div class="label">Scan Start</div>
                    <div class="value">{scan_start_formatted}</div>
                </div>
                <div class="info-card">
                    <div class="label">Scan End</div>
                    <div class="value">{scan_end_formatted}</div>
                </div>
                <div class="info-card">
                    <div class="label">Duration</div>
                    <div class="value">{duration_str}</div>
                </div>
                <div class="info-card">
                    <div class="label">Total Projects</div>
                    <div class="value">{resource_counts.get('projects', 0)}</div>
                </div>
                <div class="info-card">
                    <div class="label">Scan Type</div>
                    <div class="value">{"Partial" if org.get('partial_scan', False) else "Full"}</div>
                </div>
                <div class="info-card">
                    <div class="label">Regex Findings</div>
                    <div class="value">{total_findings}</div>
                </div>
            </div>
            
            <!-- Scan Configuration -->
            <div class="section">
                <h2 class="section-title">Scan Configuration</h2>
                <div class="config-list">
                    <ul>
"""
    
    for config_item in scan_config:
        # Check if this is a skipped item (starts with ⊘)
        is_skipped = config_item.startswith("\u2298")
        skipped_attr = ' data-skipped="true"' if is_skipped else ''
        # Remove the symbol from display text since CSS will add it
        display_text = config_item[2:] if is_skipped else config_item
        html += f"                            <li{skipped_attr}>{display_text}</li>\n"
    
    html += f"""                        </ul>
                    </div>
                </div>
                
                <!-- Overall Resource Counts -->
                <div class="section">
                    <h2 class="section-title">Overall Resource Summary</h2>
                    <table class="metrics">
                        <thead>
                            <tr>
                                <th>Resource Type</th>
                                <th>Count</th>
                            </tr>
                        </thead>
                        <tbody>
    """
    
    # Sort resource counts for better presentation
    sorted_resources = sorted(resource_counts.items(), key=lambda x: x[1], reverse=True)
    for key, value in sorted_resources:
        display_name = key.replace('_', ' ').title()
        html += f"                            <tr><td>{display_name}</td><td class='count'>{value:,}</td></tr>\n"
    
    html += """                        </tbody>
                    </table>
                </div>
                
                <!-- IAM & RBAC Tracker -->
                <div class="section">
                    <h2 class="section-title">PAT Tokens IAM & RBAC Tracker</h2>
                    <p style="margin-bottom: 12px; color: #6c757d;">Users, groups, and PAT tokens for the organisation</p>
                    <div class="summary-cards">
    """

    html += "                        <div class=\"summary-card\">\n"
    html += f"                            <div class=\"summary-value\">{iam_rbac['total_users']}</div>\n"
    html += "                            <div class=\"summary-label\">Users</div>\n"
    html += f"                            <div class=\"summary-sub\">({iam_rbac['aad_users']} AAD, {iam_rbac['service_users']} service)</div>\n"
    html += "                        </div>\n"

    html += "                        <div class=\"summary-card\">\n"
    html += f"                            <div class=\"summary-value\">{iam_rbac['total_groups']}</div>\n"
    html += "                            <div class=\"summary-label\">Groups</div>\n"
    html += f"                            <div class=\"summary-sub\">({iam_rbac['admin_groups']} admin)</div>\n"
    html += "                        </div>\n"

    html += "                        <div class=\"summary-card\">\n"
    html += f"                            <div class=\"summary-value\">{iam_rbac['total_pats']}</div>\n"
    html += "                            <div class=\"summary-label\">PATs</div>\n"
    html += f"                            <div class=\"summary-sub\">({iam_rbac['active_pats']} active, {iam_rbac['expired_pats']} expired)</div>\n"
    html += "                        </div>\n"

    html += "                        <div class=\"summary-card\">\n"
    html += f"                            <div class=\"summary-value\">{iam_rbac['high_privilege_users']}</div>\n"
    html += "                            <div class=\"summary-label\">High-privilege users</div>\n"
    html += (
        f"                            <div class=\"summary-sub\">"
        f"({iam_rbac['high_privileged_pats']} elevated PATs, "
        f"{iam_rbac['very_high_privileged_pats']} very high-privilege PATs)"
        "</div>"
    )
    html += "                        </div>\n"

    html += """                    </div>
    """

    if iam_rbac['high_privileged_pats'] > 0:
        html += (
            f"                    <div class=\"alert-note\">"
            f"{iam_rbac['high_privileged_pats']} active PAT token(s) with high-privilege scopes detected. "
            "These tokens grant elevated permissions such as security management, identity management, "
            "or full code access."
            "</div>\n"
        )

    if iam_rbac['very_high_privileged_pats'] > 0:
        html += (
            f"                    <div class=\"alert-note very-high\">"
            f"{iam_rbac['very_high_privileged_pats']} active PAT token(s) with very high-privilege scopes detected. "
            "These tokens grant the highest level of permissions and should be closely monitored."
            "</div>\n"
        )

    html += """                </div>
                
                <!-- Project Breakdown -->
                <div class="section">
                    <h2 class="section-title">Project Details</h2>
                    <div style="margin-bottom: 1rem;">
                        <input type="text" id="projectSearch" placeholder="Filter by project name..." 
                               style="width: 100%; padding: 0.75rem; font-size: 1rem; border: 2px solid #e2e8f0; border-radius: 8px;">
                    </div>
                    <table class="metrics" id="projectTable">
                        <thead>
                            <tr>
                                <th>Project Name</th>
                                <th>Endpoints</th>
                                <th>Variable Groups</th>
                                <th>Secure Files</th>
                                <th>Repositories</th>
                                <th>Environments</th>
                                <th>Pipelines</th>
                                <th>Builds</th>
                                <th>Commits</th>
                                <th>Committers</th>
                            </tr>
                        </thead>
                        <tbody>
    """
    
    # Generate project rows with Azure DevOps links
    org_name = org.get('name', '')
    for project_id, project_stats in stats.items():
        project_name = project_names.get(project_id, f"Project {project_id[:8]}")
        project_resources = project_stats.get("resource_counts", {})
        
        # Build Azure DevOps project URL
        project_url = f"https://dev.azure.com/{org_name}/{project_name}" if org_name else "#"
        
        endpoints = project_resources.get('endpoint', 0)
        vargroups = project_resources.get('variablegroup', 0)
        securefiles = project_resources.get('securefile', 0)
        repositories = project_resources.get('repository', 0)
        environments = project_resources.get('environment', 0)
        pipelines = project_resources.get('pipelines', 0)
        builds_count = project_resources.get('builds', 0)
        commits = project_resources.get('commits', 0)
        committers = project_resources.get('committers', 0)
        
        html += f"""                            <tr>
                                <td><a href="{project_url}" target="_blank" style="color: #667eea; text-decoration: none; font-weight: 600;">{project_name}</a></td>
                                <td class="count">{endpoints}</td>
                                <td class="count">{vargroups}</td>
                                <td class="count">{securefiles}</td>
                                <td class="count">{repositories}</td>
                                <td class="count">{environments}</td>
                                <td class="count">{pipelines}</td>
                                <td class="count">{builds_count}</td>
                                <td class="count">{commits}</td>
                                <td class="count">{committers}</td>
                            </tr>
"""
    
    html += """                        </tbody>
                    </table>
                </div>
                
                <!-- Regex Scan Findings -->
                <div class="section">
                    <h2 class="section-title">Security Findings (Regex Scan)</h2>
    """
    
    if total_findings > 0:
        # Sort findings by severity and count
        severity_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3, 'info': 4}
        sorted_findings = sorted(
            regex_findings.items(),
            key=lambda x: (
                severity_order.get(regex_pattern_info.get(x[0], {}).get('severity', 'info').lower(), 999),
                -x[1]  # Count descending
            )
        )
        
        html += """                    <table class="metrics">
                        <thead>
                            <tr>
                                <th>Category</th>
                                <th>Severity</th>
                                <th>Description</th>
                                <th>Count</th>
                            </tr>
                        </thead>
                        <tbody>
        """
        
        for category, count in sorted_findings:
            pattern_info = regex_pattern_info.get(category, {})
            description = pattern_info.get('description', 'No description available')
            severity = pattern_info.get('severity', 'info').lower()
            
            # Color code severity
            severity_colors = {
                'critical': '#e53e3e',
                'high': '#ed8936',
                'medium': '#ecc94b',
                'low': '#48bb78',
                'info': '#4299e1'
            }
            severity_color = severity_colors.get(severity, '#718096')
            
            html += f"""                            <tr>
                                <td style="font-weight: 600;">{category}</td>
                                <td><span style="color: {severity_color}; font-weight: 700; text-transform: uppercase;">{severity}</span></td>
                                <td>{description}</td>
                                <td class="count">{count}</td>
                            </tr>
"""
        
        html += """                        </tbody>
                    </table>
        """
    else:
        html += """                    <p style="padding: 2rem; text-align: center; color: #718096; background: #f7fafc; border-radius: 8px; border: 1px solid #e2e8f0;">
                        No security findings detected in this scan.
                    </p>
        """
    
    html += f"""                </div>
            </div>
            
            <div class="footer">
                <p>Generated by Observes Scanner v{result.get('scanner_version', '1.0.0')} on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
                <p style="margin-top: 0.5rem; font-size: 0.875rem;">© {datetime.now().year} Observes.io - All Rights Reserved</p>
            </div>
        </div>
        
        <script>
            // Project name filtering
            document.getElementById('projectSearch').addEventListener('keyup', function() {{
                const searchValue = this.value.toLowerCase();
                const table = document.getElementById('projectTable');
                const rows = table.getElementsByTagName('tbody')[0].getElementsByTagName('tr');
                
                for (let i = 0; i < rows.length; i++) {{
                    const projectName = rows[i].getElementsByTagName('td')[0].textContent.toLowerCase();
                    if (projectName.includes(searchValue)) {{
                        rows[i].style.display = '';
                    }} else {{
                        rows[i].style.display = 'none';
                    }}
                }}
            }});
        </script>
    </body>
    </html>
    """
    
    out_path = os.path.join(results_dir, f"scan_{job_id}_report.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    return out_path
