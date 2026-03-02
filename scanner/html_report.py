# HTML Report Generator for Scan Results
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0

import os
import json
from datetime import datetime
from typing import Any, Dict, Optional
from collections import defaultdict

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
                                <th>Project ID</th>
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
                                <td style="font-size: 0.85rem; color: #718096;">{project_id[:8]}...</td>
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
