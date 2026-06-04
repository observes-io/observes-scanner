#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
#
# Copyright (c) 2025 Observes io LTD, Scotland, Company No. SC864704
# Licensed under PolyForm Internal Use 1.0.0, see LICENSE or https://polyformproject.org/licenses/internal-use/1.0.0
# Internal use only; additional clarifications in LICENSE-CLARIFICATIONS.md
####

import hashlib
import json
import logging
import re
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

STATIC_PATTERNS = {
  "regex": {
    "version": "2.0",
    "lastUpdated": "2026-03-01",
    "description": "CI/CD behavioral SAST rules for detecting exfiltration, secret access, privilege escalation, persistence, supply chain abuse, and obfuscation in pipeline scripts.",
    "categories": [
      {
        "name": "Network Exfiltration",
        "severity": "critical",
        "description": "Detects outbound data transfer attempts from CI runners using HTTP, SSH, raw TCP, or file transfer utilities.",
        "patterns": [
          "(?i).*\\bcurl\\b.*https?:\\/\\/.*",
          "(?i).*\\bwget\\b.*https?:\\/\\/.*",
          "(?i).*\\bcurl\\b.*-d\\s+@.*",
          "(?i).*\\bcurl\\b.*--data-binary\\s+@.*",
          "(?i).*\\bwget\\b.*--post-file=.*",
          "(?i).*\\bscp\\b.*@.*:.*",
          "(?i).*\\brsync\\b.*@.*:.*",
          "(?i).*\\bftp\\b.*",
          "(?i).*\\bnc\\b.*",
          "(?i).*\\bncat\\b.*",
          "(?i).*\\bsocat\\b.*TCP.*",
          "(?i).*\\bssh\\b\\s+\\w+@.*",
          "(?i).*\\bsshpass\\b.*ssh.*",
          "(?i).*\\btelnet\\b.*",
          "(?i).*git\\s+push\\s+https:\\/\\/.*",
          "(?i).*git\\s+push\\s+--set-upstream.*",
          "(?i).*git\\s+submodule\\s+add\\s+https:\\/\\/.*",
          "(?i).*git\\s+clone\\s+https:\\/\\/.*",
          "(?i).*git\\s+remote\\s+add\\s+.*",
          "(?i).*git\\s+rebase\\s+-i.*",
          "(?i).*git\\s+filter-branch\\s+--tree-filter.*",
          "(?i).*rsync\\s+-\\w+\\s+.*",
          "(?i).*curl\\s+.*https?:\\/\\/.*",
          "(?i).*wget\\s+https?:\\/\\/.*",
          "(?i).*ftp\\s+.*",
          "(?i).*aws\\s+s3\\s+cp\\s+.*",
          "(?i).*aws\\s+s3\\s+sync\\s+.*",
          "(?i).*gcloud\\s+storage\\s+cp\\s+.*",
          "(?i).*gcloud\\s+storage\\s+rsync\\s+.*",
          "(?i).*az\\s+storage\\s+blob\\s+upload\\s+--account-name.*",
          "(?i).*az\\s+storage\\s+blob\\s+upload-batch\\s+--source\\s+.*",
          "(?i).*nc\\s+-e\\s+\\/bin\\/bash.*",
          "(?i).*bash\\s+-i\\s+>&\\s+\\/dev\\/tcp\\/.*",
          "(?i).*perl\\s+-e\\s+\\[\\\".*\\/dev\\/tcp\\/.*\\\"\\].*",
          "(?i).*python\\s+-c\\s+\\[\\\"import\\s+socket.*socket\\.connect.*\\\"\\].*",
          "(?i).*mail\\s+-s\\s+\\\".*\\\"\\s+.*@.*",
          "(?i).*curl\\s+.*-F\\s+.*@.*",
          "(?i).*httpie\\s+post\\s+.*",
          "(?i).*powershell\\s+-Command\\s+Invoke-WebRequest.*",
          "(?i).*powershell\\s+-Command\\s+Invoke-RestMethod.*",
          "(?i).*python\\s+-m\\s+http.server.*"
        ]
      },
      {
        "name": "Cloud Metadata Access",
        "severity": "high",
        "description": "Detects attempts to retrieve cloud instance credentials via metadata services.",
        "patterns": [
          "(?i).*169\\.254\\.169\\.254.*",
          "(?i).*metadata\\.google\\.internal.*",
          "(?i).*Invoke-WebRequest\\s+.*169\\.254\\.169\\.254.*",
          "(?i).*curl\\s+.*169\\.254\\.169\\.254.*",
          "(?i).*wget\\s+.*169\\.254\\.169\\.254.*",
          "(?i).*curl\\s+.*metadata\\.google\\.internal.*"
        ]
      },
      {
        "name": "Secret Enumeration & Dumping",
        "severity": "critical",
        "description": "Detects attempts to enumerate environment variables, credentials, or secret files within CI environments.",
        "patterns": [
          "(?i).*\\bprintenv\\b.*",
          "(?i).*\\benv\\b\\s*$",
          "(?i).*\\bset\\b\\s*$",
          "(?i).*\\/proc\\/self\\/environ.*",
          "(?i).*~\\/\\.aws\\/credentials.*",
          "(?i).*~\\/\\.ssh.*",
          "(?i).*~\\/\\.kube.*",
          "(?i).*~\\/\\.azure.*",
          "(?i).*~\\/\\.terraform.*",
          "(?i).*~\\/\\.docker.*",
          "(?i).*\\/etc\\/passwd.*",
          "(?i).*grep\\s+-r\\s+.*(_KEY|_SECRET|_TOKEN|_PASSWORD).*",
          "(?i).*echo\\s+\\$\\{?(CI_|GITHUB_|AWS_|AZURE_|GOOGLE_).*",
          "(?i).*cat\\s+\\/etc\\/passwd.*",
          "(?i).*cat\\s+~\\/\\.aws\\/credentials.*",
          "(?i).*find\\s+\\/.*\\s+-name\\s+.*\\.(pem|key|crt|env|cfg).*",
          "(?i).*grep\\s+-r\\s+\\\"\\w+(_KEY|_SECRET|_TOKEN|_PASSWORD).*",
          "(?i).*echo\\s+.*(KEY|SECRET|TOKEN|PASSWORD).*",
          "(?i).*ls\\s+-la\\s+~\\/\\.ssh.*",
          "(?i).*ls\\s+-la\\s+~\\/\\.kube.*",
          "(?i).*ls\\s+-la\\s+\\/root\\/\\.kube.*",
          "(?i).*ls\\s+-la\\s+~\\/\\.azure.*",
          "(?i).*ls\\s+-la\\s+~\\/\\.aws.*",
          "(?i).*ls\\s+-la\\s+~\\/\\.gcp.*",
          "(?i).*ls\\s+-la\\s+~\\/\\.terraform.*",
          "(?i).*ls\\s+-la\\s+~\\/\\.docker.*",
          "(?i).*ls\\s+-la\\s+~\\/\\.git.*",
          "(?i).*ls\\s+-la\\s+~\\/\\.bash_history.*",
          "(?i).*ls\\s+-la\\s+~\\/\\.bashrc.*",
          "(?i).*ls\\s+-la\\s+~\\/\\.zsh_history.*",
          "(?i).*ls\\s+-la\\s+~\\/\\.zshrc.*",
          "(?i).*ls\\s+-la\\s+~\\/\\.bash_profile.*",
          "(?i).*ls\\s+-la\\s+~\\/\\.profile.*",
          "(?i).*declare\\s+-x.*",
          "(?i).*compgen\\s+-v.*",
          "(?i).*strings\\s+\\/proc\\/\\d+\\/environ.*",
          "(?i).*export\\s+\\w+_KEY=.*",
          "(?i).*docker\\s+cp\\s+\\w+:\\/etc\\/passwd.*",
          "(?i).*kubectl\\s+cp\\s+\\w+:\\/etc\\/passwd.*"
        ]
      },
      {
        "name": "Secrets Manager Access",
        "severity": "critical",
        "description": "Detects direct retrieval of secrets from cloud secret managers during pipeline execution.",
        "patterns": [
          "(?i).*\\baws\\s+secretsmanager\\s+get-secret-value\\b.*",
          "(?i).*\\bgcloud\\s+secrets\\s+versions\\s+access\\b.*",
          "(?i).*\\baz\\s+keyvault\\s+secret\\s+download\\b.*"
        ]
      },
      {
        "name": "CI/CD Platform Manipulation",
        "severity": "high",
        "description": "Detects modification of CI runtime variables or pipeline configuration files for persistence or lateral movement.",
        "patterns": [
          "(?i).*>>\\s+\\$GITHUB_ENV.*",
          "(?i).*>>\\s+\\$GITHUB_OUTPUT.*",
          "(?i).*>>\\s+\\$GITHUB_PATH.*",
          "(?i).*\\$CI_JOB_TOKEN.*",
          "(?i).*>>\\s+\\.gitlab-ci\\.yml.*",
          "(?i).*withCredentials\\(.*\\).*",
          "(?i).*archiveArtifacts.*",
          "(?i).*cat\\s+\\$GITHUB_EVENT_PATH.*",
          "(?i).*actions\\/checkout@.*",
          "(?i).*uses:\\s+.*@main.*",
          "(?i).*uses:\\s+.*@master.*",
          "(?i).*sh\\s+\\\"curl.*\\$.*\\\".*"
        ]
      },
      {
        "name": "Privilege Escalation & Container Escape",
        "severity": "critical",
        "description": "Detects attempts to escalate privileges, escape containers, or mount sensitive host resources.",
        "patterns": [
          "(?i).*\\bsudo\\b.*",
          "(?i).*chmod\\s+\\+s\\s+.*",
          "(?i).*\\bsetcap\\b.*",
          "(?i).*\\bunshare\\b.*",
          "(?i).*\\bnsenter\\b.*",
          "(?i).*docker\\s+run\\s+.*--privileged.*",
          "(?i).*docker\\s+run\\s+.*--cap-add.*",
          "(?i).*docker\\s+run\\s+.*-v\\s+\\/:\\/host.*",
          "(?i).*docker\\s+run\\s+.*--network=host.*",
          "(?i).*kubectl\\s+exec\\s+-it.*",
          "(?i).*kubectl\\s+cp\\s+.*",
          "(?i).*docker\\s+run\\s+-d\\s+--privileged\\s+.*",
          "(?i).*docker\\s+run\\s+-v\\s+\\/var\\/run\\/docker.sock:\\/var\\/run\\/docker.sock.*",
          "(?i).*kubectl\\s+exec\\s+-it\\s+\\w+\\s+--\\s+\\/bin\\/sh.*",
          "(?i).*kubectl\\s+exec\\s+-it\\s+\\w+\\s+--\\s+.*",
          "(?i).*kubectl\\s+logs\\s+.*",
          "(?i).*kubectl\\s+apply\\s+-f\\s+http.*",
          "(?i).*kubectl\\s+create\\s+secret.*",
          "(?i).*mount\\s+.*",
          "(?i).*capsh\\s+.*"
        ]
      },
      {
        "name": "Persistence Mechanisms",
        "severity": "high",
        "description": "Detects attempts to establish persistence on CI runners via cron jobs, system services, or background execution.",
        "patterns": [
          "(?i).*crontab\\s+-e.*",
          "(?i).*>>\\s+\\/etc\\/crontab.*",
          "(?i).*systemctl\\s+enable.*",
          "(?i).*systemctl\\s+start.*",
          "(?i).*nohup\\s+.*&.*",
          "(?i).*screen\\s+-dmS.*",
          "(?i).*tmux\\s+new-session\\s+-d.*",
          "(?i).*useradd\\s+.*",
          "(?i).*adduser\\s+.*",
          "(?i).*ssh\\s+\\w+@.*",
          "(?i).*scp\\s+.*",
          "(?i).*nc\\s+.*"
        ]
      },
      {
        "name": "Obfuscation & Encoded Execution",
        "severity": "critical",
        "description": "Detects encoded payload decoding, inline execution, and shell pipe execution commonly used to evade detection.",
        "patterns": [
          "(?i).*base64\\s+-d.*",
          "(?i).*base64\\s+--decode.*",
          "(?i).*echo\\s+.*\\|\\s+base64\\s+-d.*",
          "(?i).*curl\\s+.*\\|\\s+(bash|sh).*",
          "(?i).*wget\\s+.*\\|\\s+(bash|sh).*",
          "(?i).*echo\\s+.*\\|\\s+(bash|sh).*",
          "(?i).*printf\\s+.*\\|\\s+sh.*",
          "(?i).*eval\\s+\\$.*",
          "(?i).*\\$\\(curl\\s+.*\\).*",
          "(?i).*\\$\\(wget\\s+.*\\).*",
          "(?i).*python\\s+-c\\s+\"exec\\(.*\\)\".*",
          "(?i).*bash\\s+-c\\s+\\[\\\".*\\\"\\].*",
          "(?i).*sh\\s+-c\\s+\\[\\\".*\\\"\\].*",
          "(?i).*python\\s+-c\\s+\\[\\\".*\\\"\\].*",
          "(?i).*perl\\s+-e\\s+\\[\\\".*\\\"\\].*",
          "(?i).*php\\s+-r\\s+\\[\\\".*\\\"\\].*",
          "(?i).*openssl\\s+base64\\s+-e\\s+-in\\s+.*",
          "(?i).*base64\\s+.*",
          "(?i).*openssl\\s+enc\\s+-aes-\\w+\\s+-in\\s+.*\\s+-out\\s+.*",
          "(?i).*tr\\s+-d\\s+\\\"\\s+.*",
          "(?i).*sed\\s+-e\\s+.*",
          "(?i).*xxd\\s+-r\\s+.*",
          "(?i).*\\$\\(base64\\s+-d.*\\).*"
        ]
      },
      {
        "name": "Supply Chain Manipulation",
        "severity": "high",
        "description": "Detects package publishing, alternate registry usage, or artifact push operations that may indicate supply chain compromise.",
        "patterns": [
          "(?i).*npm\\s+publish.*",
          "(?i).*npm\\s+config\\s+set\\s+.*_authToken.*",
          "(?i).*pip\\s+install\\s+--index-url.*",
          "(?i).*pip\\s+install\\s+--extra-index-url.*",
          "(?i).*gem\\s+push.*",
          "(?i).*twine\\s+upload.*",
          "(?i).*docker\\s+login.*",
          "(?i).*helm\\s+repo\\s+add\\s+http.*",
          "(?i).*terraform\\s+init\\s+-backend-config=.*"
        ]
      },
      {
        "name": "Destructive Actions",
        "severity": "high",
        "description": "Detects potentially destructive system actions affecting security agents or system integrity.",
        "patterns": [
          "(?i).*rm\\s+-rf\\s+\\/opt\\/security-agent.*",
          "(?i).*kill\\s+-9\\s+\\$?\\(pgrep.*\\).*",
          "(?i).*dd\\s+if=\\/dev\\/.*\\s+of=.*",
          "(?i).*chmod\\s+777\\s+.*",
          "(?i).*chown\\s+root\\s+.*"
        ]
      },
      {
        "name": "Shell Evasion & Command Shadowing",
        "severity": "high",
        "description": "Detects alias creation, function wrapping, builtin shadowing, PATH manipulation, command indirection, dynamic sourcing, and other shell-level evasion techniques used to bypass static CI/CD detection rules.",
        "patterns": [
          "(?i).*\\balias\\s+\\w+\\s*=.*",
          "(?i).*\\bunalias\\b.*",
          "(?i).*\\balias\\s+\\w+\\s*=\\s*['\"].*(curl|wget|nc|ssh|scp|bash|sh|git|aws).*",
          "(?i).*\\balias\\s+(curl|wget|git|ssh|nc|bash|sh)\\s*=.*",
          "(?i).*\\bfunction\\s+\\w+\\s*\\(\\)\\s*\\{.*",
          "(?i).*\\b\\w+\\s*\\(\\)\\s*\\{.*",
          "(?i).*(curl|wget|git|ssh|nc|bash|sh)\\s*\\(\\)\\s*\\{.*",
          "(?i).*\\bdeclare\\s+-f\\b.*",
          "(?i).*\\btypeset\\s+-f\\b.*",
          "(?i).*(echo|printf|test|command|source)\\s*\\(\\)\\s*\\{.*",
          "(?i).*\\bexport\\s+PATH=.*",
          "(?i).*\\bPATH=.*\\$PATH.*",
          "(?i).*\\bsetenv\\s+PATH.*",
          "(?i).*\\becho\\s+.*>>\\s+\\/etc\\/profile.*",
          "(?i).*\\becho\\s+.*>>\\s+~\\/\\.bashrc.*",
          "(?i).*\\/tmp\\/.*(git|curl|ssh|bash).*",
          "(?i).*chmod\\s+\\+x\\s+.*\\/tmp\\/.*",
          "(?i).*\\bcommand\\s+(curl|wget|git|ssh|nc|bash|sh).*",
          "(?i).*\\bbuiltin\\b.*",
          "(?i).*\\\\(curl|wget|git|ssh|nc|bash|sh).*",
          "(?i).*\\$\\w+\\s+https?:\\/\\/.*",
          "(?i).*\\b(eval|exec)\\b.*\\$.*",
          "(?i).*\\$\\(echo\\s+.*\\|\\s+base64\\s+-d.*\\).*",
          "(?i).*\\bdeclare\\s+\\w+=.*",
          "(?i).*\\bexport\\s+\\w+=.*(curl|wget|nc|bash|sh).*",
          "(?i).*source\\s+<\\(.*",
          "(?i).*\\.\\s+<\\(.*",
          "(?i).*<\\(curl.*",
          "(?i).*<\\(wget.*",
          "(?i).*<<\\s*EOF.*",
          "(?i).*cat\\s+<<.*"
        ]
      }
    ]
  }
}

class SastService:
    """
    CICD SAST scanning service that produces findings associated with PIR snapshots.

    Each unique PIR snapshot is scanned at most once.  Findings are returned as
    structured objects with a unique finding ID, referencing the snapshot they
    belong to.

    The service attempts to obtain patterns from an API first and falls back to
    the locally-stored pattern file when the API is unavailable.
    """

    def __init__(self, api_client=None, exceptions: list = None, skip=False):

        self._skip = skip
        self._api_client = api_client
        self._exceptions = exceptions or []
        self._compiled_patterns: Dict[str, list] = {}
        self._patterns_loaded = False
        self._findings: List[Dict] = []
        self._scanned_snapshots: set = set()

    # ------------------------------------------------------------------
    # Pattern loading
    # ------------------------------------------------------------------

    def _load_patterns(self):
        """Load SAST patterns from API (preferred) or local fallback."""
        if self._patterns_loaded:
            return

        patterns_data = None

        # Try API first
        if self._api_client is not None:
            try:
                patterns_data = self._api_client.get_sast_patterns()
                if patterns_data:
                    logger.info("SAST patterns loaded from API")
            except Exception as e:
                logger.warning(f"Failed to fetch SAST patterns from API, falling back to local: {e}")

        # Fallback to local file
        if not patterns_data:
            try:
                patterns_data = STATIC_PATTERNS
                logger.info("SAST patterns loaded from local file")
            except FileNotFoundError:
                logger.error("SAST patterns file not found: datastore/scanners/patterns/cicd_sast.json")
            except json.JSONDecodeError:
                logger.error("Failed to parse SAST patterns file as JSON")
            except Exception as e:
                logger.error(f"Error loading SAST patterns: {e}")

        if not patterns_data:
            self._patterns_loaded = True
            return

        # Compile regex patterns by engine
        for engine_name, engine_data in patterns_data.items():
            compiled = []
            for category in engine_data.get("categories", []):
                category_name = category.get("name", "Unknown")
                category_severity = category.get("severity", "unknown")
                category_description = category.get("description", "")
                for pattern in category.get("patterns", []):
                    try:
                        compiled.append({
                            "pattern": re.compile(pattern),
                            "category": category_name,
                            "severity": category_severity,
                            "description": category_description,
                        })
                    except re.error as e:
                        logger.warning(f"Invalid regex pattern for engine {engine_name}, category {category_name}: {e}")
            self._compiled_patterns[engine_name] = compiled

        self._patterns_loaded = True


    # ------------------------------------------------------------------
    # Scanning
    # ------------------------------------------------------------------

    def _generate_finding_id(self, snapshot_id: str, engine: str, pattern: str, start: int, end: int) -> str:
        """Deterministic finding ID from snapshot + match details."""
        raw = f"{snapshot_id}|{engine}|{pattern}|{start}|{end}"
        return f"sast:{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:16]}"

    def _scan_content(self, content: str, snapshot_id: str, engine: str) -> List[Dict]:
        """Scan a string against compiled patterns for the given engine."""
        self._load_patterns()

        compiled = self._compiled_patterns.get(engine, [])
        if not compiled:
            logger.debug(f"No SAST patterns for engine '{engine}'")
            return []

        findings = []
        for pattern_info in compiled:
            compiled_pattern = pattern_info["pattern"]
            for match in compiled_pattern.finditer(content):
                # Apply exceptions
                match_text = match.group()
                if any(exc in match_text for exc in self._exceptions if isinstance(match_text, str)):
                    logger.debug(f"Skipping match '{match_text}' due to exception")
                    continue

                finding_id = self._generate_finding_id(
                    snapshot_id, engine, compiled_pattern.pattern, match.start(), match.end()
                )
                findings.append({
                    "findingId": finding_id,
                    "snapshotId": snapshot_id,
                    "engine": engine,
                    "category": pattern_info["category"],
                    "severity": pattern_info["severity"],
                    "description": pattern_info["description"],
                    "match": match_text,
                    "start": match.start(),
                    "end": match.end(),
                    "pattern": compiled_pattern.pattern,
                })
        
        return findings

    def scan_pir_snapshots(self, pir_registry, engine: str = "regex") -> List[Dict]:
        """
        Scan all PIR snapshots that have stored YAML content.

        Each snapshot is scanned exactly once.  Results are accumulated in
        ``self.findings`` and also returned.

        Args:
            pir_registry: PIRRegistry instance with registered snapshots.
            engine: Pattern engine to use (default ``"regex"``).

        Returns:
            List of all SAST finding dicts produced by this call.
        """
        new_findings = []
        for snapshot_id in list(pir_registry.entries.keys()):
            if snapshot_id in self._scanned_snapshots:
                continue

            yaml_content = pir_registry.get_yaml_content(snapshot_id)
            if not yaml_content:
                logger.debug(f"No YAML content stored for snapshot {snapshot_id}, skipping SAST scan")
                self._scanned_snapshots.add(snapshot_id)
                continue

            findings = self._scan_content(yaml_content, snapshot_id, engine)
            new_findings.extend(findings)
            self._scanned_snapshots.add(snapshot_id)
            if findings:
                logger.debug(f"SAST scan for {snapshot_id}: {len(findings)} findings")

        self._findings.extend(new_findings)
        logger.info(f"SAST scan complete: {len(new_findings)} new findings across "
                    f"{len(self._scanned_snapshots)} snapshots ({len(self._findings)} total)")
        return new_findings

    @property
    def findings(self) -> List[Dict]:
        """All accumulated SAST findings."""
        return list(self._findings)

    def summary(self) -> Dict:
        """Stats for logging / scan metadata."""
        by_severity = {}
        by_category = {}
        for f in self._findings:
            sev = f["severity"]
            cat = f["category"]
            by_severity[sev] = by_severity.get(sev, 0) + 1
            by_category[cat] = by_category.get(cat, 0) + 1
        return {
            "total_findings": len(self._findings),
            "snapshots_scanned": len(self._scanned_snapshots),
            "by_severity": by_severity,
            "by_category": by_category,
        }
