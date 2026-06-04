#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
####

import logging
import re
from typing import Any, Optional

import yaml

from .model import (
    EvaluationTrace,
    LogEventType,
    StepExecutionInfo,
    TemplateEvaluationSummary,
    TemplateFileInfo,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# /1 log parser: Expanded pipeline YAML
# ---------------------------------------------------------------------------

class ExpansionLogParser:
    """Parse the /1 build log which contains the fully-expanded pipeline YAML."""

    def parse(self, log_content: str) -> Optional[dict]:
        """Return parsed YAML dict from the expansion log, or None."""
        if not log_content:
            return None
        try:
            return yaml.safe_load(log_content)
        except yaml.YAMLError as e:
            logger.warning(f"Failed to parse expansion log YAML: {e}")
            return None


# ---------------------------------------------------------------------------
# /2 log parser: Template evaluation trace
# ---------------------------------------------------------------------------

_RE_BEGIN_EVAL = re.compile(r"^Begin evaluating template '(.+)'$")
_RE_FINISH_EVAL = re.compile(r"^Finished evaluating template '(.+)'$")
_RE_BEGIN_LOAD = re.compile(r"^Begin load: (\w+)$")
_RE_END_LOAD = re.compile(r"^End load: (\w+)$")
_RE_BEGIN_TRANSFORM = re.compile(r"^Begin transform: (\w+)$")
_RE_END_TRANSFORM = re.compile(r"^End transform: (\w+)$")
_RE_EVALUATING = re.compile(r"^Evaluating: (.+)$")
_RE_EXPANDED = re.compile(r"^Expanded: (.+)$")
_RE_RESULT = re.compile(r"^Result: (.+)$")
_RE_PARAM = re.compile(r"^Evaluating: parameters\['(.+)'\]$")
_RE_FILE_COUNT = re.compile(r"^File Count: (\d+)")
_RE_DEPTH = re.compile(r"^Greatest Parser Depth: (\d+)")
_RE_LOAD_TIME = re.compile(r"^Load Time: (.+)$")
_RE_MEMORY = re.compile(r"^Estimated Memory: (.+)$")


class EvaluationLogParser:
    """
    Parse the /2 build log (template evaluation trace).

    Produces a TemplateEvaluationSummary with:
    - Hierarchical template usage tree
    - Expression evaluations per template
    - Parameter bindings
    - File/depth/memory stats
    """

    def parse(self, log_content: str) -> TemplateEvaluationSummary:
        summary = TemplateEvaluationSummary()
        if not log_content:
            return summary

        # State machine
        template_stack: list[TemplateFileInfo] = []
        current_load_type: Optional[str] = None
        current_eval: dict[str, Any] = {}  # partial evaluation being built
        current_step_evals: list[EvaluationTrace] = []
        in_transform: Optional[str] = None  # "step" or "stepsTemplate" etc.
        all_templates: list[TemplateFileInfo] = []

        for raw_line in log_content.splitlines():
            line = raw_line.strip()
            if not line:
                continue

            # -- Template evaluation boundaries --
            m = _RE_BEGIN_EVAL.match(line)
            if m:
                path_raw = m.group(1)
                path, alias = self._split_template_path(path_raw)
                tpl = TemplateFileInfo(
                    path=path,
                    repo_alias=alias,
                    load_type=current_load_type or "root",
                )
                if template_stack:
                    template_stack[-1].nested.append(tpl)
                template_stack.append(tpl)
                all_templates.append(tpl)
                if not summary.root_template:
                    summary.root_template = path_raw
                continue

            m = _RE_FINISH_EVAL.match(line)
            if m:
                if template_stack:
                    template_stack.pop()
                continue

            # -- Template load type --
            m = _RE_BEGIN_LOAD.match(line)
            if m:
                current_load_type = m.group(1)
                continue

            m = _RE_END_LOAD.match(line)
            if m:
                current_load_type = None
                continue

            # -- Step / template transforms --
            m = _RE_BEGIN_TRANSFORM.match(line)
            if m:
                in_transform = m.group(1)
                current_step_evals = []
                continue

            m = _RE_END_TRANSFORM.match(line)
            if m:
                if in_transform == "step" and template_stack:
                    template_stack[-1].step_count += 1
                    template_stack[-1].evaluations.extend(current_step_evals)
                in_transform = None
                current_step_evals = []
                continue

            # -- Expression evaluations (3-line sequence: Evaluating/Expanded/Result) --
            m = _RE_EVALUATING.match(line)
            if m:
                current_eval = {"expression": m.group(1)}
                # Check if it's a parameter reference
                pm = _RE_PARAM.match(line)
                if pm and template_stack:
                    # Parameter name will be captured when Result line comes
                    current_eval["_param_name"] = pm.group(1)
                continue

            m = _RE_EXPANDED.match(line)
            if m:
                current_eval["expanded"] = m.group(1)
                continue

            m = _RE_RESULT.match(line)
            if m:
                result_str = m.group(1)
                result_val = self._parse_result_value(result_str)
                if "expression" in current_eval:
                    trace = EvaluationTrace(
                        expression=current_eval.get("expression", ""),
                        expanded=current_eval.get("expanded", result_str),
                        result=result_val,
                    )
                    current_step_evals.append(trace)
                    summary.evaluations.append(trace)

                    # Store parameter binding
                    param_name = current_eval.get("_param_name")
                    if param_name and template_stack:
                        template_stack[-1].parameters[param_name] = result_val
                current_eval = {}
                continue

            # -- Stats --
            m = _RE_FILE_COUNT.match(line)
            if m:
                summary.file_count = int(m.group(1))
                continue
            m = _RE_DEPTH.match(line)
            if m:
                summary.max_depth = int(m.group(1))
                continue
            m = _RE_LOAD_TIME.match(line)
            if m:
                summary.load_time = m.group(1)
                continue
            m = _RE_MEMORY.match(line)
            if m:
                summary.memory_estimate = m.group(1)
                continue

        summary.templates_used = all_templates
        return summary

    @staticmethod
    def _split_template_path(path_raw: str) -> tuple[str, Optional[str]]:
        """Split '/path.yml@alias' into (path, alias)."""
        if "@" in path_raw:
            parts = path_raw.split("@", 1)
            return parts[0], parts[1]
        return path_raw, None

    @staticmethod
    def _parse_result_value(result_str: str) -> Any:
        s = result_str.strip().strip("'\"")
        if s == "True" or s == "true":
            return True
        if s == "False" or s == "false":
            return False
        if s == "Null" or s == "null":
            return None
        if s == "Object":
            return "__object__"
        try:
            return int(s)
        except ValueError:
            pass
        return s


# ---------------------------------------------------------------------------
# /(last) log parser: Execution / runtime log
# ---------------------------------------------------------------------------

_RE_SECTION_START = re.compile(
    r"^\d{4}-\d{2}-\d{2}T[\d:.]+Z ##\[section\]Starting: (.+)$"
)
_RE_SECTION_FINISH = re.compile(
    r"^\d{4}-\d{2}-\d{2}T[\d:.]+Z ##\[section\]Finishing: (.+)$"
)
_RE_TIMESTAMP = re.compile(r"^(\d{4}-\d{2}-\d{2}T[\d:.]+Z) (.*)$")
_RE_TASK_META = re.compile(r"^Task\s+: (.+)$")
_RE_TASK_VERSION = re.compile(r"^Version\s+: (.+)$")
_RE_SCRIPT_CONTENTS = re.compile(r"^Script contents:$")
_RE_SKIP_CONDITION = re.compile(r"^Skipping step due to condition evaluation\.")
_RE_EVAL_CONDITION = re.compile(r"^Evaluating: (.+)$")
_RE_EXPANDED_CONDITION = re.compile(r"^Expanded: (.+)$")
_RE_RESULT_CONDITION = re.compile(r"^Result: (.+)$")
_RE_DOWNLOADING_TASK = re.compile(r"^Downloading task: (.+) \((.+)\)$")
_RE_GIT_CHECKOUT = re.compile(r"^Syncing repository: (.+) \(")
_RE_HEAD_COMMIT = re.compile(r"^HEAD is now at ([0-9a-f]+)")
_RE_DECORATOR_MARKER = re.compile(r"Decorator", re.IGNORECASE)


class ExecutionLogParser:
    """
    Parse the /(last) build log for execution details.

    Extracts:
    - Step execution boundaries / timing
    - Task metadata (name, version)
    - Script contents
    - Condition evaluations / skips
    - Decorator steps
    - Repository checkouts
    - Downloaded tasks
    """

    def parse(self, log_content: str) -> dict:
        """
        Returns dict with:
            steps: list[StepExecutionInfo]
            downloaded_tasks: list[dict]  # {name, version}
            decorators: list[str]
        """
        result = {
            "steps": [],
            "downloaded_tasks": [],
            "decorators": [],
        }
        if not log_content:
            return result

        current_step: Optional[StepExecutionInfo] = None
        collecting_script = False
        script_lines: list[str] = []
        skip_next_condition: dict[str, Any] = {}

        for raw_line in log_content.splitlines():
            # Extract timestamp
            ts_match = _RE_TIMESTAMP.match(raw_line)
            timestamp = ts_match.group(1) if ts_match else None
            line = ts_match.group(2).strip() if ts_match else raw_line.strip()

            # -- Section boundaries --
            m = _RE_SECTION_START.match(raw_line)
            if m:
                step_name = m.group(1)
                if step_name in ("Job", "Initialize job", "Finalize Job"):
                    continue
                if current_step and collecting_script:
                    current_step.script_contents = "\n".join(script_lines)
                    collecting_script = False
                    script_lines = []

                current_step = StepExecutionInfo(
                    name=step_name,
                    start_time=timestamp,
                    is_decorator=bool(_RE_DECORATOR_MARKER.search(step_name)),
                )
                if current_step.is_decorator:
                    result["decorators"].append(step_name)
                continue

            m = _RE_SECTION_FINISH.match(raw_line)
            if m:
                if current_step:
                    if collecting_script:
                        current_step.script_contents = "\n".join(script_lines)
                        collecting_script = False
                        script_lines = []
                    current_step.end_time = timestamp
                    result["steps"].append(current_step)
                    current_step = None
                continue

            # -- Skipped step (appears outside a section) --
            if _RE_SKIP_CONDITION.match(line):
                skip_next_condition = {"skipped": True}
                continue

            if skip_next_condition.get("skipped"):
                em = _RE_EVAL_CONDITION.match(line)
                if em:
                    skip_next_condition["expression"] = em.group(1)
                    continue
                xm = _RE_EXPANDED_CONDITION.match(line)
                if xm:
                    skip_next_condition["expanded"] = xm.group(1)
                    continue
                rm = _RE_RESULT_CONDITION.match(line)
                if rm:
                    skip_info = StepExecutionInfo(
                        name=skip_next_condition.get("_name", "Skipped Step"),
                        was_skipped=True,
                        skip_reason="condition",
                        condition_expression=skip_next_condition.get("expression"),
                        condition_result=rm.group(1).strip() == "True",
                    )
                    result["steps"].append(skip_info)
                    skip_next_condition = {}
                    continue

            if not current_step:
                # Check for downloaded tasks
                dm = _RE_DOWNLOADING_TASK.match(line)
                if dm:
                    result["downloaded_tasks"].append({
                        "name": dm.group(1),
                        "version": dm.group(2),
                    })
                continue

            # -- Inside a step section --
            # Task metadata
            tm = _RE_TASK_META.match(line)
            if tm:
                current_step.task_name = tm.group(1)
                continue
            vm = _RE_TASK_VERSION.match(line)
            if vm:
                current_step.task_version = vm.group(1)
                continue

            # Script contents
            if _RE_SCRIPT_CONTENTS.match(line):
                collecting_script = True
                script_lines = []
                continue
            if collecting_script:
                if line.startswith("=") and "Starting Command Output" in line:
                    # End of script contents block
                    current_step.script_contents = "\n".join(script_lines)
                    collecting_script = False
                    script_lines = []
                else:
                    script_lines.append(line)
                continue

            # Repository checkout
            gm = _RE_GIT_CHECKOUT.match(line)
            if gm:
                current_step.checkout_repo = gm.group(1)
                continue
            hm = _RE_HEAD_COMMIT.match(line)
            if hm and current_step.checkout_repo:
                current_step.checkout_commit = hm.group(1)
                continue

            # Collect output lines
            if current_step and not line.startswith("##["):
                current_step.output_lines.append(line)

        return result
