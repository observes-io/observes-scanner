#### Copyright Notice
# SPDX-FileCopyrightText: 2025 Observes io LTD
# SPDX-License-Identifier: LicenseRef-PolyForm-Internal-Use-1.0.0
####

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class NodeKind(str, Enum):
    PIPELINE = "pipeline"
    TEMPLATE = "template"
    STAGE = "stage"
    JOB = "job"
    STEP = "step"
    RUN = "run"
    LOG = "log"
    EVALUATION = "evaluation"
    RESOURCE = "resource"
    DECORATOR = "decorator"


class EdgeType(str, Enum):
    CONTAINS = "contains"
    USES_TEMPLATE = "uses_template"
    FROM_TEMPLATE = "from_template"
    DEPENDS_ON = "depends_on"
    EXECUTED_IN = "executed_in"
    HAS_LOG = "has_log"
    EVALUATED = "evaluated"
    USES_RESOURCE = "uses_resource"


class LogEventType(str, Enum):
    EVALUATION = "evaluation"
    STEP = "step"
    DECORATOR = "decorator"
    SYSTEM = "system"
    CONDITION_SKIP = "condition_skip"
    CHECKOUT = "checkout"
    TASK_DOWNLOAD = "task_download"


@dataclass(frozen=True)
class TemplateKey:
    """Content-addressed template identifier for deduplication."""
    project: str
    repo: str
    path: str
    commit: str
    ref: str = "refs/heads/main"

    @property
    def id(self) -> str:
        return f"template:{self.project}:{self.repo}:{self.commit}:{self.path}"


@dataclass
class EvaluationTrace:
    """Single expression evaluation from /2 log."""
    expression: str
    expanded: str
    result: Any


@dataclass
class TemplateFileInfo:
    """Info about a template file discovered during evaluation."""
    path: str
    repo_alias: Optional[str]  # e.g. "sharedTemplates" for cross-repo
    load_type: str  # "stepsTemplateReference", "jobsTemplateReference", etc.
    evaluations: list[EvaluationTrace] = field(default_factory=list)
    parameters: dict[str, Any] = field(default_factory=dict)
    step_count: int = 0
    nested: list["TemplateFileInfo"] = field(default_factory=list)


@dataclass
class TemplateEvaluationSummary:
    """Summary of all template evaluations from /2 log."""
    root_template: str = ""
    templates_used: list[TemplateFileInfo] = field(default_factory=list)
    file_count: int = 0
    max_depth: int = 0
    load_time: str = ""
    memory_estimate: str = ""
    evaluations: list[EvaluationTrace] = field(default_factory=list)


@dataclass
class StepExecutionInfo:
    """Execution info for a single step from /(last) log."""
    name: str
    task_name: Optional[str] = None
    task_version: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    script_contents: Optional[str] = None
    is_decorator: bool = False
    was_skipped: bool = False
    skip_reason: Optional[str] = None
    condition_expression: Optional[str] = None
    condition_result: Optional[bool] = None
    checkout_repo: Optional[str] = None
    checkout_commit: Optional[str] = None
    output_lines: list[str] = field(default_factory=list)
