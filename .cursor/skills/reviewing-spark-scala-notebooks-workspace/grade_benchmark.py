#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path


EXPECTED_TABLES = [
    "ai_monitor_volume_mensal",
    "ai_monitor_por_autor",
    "ai_monitor_tempo_merge",
    "ai_monitor_lista_prs",
    "ai_monitor_outputs_por_pr",
    "ai_monitor_total_acumulado",
    "ai_monitor_pr_content",
]

REQUIRED_SECTIONS = [
    "## 1. Imports",
    "## 2. Data sources",
    "## 3. Widgets",
    "## 4. Variable filters",
    "## 5. Functions",
    "## 6. Code / analysis",
    "## 7. Validation",
    "## 8. Display",
]


def read_text(path: Path) -> str:
    return path.read_text(errors="replace")


def extract_intro(notebook_text: str) -> str:
    match = re.search(r"// MAGIC # .*?(?=// MAGIC ## 1\. Imports)", notebook_text, re.S)
    return match.group(0) if match else notebook_text[:2000]


def extract_validation(notebook_text: str) -> str:
    match = re.search(
        r"// MAGIC ## 7\. Validation([\s\S]*?)(?=// MAGIC ## 8\. Display|$)",
        notebook_text,
        re.I,
    )
    return match.group(1) if match else notebook_text


def regex_any(text: str, patterns: list[str]) -> bool:
    return any(re.search(pattern, text, re.I | re.S) for pattern in patterns)


def parse_risks(notes_text: str) -> list[str]:
    lines = notes_text.splitlines()
    capture = False
    risks: list[str] = []
    for line in lines:
        if line.strip().startswith("## Riscos"):
            capture = True
            continue
        if capture and line.strip().startswith("## "):
            break
        if capture and line.strip().startswith("- "):
            risks.append(line.strip()[2:])
    return risks


def grade_expectations(
    eval_name: str,
    notebook_text: str,
    notes_text: str,
    expectations: list[str],
) -> list[dict]:
    intro = extract_intro(notebook_text)
    validation = extract_validation(notebook_text)

    if eval_name == "full-standardization":
        checks = [
            (
                expectations[0],
                all(
                    marker in intro
                    for marker in [
                        "**Context**",
                        "**Objective**",
                        "**Method**",
                        "**Analysis structure**",
                        "**Data sources**",
                    ]
                ),
                "Found all README intro markers in notebook intro.",
                "Missing one or more intro markers among Context, Objective, Method, Analysis structure, or Data sources.",
            ),
            (
                expectations[1],
                all(section in notebook_text for section in REQUIRED_SECTIONS),
                "Found all required numbered sections from 1 through 8.",
                "Missing one or more required numbered sections.",
            ),
            (
                expectations[2],
                regex_any(validation, [r"no joins", r"no join", r"sem joins", r"sem join"])
                and regex_any(validation, [r"required columns", r"missing columns", r"require\("])
                and regex_any(validation, [r"duplic", r"gr[aã]o", r"pr_number"]),
                "Validation section mentions the no-join case, required columns, and duplicate or PR-grain checks.",
                "Validation section is missing one of: explicit no-join statement, required-column check, or duplicate/PR-grain check.",
            ),
            (
                expectations[3],
                all(table in notebook_text for table in EXPECTED_TABLES),
                "All expected Delta output table names are present.",
                "One or more expected Delta output table names are missing.",
            ),
            (
                expectations[4],
                regex_any(notes_text, [r"`sim` / `não` / `quero corrigir ou complementar`", r"sim / não / quero corrigir ou complementar"])
                and regex_any(notes_text, [r"Qual notebook", r"Qual objetivo", r"Você já tem os datasets", r"Esta estrutura faz sentido"])
                and regex_any(notes_text, [r"Racional do dataset", r"Racional dos datasets", r"Justificativa do dataset", r"Justificativa dos datasets"]),
                "Review notes include PT-BR checkpoint prompts, the expected confirmation pattern, and dataset rationale.",
                "Review notes are missing one of: PT-BR checkpoint prompts, the expected confirmation pattern, or dataset rationale.",
            ),
        ]
    elif eval_name == "validation-first-review":
        checks = [
            (
                expectations[0],
                regex_any(validation, [r"no joins", r"no join", r"sem joins", r"sem join"]),
                "Validation section explicitly states the pipeline has no joins.",
                "Validation section does not explicitly state the no-join scenario.",
            ),
            (
                expectations[1],
                regex_any(validation, [r"required columns", r"missing columns", r"require\("])
                and regex_any(validation, [r"duplic", r"pr_number", r"gr[aã]o"]),
                "Validation checks required columns and duplicate or PR-grain logic.",
                "Validation is missing a required-column check or duplicate/PR-grain check.",
            ),
            (
                expectations[2],
                regex_any(validation, [r"merged_at", r"min/max", r"date", r"cutoff"])
                and regex_any(validation, [r"repo_name", r"repo filter", r"distribution by repo", r"ai_tool"]),
                "Validation checks both date scope and repo filter behavior.",
                "Validation is missing explicit date-scope or repo-filter validation.",
            ),
            (
                expectations[3],
                regex_any(validation, [r"reconciliation", r"reconcile", r"aggregation", r"countDistinct", r"distinct PR", r"distinct-pr"]),
                "Validation includes reconciliation or aggregation-safety language.",
                "Validation does not show reconciliation or aggregation-safety checks.",
            ),
            (
                expectations[4],
                regex_any(notes_text, [r"`sim` / `não` / `quero corrigir ou complementar`", r"sim / não / quero corrigir ou complementar"])
                and regex_any(notes_text, [r"sem pesquisa", r"não usar Free Willy", r"não usar Slack", r"dataset já confirmado", r"no research required"]),
                "Review notes use checkpoint-style confirmation language and explain why dataset research was not triggered.",
                "Review notes do not clearly show the confirmation pattern or the no-research rationale.",
            ),
        ]
    elif eval_name == "checkpoint-contract-review":
        checks = [
            (
                expectations[0],
                all(section in notebook_text for section in REQUIRED_SECTIONS),
                "Found all required numbered sections from 1 through 8.",
                "Missing one or more required numbered sections.",
            ),
            (
                expectations[1],
                all(table in notebook_text for table in EXPECTED_TABLES),
                "All expected Delta output table names are present.",
                "One or more expected Delta output table names are missing.",
            ),
            (
                expectations[2],
                regex_any(validation, [r"required columns", r"missing columns", r"require\("])
                and regex_any(validation, [r"duplic", r"pr_number", r"gr[aã]o"])
                and regex_any(validation, [r"reconciliation", r"reconcile", r"aggregation", r"countDistinct"]),
                "Validation section checks required columns, duplicates/PR grain, and aggregation safety.",
                "Validation section is missing a required-column, duplicate/PR-grain, or aggregation-safety signal.",
            ),
            (
                expectations[3],
                regex_any(notes_text, [r"`sim` / `não` / `quero corrigir ou complementar`", r"sim / não / quero corrigir ou complementar"]),
                "Checkpoint artifact includes the expected PT-BR confirmation pattern.",
                "Checkpoint artifact is missing the PT-BR confirmation pattern `sim / não / quero corrigir ou complementar`.",
            ),
            (
                expectations[4],
                regex_any(notes_text, [r"Qual notebook", r"Qual objetivo", r"Você já tem os datasets", r"Esta estrutura faz sentido"]),
                "Checkpoint artifact contains PT-BR checkpoint prompts.",
                "Checkpoint artifact does not contain the expected PT-BR checkpoint prompts.",
            ),
            (
                expectations[5],
                regex_any(notes_text, [r"sem pesquisa", r"não usar Free Willy", r"não usar Slack", r"dataset já confirmado", r"research is not triggered", r"no dataset research is needed", r"only dataset"]),
                "Checkpoint artifact documents why dataset research was not triggered.",
                "Checkpoint artifact does not explain why dataset research was not triggered.",
            ),
        ]
    else:
        raise ValueError(f"Unknown eval name: {eval_name}")

    return [
        {
            "text": text,
            "passed": passed,
            "evidence": pass_evidence if passed else fail_evidence,
        }
        for text, passed, pass_evidence, fail_evidence in checks
    ]


def build_grading(notebook_text: str, notes_text: str, expectations_result: list[dict]) -> dict:
    passed = sum(1 for item in expectations_result if item["passed"])
    total = len(expectations_result)
    failed = total - passed
    pass_rate = round(passed / total, 2) if total else 0.0

    return {
        "expectations": expectations_result,
        "summary": {
            "passed": passed,
            "failed": failed,
            "total": total,
            "pass_rate": pass_rate,
        },
        "execution_metrics": {
            "tool_calls": {},
            "total_tool_calls": 0,
            "total_steps": 0,
            "errors_encountered": 0,
            "output_chars": len(notebook_text) + len(notes_text),
            "transcript_chars": 0,
        },
        "timing": {
            "executor_duration_seconds": 0.0,
            "grader_duration_seconds": 0.0,
            "total_duration_seconds": 0.0,
        },
        "claims": [],
        "user_notes_summary": {
            "uncertainties": parse_risks(notes_text),
            "needs_review": [],
            "workarounds": [],
        },
    }


def stats(values: list[float]) -> dict:
    mean = round(sum(values) / len(values), 4) if values else 0.0
    return {
        "mean": mean,
        "stddev": 0.0,
        "min": min(values) if values else 0.0,
        "max": max(values) if values else 0.0,
    }


def grade_iteration(iteration_dir: Path, skill_path: str) -> tuple[list[dict], dict]:
    runs: list[dict] = []

    for eval_dir in sorted(child for child in iteration_dir.iterdir() if child.is_dir()):
        metadata = json.loads((eval_dir / "eval_metadata.json").read_text())
        eval_id = metadata["eval_id"]
        eval_name = metadata["eval_name"]
        expectations = metadata["assertions"]

        for config_name in ["with_skill", "old_skill"]:
            config_dir = eval_dir / config_name
            output_dir = config_dir / "outputs"
            scala_files = list(output_dir.glob("*.scala"))
            if not scala_files:
                raise SystemExit(f"No scala output found in {output_dir}")
            if len(scala_files) > 1:
                raise SystemExit(
                    f"Expected exactly one scala output in {output_dir}, found {len(scala_files)}: "
                    + ", ".join(path.name for path in sorted(scala_files))
                )

            notebook_text = read_text(scala_files[0])
            notes_text = read_text(output_dir / "review-notes.md")
            expectations_result = grade_expectations(eval_name, notebook_text, notes_text, expectations)
            grading = build_grading(notebook_text, notes_text, expectations_result)

            (config_dir / "grading.json").write_text(
                json.dumps(grading, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            (config_dir / "timing.json").write_text(
                json.dumps(
                    {
                        "total_tokens": 0,
                        "duration_ms": 0,
                        "total_duration_seconds": 0.0,
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            runs.append(
                {
                    "eval_id": eval_id,
                    "eval_name": eval_name,
                    "configuration": config_name,
                    "run_number": 1,
                    "result": {
                        "pass_rate": grading["summary"]["pass_rate"],
                        "passed": grading["summary"]["passed"],
                        "failed": grading["summary"]["failed"],
                        "total": grading["summary"]["total"],
                        "time_seconds": 0.0,
                        "tokens": 0,
                        "tool_calls": 0,
                        "errors": 0,
                    },
                    "expectations": grading["expectations"],
                    "notes": grading["user_notes_summary"]["uncertainties"],
                }
            )

    summary = {}
    for config_name in ["with_skill", "old_skill"]:
        config_runs = [run for run in runs if run["configuration"] == config_name]
        summary[config_name] = {
            "pass_rate": stats([run["result"]["pass_rate"] for run in config_runs]),
            "time_seconds": stats([0.0 for _ in config_runs]),
            "tokens": stats([0 for _ in config_runs]),
        }

    summary["delta"] = {
        "pass_rate": f"{summary['with_skill']['pass_rate']['mean'] - summary['old_skill']['pass_rate']['mean']:+.2f}",
        "time_seconds": f"{summary['with_skill']['time_seconds']['mean'] - summary['old_skill']['time_seconds']['mean']:+.1f}",
        "tokens": f"{summary['with_skill']['tokens']['mean'] - summary['old_skill']['tokens']['mean']:+.0f}",
    }

    benchmark = {
        "metadata": {
            "skill_name": Path(skill_path).name,
            "skill_path": skill_path,
            "executor_model": "fast-subagent",
            "analyzer_model": "gpt-5.4",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "evals_run": sorted({run["eval_id"] for run in runs}),
            "runs_per_configuration": 1,
        },
        "runs": runs,
        "run_summary": summary,
    }
    return runs, benchmark


def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: grade_benchmark.py <iteration-dir> <skill-path>")
        return 1

    iteration_dir = Path(sys.argv[1]).resolve()
    skill_path = sys.argv[2]
    workspace_root = iteration_dir.parent

    _, benchmark = grade_iteration(iteration_dir, skill_path)
    (workspace_root / "benchmark.json").write_text(
        json.dumps(benchmark, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    lines = [
        "# Benchmark Summary",
        "",
        "| Configuration | Mean pass rate | Mean time (s) | Mean tokens |",
        "| --- | ---: | ---: | ---: |",
        f"| with_skill | {benchmark['run_summary']['with_skill']['pass_rate']['mean']:.2f} | {benchmark['run_summary']['with_skill']['time_seconds']['mean']:.1f} | {benchmark['run_summary']['with_skill']['tokens']['mean']:.0f} |",
        f"| old_skill | {benchmark['run_summary']['old_skill']['pass_rate']['mean']:.2f} | {benchmark['run_summary']['old_skill']['time_seconds']['mean']:.1f} | {benchmark['run_summary']['old_skill']['tokens']['mean']:.0f} |",
        "",
        f"Delta pass rate: {benchmark['run_summary']['delta']['pass_rate']}",
    ]
    (workspace_root / "benchmark.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote benchmark artifacts to {workspace_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
