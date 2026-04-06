#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from collections import Counter
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore

TOKEN_PATTERN = re.compile(
    r"[A-Za-z0-9.-]+(?:\+[A-Za-z0-9.-]+)?(?:-or-later|-only)?(?:\s+WITH\s+[A-Za-z0-9.-]+)?"
)
SPDX_KEYWORDS = {"AND", "OR", "WITH"}


def load_policy(policy_path: Path) -> tuple[set[str], set[str], dict[str, str]]:
    with policy_path.open("rb") as f:
        data = tomllib.load(f)

    licenses = data.get("licenses", {})
    allow = set(licenses.get("allow", []))
    deny = set(licenses.get("deny", []))

    if not allow:
        raise ValueError("[licenses].allow is empty in policy file")

    overrides = data.get("overrides", {})
    rust_overrides_raw = overrides.get("rust", {}) if isinstance(overrides, dict) else {}
    rust_overrides = {
        str(key).strip(): str(value).strip()
        for key, value in rust_overrides_raw.items()
        if str(key).strip() and str(value).strip()
    }

    return allow, deny, rust_overrides


def tokenize(expression: str) -> set[str]:
    raw = {m.group(0).strip() for m in TOKEN_PATTERN.finditer(expression)}
    return {token for token in raw if token.upper() not in SPDX_KEYWORDS}


def evaluate_expression(
    expression: str,
    allow: set[str],
    deny: set[str],
) -> list[str]:
    issues: list[str] = []
    expr = (expression or "").strip()

    if not expr:
        return ["missing license expression"]

    tokens = tokenize(expr)
    if not tokens:
        return [f"could not parse license expression: {expr}"]

    unknown = sorted(token for token in tokens if token not in allow and token not in deny)
    if unknown:
        issues.append(f"unknown license token(s): {', '.join(unknown)}")

    denied_tokens = sorted(token for token in tokens if token in deny)
    if denied_tokens:
        has_or = " OR " in f" {expr} "
        has_allowed_alternative = any(token in allow for token in tokens)
        if not (has_or and has_allowed_alternative):
            issues.append(f"denied license token(s): {', '.join(denied_tokens)}")

    if not any(token in allow for token in tokens):
        issues.append("no allowed license token found")

    return issues


def validate_rust(
    rust_report_path: Path,
    allow: set[str],
    deny: set[str],
    rust_overrides: dict[str, str],
) -> tuple[list[str], list[dict], Counter[str]]:
    data = json.loads(rust_report_path.read_text(encoding="utf-8"))
    errors: list[str] = []
    normalized: list[dict] = []
    counts: Counter[str] = Counter()

    for dep in data:
        name = dep.get("name", "<unknown>")
        version = dep.get("version", "<unknown>")
        license_expr = (dep.get("license") or "").strip()
        if not license_expr:
            exact_key = f"{name}@{version}"
            license_expr = rust_overrides.get(exact_key, rust_overrides.get(name, ""))
        counts[license_expr or "<missing>"] += 1

        issues = evaluate_expression(license_expr, allow, deny)
        if issues:
            errors.append(
                f"rust:{name}@{version}: {license_expr or '<missing>'} -> {'; '.join(issues)}"
            )

        normalized.append(
            {
                "name": name,
                "version": version,
                "license": license_expr,
                "repository": dep.get("repository", ""),
            }
        )

    normalized.sort(key=lambda x: (x["name"], x["version"]))
    return errors, normalized, counts


def _npm_entries_from_file(npm_report_path: Path) -> list[dict]:
    raw = json.loads(npm_report_path.read_text(encoding="utf-8"))
    entries: list[dict] = []

    for package, metadata in raw.items():
        license_value = metadata.get("licenses", "")
        if isinstance(license_value, list):
            license_expr = " OR ".join(str(item) for item in license_value)
        else:
            license_expr = str(license_value)

        entries.append(
            {
                "package": package,
                "license": license_expr.strip(),
                "repository": str(metadata.get("repository", "")).strip(),
            }
        )

    return sorted(entries, key=lambda x: x["package"])


def validate_npm(
    npm_report_path: Path,
    scope: str,
    allow: set[str],
    deny: set[str],
) -> tuple[list[str], list[dict], Counter[str]]:
    entries = _npm_entries_from_file(npm_report_path)
    errors: list[str] = []
    counts: Counter[str] = Counter()

    for entry in entries:
        package = entry["package"]
        license_expr = entry["license"]
        counts[license_expr or "<missing>"] += 1

        if package.startswith(f"{scope}@") and "UNLICENSED" in license_expr.upper():
            # Root package in some lock states may still surface as UNLICENSED.
            continue

        issues = evaluate_expression(license_expr, allow, deny)
        if issues:
            errors.append(
                f"npm:{scope}:{package}: {license_expr or '<missing>'} -> {'; '.join(issues)}"
            )

    return errors, entries, counts


def write_markdown(
    output_path: Path,
    rust_entries: list[dict],
    ui_entries: list[dict],
    sdk_entries: list[dict],
    rust_counts: Counter[str],
    ui_counts: Counter[str],
    sdk_counts: Counter[str],
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    now = dt.datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M:%SZ")

    lines: list[str] = []
    lines.append("# Third-Party Licenses")
    lines.append("")
    lines.append(f"Generated at: {now} (UTC)")
    lines.append("")

    lines.append("## Rust Runtime Dependencies")
    lines.append("")
    lines.append("| License | Count |")
    lines.append("|---|---:|")
    for license_expr, count in sorted(rust_counts.items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"| {license_expr or '<missing>'} | {count} |")
    lines.append("")
    lines.append("| Crate | Version | License | Repository |")
    lines.append("|---|---:|---|---|")
    for entry in rust_entries:
        lines.append(
            f"| {entry['name']} | {entry['version']} | {entry['license'] or '<missing>'} | {entry['repository'] or ''} |"
        )
    lines.append("")

    lines.append("## UI Production Dependencies")
    lines.append("")
    lines.append("| License | Count |")
    lines.append("|---|---:|")
    for license_expr, count in sorted(ui_counts.items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"| {license_expr or '<missing>'} | {count} |")
    lines.append("")
    lines.append("| Package | License | Repository |")
    lines.append("|---|---|---|")
    for entry in ui_entries:
        lines.append(
            f"| {entry['package']} | {entry['license'] or '<missing>'} | {entry['repository'] or ''} |"
        )
    lines.append("")

    lines.append("## TypeScript SDK Production Dependencies")
    lines.append("")
    lines.append("| License | Count |")
    lines.append("|---|---:|")
    for license_expr, count in sorted(sdk_counts.items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"| {license_expr or '<missing>'} | {count} |")
    lines.append("")
    lines.append("| Package | License | Repository |")
    lines.append("|---|---|---|")
    for entry in sdk_entries:
        lines.append(
            f"| {entry['package']} | {entry['license'] or '<missing>'} | {entry['repository'] or ''} |"
        )
    lines.append("")

    output_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate and export third-party license reports")
    parser.add_argument("--policy", required=True, help="Path to TOML policy file")
    parser.add_argument("--rust-report", required=True, help="Path to cargo-license JSON report")
    parser.add_argument("--ui-report", required=True, help="Path to UI npm license-checker JSON report")
    parser.add_argument("--sdk-report", required=True, help="Path to SDK npm license-checker JSON report")
    parser.add_argument("--output", required=True, help="Output markdown report path")
    args = parser.parse_args()

    policy_path = Path(args.policy)
    rust_report_path = Path(args.rust_report)
    ui_report_path = Path(args.ui_report)
    sdk_report_path = Path(args.sdk_report)
    output_path = Path(args.output)

    allow, deny, rust_overrides = load_policy(policy_path)

    rust_errors, rust_entries, rust_counts = validate_rust(
        rust_report_path,
        allow,
        deny,
        rust_overrides,
    )
    ui_errors, ui_entries, ui_counts = validate_npm(ui_report_path, "kalamdb-admin-ui", allow, deny)
    sdk_errors, sdk_entries, sdk_counts = validate_npm(sdk_report_path, "@kalamdb/client", allow, deny)

    all_errors = rust_errors + ui_errors + sdk_errors

    write_markdown(
        output_path,
        rust_entries,
        ui_entries,
        sdk_entries,
        rust_counts,
        ui_counts,
        sdk_counts,
    )

    if all_errors:
        print("License compliance check failed:")
        for err in all_errors:
            print(f" - {err}")
        return 1

    print("License compliance check passed.")
    print(f"Report written to {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
