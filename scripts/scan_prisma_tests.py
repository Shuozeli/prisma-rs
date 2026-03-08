#!/usr/bin/env python3
"""Scan the original Prisma TypeScript repo for all unit/integration tests.

Finds test files (*.test.ts, *.spec.ts, *.test.js, *.spec.js), parses
describe/test/it blocks, and produces a structured summary grouped by
package and category.

Usage:
    python3 scripts/scan_prisma_tests.py [--json] [--verbose]
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

PRISMA_ROOT = Path(__file__).resolve().parent.parent / "prisma"

# Regex patterns for extracting test structure from JS/TS files.
# Handles: describe('name', ...), test('name', ...), it('name', ...),
#           describe.each(...)('name', ...), test.each(...)('name', ...),
#           test.skip('name', ...), describe.only('name', ...), etc.
RE_DESCRIBE = re.compile(
    r"""(?:^|\s)describe(?:\.(?:each|only|skip|concurrent|sequential|todo)(?:\([^)]*\))?)?"""
    r"""\s*\(\s*(['"`])(.+?)\1""",
    re.MULTILINE,
)

RE_TEST = re.compile(
    r"""(?:^|\s)(?:test|it)(?:\.(?:each|only|skip|concurrent|sequential|todo|failing|skipIf|runIf)(?:\([^)]*\))?)?"""
    r"""\s*\(\s*(['"`])(.+?)\1""",
    re.MULTILINE,
)

# Template literal describe/test (backtick strings)
RE_DESCRIBE_TEMPLATE = re.compile(
    r"""(?:^|\s)describe(?:\.(?:each|only|skip|concurrent|sequential|todo)(?:\([^)]*\))?)?"""
    r"""\s*\(\s*`([^`]+)`""",
    re.MULTILINE,
)

RE_TEST_TEMPLATE = re.compile(
    r"""(?:^|\s)(?:test|it)(?:\.(?:each|only|skip|concurrent|sequential|todo|failing|skipIf|runIf)(?:\([^)]*\))?)?"""
    r"""\s*\(\s*`([^`]+)`""",
    re.MULTILINE,
)


@dataclass
class TestCase:
    name: str
    line: int


@dataclass
class DescribeBlock:
    name: str
    line: int
    tests: list[TestCase] = field(default_factory=list)


@dataclass
class TestFile:
    path: str  # relative to prisma root
    package: str
    category: str  # sub-path within the package
    describes: list[DescribeBlock] = field(default_factory=list)
    top_level_tests: list[TestCase] = field(default_factory=list)

    @property
    def total_test_count(self) -> int:
        count = len(self.top_level_tests)
        for d in self.describes:
            count += len(d.tests)
        return count


def find_test_files(root: Path) -> list[Path]:
    """Find all test files under the Prisma repo."""
    patterns = ["*.test.ts", "*.spec.ts", "*.test.js", "*.spec.js"]
    results = []
    for pattern in patterns:
        results.extend(root.rglob(pattern))

    # Exclude node_modules and build artifacts
    filtered = []
    for p in results:
        rel = str(p.relative_to(root))
        if "node_modules" in rel or "/dist/" in rel or "/.generated/" in rel:
            continue
        filtered.append(p)

    return sorted(filtered)


def extract_package(filepath: Path, root: Path) -> tuple[str, str]:
    """Extract package name and sub-category from file path."""
    rel = filepath.relative_to(root)
    parts = rel.parts

    if parts[0] == "packages" and len(parts) > 1:
        package = parts[1]
        # Category is the path between the package and the test file
        category_parts = parts[2:-1]
        # Simplify common prefixes
        cat = "/".join(category_parts)
        cat = cat.replace("src/__tests__/", "").replace("src/", "")
        cat = cat.replace("tests/", "").replace("__tests__/", "")
        return package, cat or "(root)"
    return "(other)", str(rel.parent)


def parse_test_file(filepath: Path, root: Path) -> TestFile:
    """Parse a test file to extract describe blocks and test cases."""
    package, category = extract_package(filepath, root)
    rel_path = str(filepath.relative_to(root))

    tf = TestFile(
        path=rel_path,
        package=package,
        category=category,
    )

    try:
        content = filepath.read_text(encoding="utf-8", errors="replace")
    except (OSError, UnicodeDecodeError):
        return tf

    lines = content.split("\n")

    # Find all describe blocks with their line numbers
    describes: list[tuple[int, str]] = []
    for match in RE_DESCRIBE.finditer(content):
        line_no = content[: match.start()].count("\n") + 1
        name = match.group(2)
        describes.append((line_no, name))
    for match in RE_DESCRIBE_TEMPLATE.finditer(content):
        line_no = content[: match.start()].count("\n") + 1
        name = match.group(1).strip()
        describes.append((line_no, name))

    # Find all test/it blocks with their line numbers
    tests: list[tuple[int, str]] = []
    for match in RE_TEST.finditer(content):
        line_no = content[: match.start()].count("\n") + 1
        name = match.group(2)
        tests.append((line_no, name))
    for match in RE_TEST_TEMPLATE.finditer(content):
        line_no = content[: match.start()].count("\n") + 1
        name = match.group(1).strip()
        tests.append((line_no, name))

    # Deduplicate (template patterns may overlap with quoted patterns)
    describes = sorted(set(describes))
    tests = sorted(set(tests))

    # Build describe blocks - assign tests to the nearest preceding describe
    describe_blocks: list[DescribeBlock] = []
    for line_no, name in describes:
        describe_blocks.append(DescribeBlock(name=name, line=line_no))

    # Sort by line number
    describe_blocks.sort(key=lambda d: d.line)

    # Assign each test to its enclosing describe block (simple heuristic:
    # the last describe that started before this test)
    for test_line, test_name in tests:
        tc = TestCase(name=test_name, line=test_line)
        # Find the innermost describe block containing this test
        assigned = False
        for db in reversed(describe_blocks):
            if db.line < test_line:
                db.tests.append(tc)
                assigned = True
                break
        if not assigned:
            tf.top_level_tests.append(tc)

    tf.describes = describe_blocks
    return tf


def print_summary(test_files: list[TestFile], verbose: bool = False) -> None:
    """Print a human-readable summary."""
    # Group by package
    by_package: dict[str, list[TestFile]] = defaultdict(list)
    for tf in test_files:
        by_package[tf.package].append(tf)

    total_files = len(test_files)
    total_tests = sum(tf.total_test_count for tf in test_files)
    total_describes = sum(len(tf.describes) for tf in test_files)

    print("=" * 78)
    print(f"  PRISMA TYPESCRIPT TEST SUITE OVERVIEW")
    print(f"  {total_files} test files | {total_describes} describe blocks | {total_tests} test cases")
    print("=" * 78)
    print()

    # Sort packages by test count (descending)
    sorted_packages = sorted(
        by_package.items(),
        key=lambda kv: sum(tf.total_test_count for tf in kv[1]),
        reverse=True,
    )

    for package, files in sorted_packages:
        pkg_tests = sum(tf.total_test_count for tf in files)
        pkg_describes = sum(len(tf.describes) for tf in files)

        print(f"## {package}")
        print(f"   {len(files)} files | {pkg_describes} describes | {pkg_tests} tests")
        print()

        # Group by category within package
        by_category: dict[str, list[TestFile]] = defaultdict(list)
        for tf in files:
            by_category[tf.category].append(tf)

        sorted_categories = sorted(
            by_category.items(),
            key=lambda kv: sum(tf.total_test_count for tf in kv[1]),
            reverse=True,
        )

        for category, cat_files in sorted_categories:
            cat_tests = sum(tf.total_test_count for tf in cat_files)
            cat_describes = sum(len(tf.describes) for tf in cat_files)

            if category == "(root)":
                cat_label = "(root level)"
            else:
                cat_label = category

            print(f"   [{cat_label}] {len(cat_files)} files, {cat_tests} tests")

            if verbose:
                for tf in sorted(cat_files, key=lambda f: f.path):
                    filename = os.path.basename(tf.path)
                    print(f"      {filename} ({tf.total_test_count} tests)")

                    for db in tf.describes:
                        if db.tests:
                            print(f"        describe: {db.name}")
                            for tc in db.tests:
                                print(f"          - {tc.name}")
                        else:
                            print(f"        describe: {db.name} (no direct tests)")

                    for tc in tf.top_level_tests:
                        print(f"        - {tc.name}")
            else:
                # In non-verbose, show describe names grouped
                all_describes = []
                for tf in cat_files:
                    for db in tf.describes:
                        all_describes.append(db.name)

                if all_describes:
                    # Show unique describe names
                    unique = list(dict.fromkeys(all_describes))  # preserve order, dedupe
                    for name in unique[:15]:
                        count = all_describes.count(name)
                        suffix = f" (x{count})" if count > 1 else ""
                        print(f"      - {name}{suffix}")
                    if len(unique) > 15:
                        print(f"      ... and {len(unique) - 15} more")

        print()

    # Print top-level statistics table
    print("=" * 78)
    print(f"  PACKAGE SUMMARY")
    print("=" * 78)
    print(f"  {'Package':<40} {'Files':>6} {'Describes':>10} {'Tests':>7}")
    print(f"  {'-'*40} {'-'*6} {'-'*10} {'-'*7}")
    for package, files in sorted_packages:
        pkg_tests = sum(tf.total_test_count for tf in files)
        pkg_describes = sum(len(tf.describes) for tf in files)
        print(f"  {package:<40} {len(files):>6} {pkg_describes:>10} {pkg_tests:>7}")
    print(f"  {'-'*40} {'-'*6} {'-'*10} {'-'*7}")
    print(f"  {'TOTAL':<40} {total_files:>6} {total_describes:>10} {total_tests:>7}")
    print()


def print_json(test_files: list[TestFile]) -> None:
    """Print results as JSON."""
    by_package: dict[str, list[TestFile]] = defaultdict(list)
    for tf in test_files:
        by_package[tf.package].append(tf)

    output = {
        "summary": {
            "total_files": len(test_files),
            "total_describes": sum(len(tf.describes) for tf in test_files),
            "total_tests": sum(tf.total_test_count for tf in test_files),
        },
        "packages": {},
    }

    for package, files in sorted(by_package.items()):
        pkg_data = {
            "file_count": len(files),
            "test_count": sum(tf.total_test_count for tf in files),
            "describe_count": sum(len(tf.describes) for tf in files),
            "files": [],
        }

        for tf in sorted(files, key=lambda f: f.path):
            file_data = {
                "path": tf.path,
                "category": tf.category,
                "test_count": tf.total_test_count,
                "describes": [
                    {
                        "name": db.name,
                        "line": db.line,
                        "tests": [
                            {"name": tc.name, "line": tc.line}
                            for tc in db.tests
                        ],
                    }
                    for db in tf.describes
                ],
                "top_level_tests": [
                    {"name": tc.name, "line": tc.line}
                    for tc in tf.top_level_tests
                ],
            }
            pkg_data["files"].append(file_data)

        output["packages"][package] = pkg_data

    print(json.dumps(output, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scan Prisma TS repo for test structure"
    )
    parser.add_argument(
        "--json", action="store_true", help="Output as JSON"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Show individual test case names"
    )
    parser.add_argument(
        "--package", "-p", type=str, default=None,
        help="Filter to a specific package (e.g., 'client')"
    )
    args = parser.parse_args()

    if not PRISMA_ROOT.exists():
        print(f"Error: Prisma repo not found at {PRISMA_ROOT}", file=sys.stderr)
        sys.exit(1)

    print(f"Scanning {PRISMA_ROOT} ...", file=sys.stderr)
    test_paths = find_test_files(PRISMA_ROOT)
    print(f"Found {len(test_paths)} test files", file=sys.stderr)

    test_files = []
    for path in test_paths:
        tf = parse_test_file(path, PRISMA_ROOT)
        if args.package and tf.package != args.package:
            continue
        test_files.append(tf)

    if args.json:
        print_json(test_files)
    else:
        print_summary(test_files, verbose=args.verbose)


if __name__ == "__main__":
    main()
