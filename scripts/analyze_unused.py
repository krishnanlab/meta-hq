#!/usr/bin/env python3
"""
AST-based analysis to find unused functions and classes in Python codebase.

Author: Parker Hicks
Date: 2025-11-06

Last updated: 2025-11-06 by Parker Hicks
"""

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGES = ROOT / "packages"


class DefinitionCollector(ast.NodeVisitor):
    """Collect all function and class definitions."""

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.functions: list[tuple[str, int]] = []
        self.classes: list[tuple[str, int]] = []
        self.methods: list[tuple[str, str, int]] = (
            []
        )  # (class_name, method_name, lineno)
        self.current_class = None

    def visit_FunctionDef(self, node):
        if self.current_class:
            self.methods.append((self.current_class, node.name, node.lineno))
        else:
            self.functions.append((node.name, node.lineno))
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node):
        if self.current_class:
            self.methods.append((self.current_class, node.name, node.lineno))
        else:
            self.functions.append((node.name, node.lineno))
        self.generic_visit(node)

    def visit_ClassDef(self, node):
        self.classes.append((node.name, node.lineno))
        old_class = self.current_class
        self.current_class = node.name
        self.generic_visit(node)
        self.current_class = old_class


class UsageCollector(ast.NodeVisitor):
    """Collect all name references."""

    def __init__(self):
        self.names: set[str] = set()
        self.imports: set[str] = set()

    def visit_Name(self, node):
        self.names.add(node.id)
        self.generic_visit(node)

    def visit_Attribute(self, node):
        # track attribute accesses like obj.method()
        if isinstance(node.value, ast.Name):
            self.names.add(node.value.id)
        self.names.add(node.attr)
        self.generic_visit(node)

    def visit_Import(self, node):
        for alias in node.names:
            name = alias.asname if alias.asname else alias.name
            self.imports.add(name)
        self.generic_visit(node)

    def visit_ImportFrom(self, node):
        for alias in node.names:
            name = alias.asname if alias.asname else alias.name
            self.imports.add(name)
        self.generic_visit(node)


def analyze_file(
    filepath: Path,
) -> tuple[DefinitionCollector | None, UsageCollector | None]:
    """Analyze a single Python file."""
    with open(filepath, "r", encoding="utf-8") as f:
        try:
            tree = ast.parse(f.read(), filename=str(filepath))
        except SyntaxError:
            return None, None

    def_collector = DefinitionCollector(str(filepath))
    def_collector.visit(tree)

    use_collector = UsageCollector()
    use_collector.visit(tree)

    return def_collector, use_collector


def find_python_files(root_dir: str) -> list[Path]:
    """Find all Python files in the directory."""
    python_files = []
    for path in Path(root_dir).rglob("*.py"):
        # skip __pycache__ and .venv
        if "__pycache__" not in str(path) and ".venv" not in str(path):
            python_files.append(path)
    return python_files


def analyze_codebase(root_dir: str):
    """Analyze entire codebase for unused code."""

    # collect all definitions and usages
    all_definitions = {
        "functions": {},  # filepath -> [(name, lineno), ...]
        "classes": {},
        "methods": {},
    }
    all_usages = set()  # all names referenced anywhere

    files = find_python_files(root_dir)

    for filepath in files:
        def_collector, use_collector = analyze_file(filepath)
        if def_collector is None:
            continue

        rel_path = str(filepath.relative_to(root_dir))

        if def_collector.functions:
            all_definitions["functions"][rel_path] = def_collector.functions
        if def_collector.classes:
            all_definitions["classes"][rel_path] = def_collector.classes
        if def_collector.methods:
            all_definitions["methods"][rel_path] = def_collector.methods

        # collect all names used in this file
        all_usages.update(use_collector.names)
        all_usages.update(use_collector.imports)

    # find unused items
    unused = {"functions": [], "classes": [], "methods": []}

    # check functions
    for filepath, functions in all_definitions["functions"].items():
        for func_name, lineno in functions:
            # skip dunder methods
            if func_name.startswith("__") and func_name.endswith("__"):
                continue
            # skip if used anywhere
            if func_name not in all_usages:
                unused["functions"].append((filepath, func_name, lineno))

    # check classes
    for filepath, classes in all_definitions["classes"].items():
        for class_name, lineno in classes:
            if class_name not in all_usages:
                unused["classes"].append((filepath, class_name, lineno))

    # check methods (more conservative - only if class is used but method isn't)
    for filepath, methods in all_definitions["methods"].items():
        for class_name, method_name, lineno in methods:
            # skip dunder methods
            if method_name.startswith("__") and method_name.endswith("__"):
                continue
            # if class is used but method isn't, it might be unused
            if class_name in all_usages and method_name not in all_usages:
                unused["methods"].append((filepath, class_name, method_name, lineno))

    return unused, all_definitions


def main():
    """Run analysis and print results."""

    print("Analyzing codebase for unused code...")
    print(f"Root directory: {PACKAGES}\n")

    unused, all_defs = analyze_codebase(PACKAGES)

    # Count totals
    total_funcs = sum(len(funcs) for funcs in all_defs["functions"].values())
    total_classes = sum(len(classes) for classes in all_defs["classes"].values())
    total_methods = sum(len(methods) for methods in all_defs["methods"].values())

    print("=" * 80)
    print("STATISTICS")
    print("=" * 80)
    print(f"Total functions defined: {total_funcs}")
    print(f"Total classes defined: {total_classes}")
    print(f"Total methods defined: {total_methods}")
    print(f"\nPotentially unused functions: {len(unused['functions'])}")
    print(f"Potentially unused classes: {len(unused['classes'])}")
    print(f"Potentially unused methods: {len(unused['methods'])}")
    print()

    # Print unused functions
    if unused["functions"]:
        print("=" * 80)
        print("POTENTIALLY UNUSED FUNCTIONS")
        print("=" * 80)
        for filepath, name, lineno in sorted(unused["functions"]):
            print(f"{filepath}:{lineno} - {name}")
        print()

    # Print unused classes
    if unused["classes"]:
        print("=" * 80)
        print("POTENTIALLY UNUSED CLASSES")
        print("=" * 80)
        for filepath, name, lineno in sorted(unused["classes"]):
            print(f"{filepath}:{lineno} - {name}")
        print()

    # Print unused methods
    if unused["methods"]:
        print("=" * 80)
        print("POTENTIALLY UNUSED METHODS")
        print("=" * 80)
        for filepath, class_name, method_name, lineno in sorted(unused["methods"]):
            print(f"{filepath}:{lineno} - {class_name}.{method_name}")
        print()


if __name__ == "__main__":
    main()
