import os
import pytest
import json
import ast
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
import student_submission.solution as sol

# Load test configuration
with open("tests/test_config.json") as f:
    config = json.load(f)

# Color codes for console output
GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"

# Setup SparkSession
@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[*]").config("spark.hadoop.fs.defaultFS", "file:///").appName("WarrantyTest").getOrCreate()

# Function to evaluate dependency chain
def resolve_dependency(function_name, cache, spark):
    if function_name in cache:
        return cache[function_name]
    func = getattr(sol, function_name)
    args_config = next((item for item in config if item["function"] == function_name), None)
    args = []
    for arg in args_config["args"]:
        if arg == "spark":
            args.append(spark)
        elif isinstance(arg, str) and hasattr(sol, arg):
            args.append(resolve_dependency(arg, cache, spark))
        else:
            args.append(arg)
    result = func(*args)
    cache[function_name] = result
    return result

# Logger
def log_status(status, func_name):
    timestamp = datetime.now()
    color = GREEN if status == "PASS" else RED
    print(f"{color}[{status}] {func_name} at {timestamp}{RESET}")
    with open("test_report.log", "a") as f:
        f.write(f"[{status}] {func_name} at {timestamp}\n")

@pytest.mark.parametrize("test_case", config)
def test_configurable_functions(test_case, spark):
    func_name = test_case["function"]
    expected = test_case["expected"]

    try:
        # Anti-cheat check
        with open("student_submission/solution.py") as f:
            tree = ast.parse(f.read())
        func_node = next((n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == func_name), None)
        if func_node:
            source = ast.get_source_segment(open("student_submission/solution.py").read(), func_node)
            for banned in ["pass", "Alice", "Data Science"]:
                if banned in source:
                    log_status("FAIL", func_name)
                    raise AssertionError(f"Anti-cheat violation: {banned}")

        cache = {}
        result = resolve_dependency(func_name, cache, spark)

        if expected == "DataFrame":
            assert isinstance(result, DataFrame)
        elif expected == "float":
            assert isinstance(result, float)

        log_status("PASS", func_name)

    except Exception as e:
        log_status("FAIL", func_name)
        raise e
