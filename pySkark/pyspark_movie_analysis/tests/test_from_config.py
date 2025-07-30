import pytest
import ast
import student_submission.solution as sol
from pyspark.sql import SparkSession
import os
import json

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOG_PATH = os.path.join(ROOT_DIR, "test_report.log")
CONFIG_PATH = os.path.join(ROOT_DIR, "tests", "test_config.json")

def log_result(func_name, status):
    try:
        with open(LOG_PATH, "a") as f:
            f.write(f"[{status.upper()}] {func_name}\n")
    except Exception as e:
        print(f"Logging failed: {e}")

def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("StreamingPlatformTest") \
        .getOrCreate()

@pytest.fixture(scope="module")
def test_data(spark):
    # Pre-load dataframes for use in tests
    watch_df = sol.create_watch_history_df(spark, "data/watch_history.csv")
    user_df = sol.create_user_df(spark, "data/users.csv")
    return [watch_df, user_df]

def check_no_pass(func_name):
    tree = ast.parse(open("student_submission/solution.py").read())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == func_name:
            for sub in ast.walk(node):
                if isinstance(sub, ast.Pass):
                    raise AssertionError(f"Anti-cheat violation: 'pass' in {func_name}()")

def match_schema(df, expected_schema):
    actual = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
    expected = [(name, dtype) for name, dtype in expected_schema]
    for (a_name, a_type), (e_name, e_type) in zip(actual, expected):
        assert a_name == e_name, f"Column mismatch: expected {e_name}, got {a_name}"
        assert a_type == e_type, f"Type mismatch for {a_name}: expected {e_type}, got {a_type}"

@pytest.mark.parametrize("test_case", load_config())
def test_from_config(test_case, spark, test_data):
    func_name = test_case["function"]
    args = test_case.get("args", {})
    status = "pass"

    try:
        check_no_pass(func_name)

        # Map special args
        for key, val in args.items():
            if val == "__spark__":
                args[key] = spark
            elif isinstance(val, str) and val.startswith("__result__."):
                index = int(val.split(".")[1])
                args[key] = test_data[index]

        # Call the function
        result = getattr(sol, func_name)(**args)

        # Handle schema checks
        if test_case.get("check_schema", False):
            expected_schema = test_case["expected_schema"]
            match_schema(result, expected_schema)

        # Handle type checks
        elif "expected" in test_case:
            expected_type = test_case["expected"]
            if expected_type == "DataFrame":
                assert result.count() >= 0
            elif expected_type == "list":
                assert isinstance(result, list)
            elif expected_type == "str":
                assert isinstance(result, str)
            elif expected_type == "tuple":
                assert isinstance(result, tuple)

    except Exception:
        status = "fail"
        raise
    finally:
        log_result(func_name, status)
        print(f"[{status.upper()}] {func_name}")

