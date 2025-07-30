import os
import pytest
import json
import ast
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import student_submission.solution as sol

with open("tests/test_config.json") as f:
    config = json.load(f)

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[*]").appName("RetailOrderTests").getOrCreate()

@pytest.fixture(scope="module")
def sample_orders_df(spark):
    from student_submission.solution import create_sample_orders
    return create_sample_orders(spark)

@pytest.fixture(scope="module")
def sample_returns_df(spark):
    data = [
        ("O2", "Damaged")
    ]
    schema = ["order_id", "return_reason"]
    return spark.createDataFrame(data, schema)

def log_result(status, func_name):
    msg = f"[{status}] {func_name} at {datetime.now()}"
    with open("test_report.log", "a") as f:
        f.write(msg + "\n")

GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"

def validate_sorting(code, func_name):
    if func_name in ["top_categories_by_revenue", "highest_value_order"]:
        tree = ast.parse(code)
        found_orderby = False
        found_limit = False
        is_descending = False

        class Visitor(ast.NodeVisitor):
            def visit_Call(self, node):
                nonlocal found_orderby, found_limit, is_descending
                if isinstance(node.func, ast.Attribute):
                    if node.func.attr == "orderBy":
                        found_orderby = True
                        for arg in node.args:
                            if isinstance(arg, ast.Call) and getattr(arg.func, 'attr', '') == 'desc':
                                is_descending = True
                    elif node.func.attr == "limit":
                        found_limit = True
                self.generic_visit(node)

        Visitor().visit(tree)
        if not found_orderby:
            raise AssertionError("Anti-cheat violation: Missing orderBy operation")
        if not is_descending:
            raise AssertionError("Anti-cheat violation: Must use desc() for descending sort")
        if not found_limit:
            raise AssertionError("Anti-cheat violation: Missing limit() after orderBy")

def validate_null_handling(code, func_name):
    if func_name == "replace_null_prices":
        tree = ast.parse(code)
        found_fillna = False

        class Visitor(ast.NodeVisitor):
            def visit_Call(self, node):
                nonlocal found_fillna
                if isinstance(node.func, ast.Attribute) and node.func.attr == "fillna":
                    found_fillna = True
                self.generic_visit(node)

        Visitor().visit(tree)
        if not found_fillna:
            raise AssertionError("Anti-cheat violation: Missing fillna() for null handling")

@pytest.mark.parametrize("test_case", config)
def test_configurable_functions(test_case, spark, sample_orders_df, sample_returns_df):
    func_name = test_case["function"]
    args = test_case["args"]
    expected = test_case["expected"]

    arg_map = {
        "spark": spark,
        "data/orders.csv": "data/orders.csv",
        "sample_orders_df": sample_orders_df,
        "sample_returns_df": sample_returns_df,
        "2024-01-01": "2024-01-01",
        "2023": 2023,
        "Electronics": "Electronics"
    }

    try:
        with open("student_submission/solution.py") as f:
            src = f.read()
            tree = ast.parse(src)
            fn_node = next((n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == func_name), None)
            if not fn_node:
                raise AssertionError(f"Function '{func_name}' not found in solution.py")
            code = ast.get_source_segment(src, fn_node)

            # Anti-cheat: detect "pass"
            for node in ast.walk(fn_node):
                if isinstance(node, ast.Pass):
                    raise AssertionError("Anti-cheat violation: use of 'pass' detected")

        fn = getattr(sol, func_name)
        actual_args = [arg_map.get(arg, arg) for arg in args]
        result = fn(*actual_args)

        if expected == "DataFrame":
            assert isinstance(result, DataFrame), "Result is not a DataFrame"
        elif expected == "list":
            assert isinstance(result, list), "Result is not a list"
        elif expected == "str":
            assert isinstance(result, str), "Result is not a string"
        elif expected == "tuple":
            assert isinstance(result, tuple), "Result is not a tuple"
        elif expected == "float":
            assert isinstance(result, float), "Result is not a float"

        validate_sorting(code, func_name)
        validate_null_handling(code, func_name)

        print(f"{GREEN}✅ Test PASSED: {func_name}{RESET}")
        log_result("PASS", func_name)

    except AssertionError as ae:
        print(f"{RED}❌ Test FAILED: {func_name} - Reason: {ae}{RESET}")
        log_result(f"FAIL - {ae}", func_name)
        pytest.fail(str(ae))

    except Exception as e:
        print(f"{RED}❌ Test FAILED: {func_name} - Reason: Unexpected Error: {e}{RESET}")
        log_result(f"FAIL - Unexpected Error: {e}", func_name)
        pytest.fail(str(e))
