import pytest
import ast
import student_submission.solution as sol
from pyspark.sql import SparkSession
import os

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOG_PATH = os.path.join(ROOT_DIR, "test_report.log")

def log_result(func_name, status):
    try:
        with open(LOG_PATH, "a") as f:
            f.write(f"[{status.upper()}] {func_name}\n")
    except Exception as e:
        print(f"Logging failed: {e}")

def load_config(path="tests/test_config.json"):
    import json
    with open(path) as f:
        return json.load(f)

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("PatientDoctorTest") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .getOrCreate()

@pytest.fixture(scope="module")
def df_pair(spark):
    df1 = sol.create_patient_df(spark, "data/patients.csv")
    df2 = sol.create_doctor_df(spark, "data/doctors.csv")
    return df1, df2

def check_no_pass(func_name):
    tree = ast.parse(open("student_submission/solution.py").read())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == func_name:
            for sub in ast.walk(node):
                if isinstance(sub, ast.Pass):
                    raise AssertionError(f"Anti-cheat violation: 'pass' in {func_name}()")

def pytest_generate_tests(metafunc):
    if "test_case" in metafunc.fixturenames:
        config = load_config()
        metafunc.parametrize("test_case", config)

def test_from_config(test_case, spark, df_pair):
    func_name = test_case["function"]
    status = "pass"

    try:
        check_no_pass(func_name)

        df1, df2 = df_pair

        if func_name == "create_patient_df":
            result = sol.create_patient_df(spark, "data/patients.csv")

        elif func_name == "create_doctor_df":
            result = sol.create_doctor_df(spark, "data/doctors.csv")

        elif func_name == "join_patient_doctor_df":
            result = sol.join_patient_doctor_df(df1, df2)

        elif func_name == "group_by_units_received_sum":
            joined = sol.join_patient_doctor_df(df1, df2)
            result = sol.group_by_units_received_sum(joined)

        elif func_name == "group_by_patient_count":
            joined = sol.join_patient_doctor_df(df1, df2)
            result = sol.group_by_patient_count(joined)

        else:
            raise ValueError(f"Unhandled function: {func_name}")

        if test_case["expected"] == "DataFrame":
            assert result.count() >= 0
        elif test_case["expected"] == "tuple":
            assert isinstance(result, tuple)
        elif test_case["expected"] == "list":
            assert isinstance(result, list)
        elif test_case["expected"] == "dict":
            assert isinstance(result, dict)
        elif test_case["expected"] == "int":
            assert isinstance(result, int)

    except Exception:
        status = "fail"
        raise
    finally:
        log_result(func_name, status)
        print(f"[{status.upper()}] {func_name}")

